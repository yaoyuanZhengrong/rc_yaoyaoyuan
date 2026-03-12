import json
import time
import signal
import sys
import logging
import threading
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from kafka import KafkaConsumer
from kafka.errors import KafkaError, NoBrokersAvailable
from sqlalchemy.exc import SQLAlchemyError

from config import settings
from database import get_db_context
from models import NotificationRecord, MessageStatus
from kafka_producer import send_notification_message, send_to_dlq_topic
from message_sender import send_notification

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

RETRY_DELAYS = [10, 60, 300, 600, 1800]
MAX_WORKERS = settings.consumer_workers
BATCH_TIMEOUT_MS = 1000  # 降低批处理超时，减少低频消息延迟


def create_consumer():
    """创建 Kafka 消费者，支持重连"""
    while True:
        try:
            consumer = KafkaConsumer(
                settings.kafka_topic,
                bootstrap_servers=settings.kafka_bootstrap_servers,
                group_id=settings.kafka_consumer_group,
                value_deserializer=lambda m: m,
                auto_offset_reset='earliest',
                enable_auto_commit=False,
                reconnect_backoff_ms=1000,
                reconnect_backoff_max_ms=30000,
            )
            logger.info(f"Connected to Kafka: {settings.kafka_bootstrap_servers}")
            return consumer
        except NoBrokersAvailable as e:
            logger.error(f"No brokers available, retrying in 5s: {e}")
            time.sleep(5)
        except Exception as e:
            logger.error(f"Failed to create consumer: {e}, retrying in 5s")
            time.sleep(5)


def save_to_database(message_data: dict) -> bool:
    """保存消息到数据库，状态为pending"""
    with get_db_context() as db:
        business_id = message_data.get("business_id")
        
        existing = db.query(NotificationRecord).filter(
            NotificationRecord.business_id == business_id
        ).first()
        
        if existing:
            return True
        
        record = NotificationRecord(
            business_id=business_id,
            user_id=message_data.get("user_id"),
            msg_channel=message_data.get("msg_channel"),
            msg=message_data.get("msg"),
            receiver_info=json.dumps(message_data.get("receiver_info")) if message_data.get("receiver_info") else None,
            status=MessageStatus.PENDING,
            retry_count=0
        )
        db.add(record)
        return True


def safe_parse_message(message) -> tuple:
    """安全解析 Kafka 消息，解析失败发送到 DLQ，若 DLQ 失败则抛出异常"""
    try:
        message_data = json.loads(message.value.decode('utf-8'))
        return message_data, None
    except (json.JSONDecodeError, UnicodeDecodeError, AttributeError) as e:
        logger.error(f"Failed to parse message: {e}, sending to DLQ")
        raw_value = message.value.decode('utf-8', errors='replace') if message.value else ""
        try:
            success = send_to_dlq_topic(
                business_id=f"invalid_{int(time.time() * 1000)}",
                message={"raw": raw_value},
                error=str(e)
            )
            if not success:
                logger.error(f"Failed to send to DLQ: {e}")
                raise Exception(f"DLQ sending failed: {e}")
        except Exception as dlq_err:
            logger.error(f"DLQ service error: {dlq_err}")
            raise
        return None, e


def process_message(message_data: dict) -> bool:
    """处理消息，调用实际的发送逻辑"""
    result = send_notification(
        msg_channel=message_data.get("msg_channel"),
        receiver_info=message_data.get("receiver_info"),
        msg=message_data.get("msg")
    )
    return result


def update_message_status_atomic(business_id: str, to_status: MessageStatus, from_status: MessageStatus = None, **kwargs) -> bool:
    """原子化更新状态，确保状态机流向正确且防并发竞争"""
    with get_db_context() as db:
        query = db.query(NotificationRecord).filter(NotificationRecord.business_id == business_id)
        if from_status:
            query = query.filter(NotificationRecord.status == from_status)
        
        record = query.first()
        if record:
            record.status = to_status
            record.updated_at = datetime.now(timezone.utc).replace(tzinfo=None)
            for key, value in kwargs.items():
                if hasattr(record, key):
                    setattr(record, key, value)
            return True
        return False


def update_message_status(business_id: str, status: MessageStatus, error_message: str = None, available_after: datetime = None):
    """更新消息状态 (向后兼容调用，内部改为原子更新)"""
    return update_message_status_atomic(
        business_id, 
        status, 
        error_message=error_message, 
        available_after=available_after
    )


def increment_retry_count(business_id: str) -> int:
    """增加重试计数，返回当前重试次数"""
    with get_db_context() as db:
        record = db.query(NotificationRecord).filter(
            NotificationRecord.business_id == business_id
        ).first()
        
        if record:
            record.retry_count += 1
            return record.retry_count
        return 0


def get_message_status(business_id: str) -> MessageStatus:
    """获取消息状态"""
    with get_db_context() as db:
        record = db.query(NotificationRecord).filter(
            NotificationRecord.business_id == business_id
        ).first()
        
        if record:
            return record.status
        return None


def handle_message(message_data: dict) -> bool:
    """处理单条消息：存库 → 发送"""
    business_id = message_data.get("business_id")
    
    logger.info(f"Processing message: {business_id}")
    
    current_status = get_message_status(business_id)
    if current_status == MessageStatus.SUCCESS:
        logger.info(f"Message {business_id} already processed successfully, skipping")
        return True
    
    if current_status is None:
        if not save_to_database(message_data):
            logger.error(f"Failed to save message {business_id} to database")
            raise Exception(f"Failed to save message {business_id} to database")
    
    success = process_message(message_data)
    
    if success:
        # 使用原子更新确保状态机正确，只有当记录仍然是当前状态时才更新为成功
        updated = update_message_status_atomic(
            business_id, 
            MessageStatus.SUCCESS,
            from_status=current_status
        )
        if updated:
            logger.info(f"Message {business_id} processed successfully")
    else:
        raise Exception(f"Message {business_id} send failed")
    
    return True


def process_message_once(message) -> bool:
    """主消费者：处理一次，失败则存入数据库等待重试"""
    message_data, parse_error = safe_parse_message(message)
    if message_data is None:
        logger.error(f"Skipping invalid message due to parse error: {parse_error}")
        return False
    
    business_id = message_data.get("business_id")
    
    try:
        handle_message(message_data)
        return True
    except Exception as e:
        logger.error(f"Error processing message {business_id}: {e}")
        new_count = increment_retry_count(business_id)
        
        if new_count >= settings.max_retry_count:
            logger.error(f"Message {business_id} exceeded max retry count, marking as FAILED")
            update_message_status_atomic(
                business_id, 
                MessageStatus.FAILED, 
                # 仅从非终态转换到失败状态
                error_message=f"Failed after {new_count} attempts: {str(e)}"
            )
        else:
            delay = RETRY_DELAYS[min(new_count - 1, len(RETRY_DELAYS) - 1)]
            available_after = (datetime.now(timezone.utc) + timedelta(seconds=delay)).replace(tzinfo=None)
            
            update_message_status_atomic(
                business_id, 
                MessageStatus.RETRY_PENDING, 
                error_message=str(e),
                available_after=available_after
            )
            logger.info(f"Message {business_id} failed, scheduled for retry {new_count + 1} (in {delay}s)")
        
        return False


def start_retry_scanner():
    """定期扫描并重试任务"""
    logger.info("Starting retry scanner...")
    while True:
        try:
            scan_and_retry_messages()
        except Exception as e:
            logger.error(f"Retry scanner error: {e}")
        time.sleep(settings.retry_scan_interval)


def start_consumer():
    """启动多线程并发消费者和重试扫描器"""
    logger.info(f"Starting consumer with {MAX_WORKERS} workers")
    
    # 启动重试扫描器子线程
    retry_thread = threading.Thread(target=start_retry_scanner, daemon=True)
    retry_thread.start()
    
    while True:
        try:
            consumer = create_consumer()
            
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                message_batch = []
                batch_start_time = None

                def process_batch():
                    nonlocal batch_start_time
                    if not message_batch:
                        return
                    futures = [executor.submit(process_message_once, msg) for msg in message_batch]
                    for future in as_completed(futures):
                        try:
                            future.result()
                        except Exception as e:
                            logger.error(f"Future error: {e}")
                    
                    for msg in message_batch:
                        try:
                            consumer.commit()
                        except Exception as e:
                            logger.error(f"Commit error: {e}")
                    
                    message_batch.clear()
                    batch_start_time = None
                
                while True:
                    records = consumer.poll(timeout_ms=1000)
                    
                    if not records:
                        if message_batch and batch_start_time:
                            elapsed = (time.time() - batch_start_time) * 1000
                            if elapsed >= BATCH_TIMEOUT_MS:
                                logger.info(f"Batch timeout after {elapsed}ms, processing {len(message_batch)} messages")
                                process_batch()
                        continue
                    
                    for topic_partition, messages in records.items():
                        for message in messages:
                            if not message_batch:
                                batch_start_time = time.time()
                            message_batch.append(message)
                            
                            if len(message_batch) >= MAX_WORKERS:
                                process_batch()
                            
        except KeyboardInterrupt:
            logger.info("Consumer interrupted, shutting down...")
            break
        except Exception as e:
            logger.error(f"Consumer error: {e}, reconnecting in 5s...")
            time.sleep(5)
    
    logger.info("Consumer stopped")


def scan_and_retry_messages():
    """定时扫描数据库中超过available_after时间的消息，并发安全地发回 Kafka 主队列"""
    logger.info("Scanning for retryable messages...")
    
    with get_db_context() as db:
        now = datetime.now(timezone.utc).replace(tzinfo=None) # 使用 naive UTC 进行查询
        # 使用 with_for_update(skip_locked=True) 确保多实例部署时不会重复捞取任务
        # 注意：这需要数据库支持（如 MySQL 8.0+, PostgreSQL）
        retry_records = db.query(NotificationRecord).filter(
            NotificationRecord.status == MessageStatus.RETRY_PENDING,
            NotificationRecord.available_after <= now
        ).with_for_update(skip_locked=True).all()
        
        if not retry_records:
            return
        
        logger.info(f"Found {len(retry_records)} messages ready for retry")
        
        for record in retry_records:
            business_id = record.business_id
            
            # 再次确认重试次数（双重校验）
            if record.retry_count >= settings.max_retry_count:
                logger.error(f"Message {business_id} already at max retry count, marking as FAILED")
                record.status = MessageStatus.FAILED
                continue

            try:
                message_data = {
                    "business_id": record.business_id,
                    "user_id": record.user_id,
                    "msg_channel": record.msg_channel,
                    "msg": record.msg,
                    "receiver_info": json.loads(record.receiver_info) if record.receiver_info else None,
                    "retry_count": record.retry_count
                }
                
                logger.info(f"Re-queueing message {business_id} to main topic (attempt {record.retry_count + 1})")
                
                # 发回主队列
                if send_notification_message(business_id, message_data):
                    # 标记为 PENDING，防止扫描器重复处理
                    record.status = MessageStatus.PENDING
                    record.updated_at = datetime.now(timezone.utc).replace(tzinfo=None)
                else:
                    logger.error(f"Failed to re-queue message {business_id}")
                
            except Exception as e:
                logger.error(f"Error re-queueing message {business_id}: {e}")
                record.error_message = f"Re-queue error: {str(e)}"
                record.updated_at = datetime.now(timezone.utc).replace(tzinfo=None)


def start_retry_scanner(interval_seconds: int = 5):
    """启动定时扫描任务"""
    logger.info(f"Starting retry scanner with {interval_seconds}s interval")
    
    while True:
        try:
            scan_and_retry_messages()
            time.sleep(interval_seconds)
        except KeyboardInterrupt:
            logger.info("Retry scanner interrupted, shutting down...")
            break
        except Exception as e:
            logger.error(f"Retry scanner error: {e}, retrying in 5s...")
            time.sleep(5)
    
    logger.info("Retry scanner stopped")


def signal_handler(signum, frame):
    """信号处理器"""
    logger.info(f"Received signal {signum}, shutting down...")
    sys.exit(0)


if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    import sys
    
    if len(sys.argv) > 1:
        if sys.argv[1] == "scanner":
            start_retry_scanner()
        else:
            print(f"Unknown command: {sys.argv[1]}")
            print("Usage: python kafka_consumer.py [scanner]")
    else:
        start_consumer()
