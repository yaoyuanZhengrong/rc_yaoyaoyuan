import requests
import json
import logging
from abc import ABC, abstractmethod
from typing import Optional, Dict, Any
from sqlalchemy.orm import Session
from database import get_db_context
from models import ApiConfig

logger = logging.getLogger(__name__)


class SendStrategy(ABC):
    @abstractmethod
    def send(self, receiver_info: Optional[Dict[str, Any]], msg: str, channel: str) -> bool:
        pass

    @abstractmethod
    def validate_receiver_info(self, receiver_info: Optional[Dict[str, Any]]) -> bool:
        pass


class HttpApiStrategy(SendStrategy):
    """通用 HTTP API 投递策略"""
    def validate_receiver_info(self, receiver_info: Optional[Dict[str, Any]]) -> bool:
        # HTTP 策略通常由 ApiConfig 定义，业务参数作为变量注入
        return True

    def _get_config(self, channel: str) -> Optional[ApiConfig]:
        with get_db_context() as db:
            return db.query(ApiConfig).filter(ApiConfig.channel == channel).first()

    def send(self, receiver_info: Optional[Dict[str, Any]], msg: str, channel: str) -> bool:
        config = self._get_config(channel)
        if not config:
            logger.error(f"ApiConfig not found for channel: {channel}")
            return False

        try:
            # 1. 组装 URL
            url = config.base_url
            
            # 2. 组装 Headers (简单合并模板与动态参数)
            headers = {}
            if config.headers_template:
                headers.update(json.loads(config.headers_template))
            
            # 3. 组装 Payload (支持简单的变量替换，如 {{msg}}, {{user_id}})
            payload = {}
            if config.payload_template:
                template_str = config.payload_template
                # 简单替换 (实际工程可用 Jinja2)
                template_str = template_str.replace("{{msg}}", msg)
                if receiver_info:
                    for k, v in receiver_info.items():
                        template_str = template_str.replace(f"{{{{{k}}}}}", str(v))
                payload = json.loads(template_str)
            else:
                # 默认透传 msg 和 receiver_info
                payload = {"msg": msg, "receiver_info": receiver_info}

            # 4. 执行 HTTP 调用
            response = requests.request(
                method=config.method,
                url=url,
                headers=headers,
                json=payload,
                timeout=10
            )
            
            logger.info(f"Sent notification to {config.provider_name} ({channel}): Status {response.status_code}")
            
            # 业务判断逻辑：通常 2xx 视为成功
            return 200 <= response.status_code < 300
            
        except Exception as e:
            logger.error(f"HTTP send error for {channel}: {e}")
            return False


class SmsStrategy(SendStrategy):
    def validate_receiver_info(self, receiver_info: Optional[Dict[str, Any]]) -> bool:
        if not receiver_info or "phone" not in receiver_info:
            return False
        return True

    def send(self, receiver_info: Optional[Dict[str, Any]], msg: str, channel: str) -> bool:
        phone = receiver_info.get("phone")
        print(f"SMS: Sending to {phone}: {msg}")
        return self._simulate_api_call()

    def _simulate_api_call(self) -> bool:
        import time
        import random
        time.sleep(0.1)
        return random.random() > 0.1


class EmailStrategy(SendStrategy):
    def validate_receiver_info(self, receiver_info: Optional[Dict[str, Any]]) -> bool:
        if not receiver_info or "email" not in receiver_info:
            return False
        return True

    def send(self, receiver_info: Optional[Dict[str, Any]], msg: str, channel: str) -> bool:
        email = receiver_info.get("email")
        print(f"Email: Sending to {email}: {msg}")
        return self._simulate_api_call()

    def _simulate_api_call(self) -> bool:
        import time
        import random
        time.sleep(0.1)
        return random.random() > 0.1


class PushStrategy(SendStrategy):
    def validate_receiver_info(self, receiver_info: Optional[Dict[str, Any]]) -> bool:
        if not receiver_info or "device_token" not in receiver_info:
            return False
        return True

    def send(self, receiver_info: Optional[Dict[str, Any]], msg: str, channel: str) -> bool:
        token = receiver_info.get("device_token")
        print(f"Push: Sending to device {token}: {msg}")
        return self._simulate_api_call()

    def _simulate_api_call(self) -> bool:
        import time
        import random
        time.sleep(0.1)
        return random.random() > 0.1


class MessageSender:
    def __init__(self):
        # 默认内置策略映射
        self._default_strategy = HttpApiStrategy()
        self._strategies: Dict[str, SendStrategy] = {}

    def register_strategy(self, channel: str, strategy: SendStrategy):
        self._strategies[channel] = strategy

    def get_strategy(self, channel: str) -> Optional[SendStrategy]:
        # 如果没有注册特定策略，则使用默认的 HttpApiStrategy
        return self._strategies.get(channel, self._default_strategy)

    def get_available_channels(self) -> list:
        # 从数据库中动态获取已配置的渠道
        try:
            with get_db_context() as db:
                configs = db.query(ApiConfig).all()
                db_channels = [c.channel for c in configs]
                # 合并手动注册的策略渠道
                return list(set(db_channels + list(self._strategies.keys())))
        except Exception as e:
            logger.error(f"Failed to fetch channels from DB: {e}")
            return list(self._strategies.keys())

    def send(self, msg_channel: str, receiver_info: Optional[Dict[str, Any]], msg: str) -> bool:
        strategy = self.get_strategy(msg_channel)
        
        if not strategy:
            print(f"Unsupported msg_channel: {msg_channel}")
            return False

        if not strategy.validate_receiver_info(receiver_info):
            print(f"Invalid receiver_info for {msg_channel}")
            return False

        return strategy.send(receiver_info, msg, msg_channel)



message_sender = MessageSender()



def send_notification(
    msg_channel: str,
    receiver_info: Optional[Dict[str, Any]],
    msg: str
) -> bool:
    return message_sender.send(msg_channel, receiver_info, msg)
