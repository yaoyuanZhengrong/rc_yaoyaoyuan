from sqlalchemy import Column, String, Integer, DateTime, Text, Enum as SQLEnum, create_engine
from sqlalchemy.orm import declarative_base
from datetime import datetime
import enum

Base = declarative_base()


class MessageStatus(str, enum.Enum):
    PENDING = "pending"
    SUCCESS = "success"
    FAILED = "failed"
    RETRY_PENDING = "retry_pending"


class ApiConfig(Base):
    __tablename__ = "api_configs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    channel = Column(String(32), unique=True, nullable=False, index=True, comment="消息渠道名称")
    provider_name = Column(String(64), nullable=False, comment="供应商名称")
    base_url = Column(String(255), nullable=False, comment="API 基础地址")
    headers_template = Column(Text, nullable=True, comment="Header 模板 (JSON)")
    payload_template = Column(Text, nullable=True, comment="Body 模板 (JSON)")
    method = Column(String(10), default="POST", nullable=False, comment="HTTP 方法")
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    def __repr__(self):
        return f"<ApiConfig(channel={self.channel}, provider={self.provider_name})>"


class NotificationRecord(Base):
    __tablename__ = "notification_records"

    id = Column(Integer, primary_key=True, autoincrement=True)
    business_id = Column(String(64), unique=True, nullable=False, index=True, comment="业务流水号")
    user_id = Column(String(64), nullable=False, index=True, comment="用户ID")
    msg_channel = Column(String(32), nullable=False, comment="消息渠道")
    msg = Column(Text, nullable=False, comment="消息内容")
    receiver_info = Column(Text, nullable=True, comment="接收人信息(JSON)")
    status = Column(SQLEnum(MessageStatus), default=MessageStatus.PENDING, nullable=False, comment="状态")
    retry_count = Column(Integer, default=0, nullable=False, comment="重试次数")
    available_after = Column(DateTime, nullable=True, comment="可重试时间")
    error_message = Column(Text, nullable=True, comment="错误信息")
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    def __repr__(self):
        return f"<NotificationRecord(business_id={self.business_id}, status={self.status})>"
