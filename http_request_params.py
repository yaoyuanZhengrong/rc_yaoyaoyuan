from pydantic import BaseModel, Field, field_validator, model_validator
from typing import Dict, Any, Optional
from enum import Enum

from message_sender import message_sender


class HttpMethod(str, Enum):
    POST = "POST"
    PUT = "PUT"
    GET = "GET"
    DELETE = "DELETE"


class NotificationRequest(BaseModel):
    business_id: str = Field(..., description="业务流水号，用于幂等校验和日志追踪", example="ORDER_2024001")
    user_id: str = Field(..., description="用户ID", example="user_12345")
    msg_channel: str = Field(..., description="消息渠道，用于匹配供应商配置", example="sms")
    msg: str = Field(..., description="消息内容", example="您的验证码为 123456")
    receiver_info: Optional[Dict[str, Any]] = Field(default=None, description="接收人信息，如手机号、邮箱、设备Token等")

    @field_validator('business_id')
    @classmethod
    def validate_business_id(cls, v):
        if not v or len(v.strip()) == 0:
            raise ValueError("business_id不能为空")
        if len(v) > 64:
            raise ValueError("business_id长度不能超过64个字符")
        return v.strip()
    
    @field_validator('user_id')
    @classmethod
    def validate_user_id(cls, v):
        if not v or len(v.strip()) == 0:
            raise ValueError("user_id不能为空")
        return v.strip()
    
    @field_validator('msg_channel')
    @classmethod
    def validate_msg_channel(cls, v):
        allowed_channels = message_sender.get_available_channels()
        if v not in allowed_channels:
            raise ValueError(f"msg_channel必须是以下值之一: {', '.join(allowed_channels)}")
        return v
    
    @field_validator('msg')
    @classmethod
    def validate_msg(cls, v):
        if not v or len(v.strip()) == 0:
            raise ValueError("msg不能为空")
        if len(v) > 5000:
            raise ValueError("msg长度不能超过5000个字符")
        return v

    @model_validator(mode='after')
    def validate_receiver_info_by_strategy(self):
        strategy = message_sender.get_strategy(self.msg_channel)
        if strategy and not strategy.validate_receiver_info(self.receiver_info):
            raise ValueError(f"msg_channel为{self.msg_channel}时，receiver_info必须包含有效的接收信息")
        return self

    class Config:
        json_schema_extra = {
            "example": {
                "business_id": "req_889231",
                "user_id": "user_12345",
                "msg_channel": "sms",
                "msg": "您的验证码为 123456",
                "receiver_info": {"phone": "13800138000"}
            }
        }
