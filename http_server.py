from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from pydantic import BaseModel, Field
from typing import Optional, Dict, Any

from http_request_params import NotificationRequest
from config import settings
from kafka_producer import send_notification_message

app = FastAPI(title="Notification Service", version="1.0.0")


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    errors = exc.errors()
    error_messages = []
    for error in errors:
        field = ".".join(str(loc) for loc in error["loc"] if loc != "body")
        error_messages.append(f"{field}: {error['msg']}")
    
    return JSONResponse(
        status_code=422,
        content={
            "code": 422,
            "message": "参数校验失败",
            "data": {"errors": error_messages}
        }
    )


class ResponseModel(BaseModel):
    code: int = Field(200, description="响应码")
    message: str = Field(..., description="响应消息")
    data: Optional[Dict[str, Any]] = Field(None, description="响应数据")


@app.post("/api/notification", response_model=ResponseModel)
def create_notification(request: NotificationRequest):
    message = {
        "business_id": request.business_id,
        "user_id": request.user_id,
        "msg_channel": request.msg_channel,
        "msg": request.msg,
        "receiver_info": request.receiver_info
    }

    success = send_notification_message(request.business_id, message)
    
    if not success:
        raise HTTPException(status_code=500, detail="消息发送失败，请稍后重试")

    return ResponseModel(
        code=200,
        message="通知请求已接收",
        data={"business_id": request.business_id}
    )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=settings.host, port=settings.port)
