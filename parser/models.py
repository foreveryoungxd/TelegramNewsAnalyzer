from pydantic import BaseModel


class NewMessage(BaseModel):
    timestamp: str
    url: str
    channel: str
    channel_id: int
    text: str
