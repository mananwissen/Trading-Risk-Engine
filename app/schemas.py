from typing import Literal
from pydantic import BaseModel, Field, ConfigDict
class OrderRequest(BaseModel):
    model_config=ConfigDict(str_strip_whitespace=True)
    order_id: str | None = Field(default=None,max_length=80)
    symbol: str = Field(min_length=1,max_length=20,pattern=r"^[A-Za-z0-9._-]+$")
    side: Literal["BUY","SELL"]
    quantity: int = Field(gt=0,le=1000000)
    price: float = Field(gt=0,le=1000000000)
class StressTestRequest(BaseModel):
    shocks: dict[str,float] = Field(min_length=1)
class DailyPnlRequest(BaseModel):
    daily_pnl: float
