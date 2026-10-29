import os
from dotenv import load_dotenv
load_dotenv()
DATABASE_PATH=os.getenv("DATABASE_PATH","./risk_engine.db")
MAX_ORDER_VALUE=float(os.getenv("MAX_ORDER_VALUE","100000"))
MAX_POSITION_VALUE=float(os.getenv("MAX_POSITION_VALUE","200000"))
MAX_GROSS_EXPOSURE=float(os.getenv("MAX_GROSS_EXPOSURE","500000"))
ACCOUNT_EQUITY=float(os.getenv("ACCOUNT_EQUITY","500000"))
MAX_LEVERAGE=float(os.getenv("MAX_LEVERAGE","2.0"))
MAX_DAILY_LOSS=float(os.getenv("MAX_DAILY_LOSS","25000"))
MAX_ORDERS_PER_WINDOW=int(os.getenv("MAX_ORDERS_PER_WINDOW","10"))
ORDER_WINDOW_SECONDS=int(os.getenv("ORDER_WINDOW_SECONDS","60"))
KAFKA_BOOTSTRAP_SERVERS=os.getenv("KAFKA_BOOTSTRAP_SERVERS","").strip()
KAFKA_TOPIC=os.getenv("KAFKA_TOPIC","risk-events")
