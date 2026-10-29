import json
from .config import KAFKA_BOOTSTRAP_SERVERS,KAFKA_TOPIC
class KafkaPublisher:
    def __init__(self):
        self.producer=None; self.last_error=None
        if KAFKA_BOOTSTRAP_SERVERS:
            try:
                from kafka import KafkaProducer
                self.producer=KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
                    value_serializer=lambda v:json.dumps(v).encode(),key_serializer=lambda v:str(v).encode(),acks="all",retries=3)
            except Exception as e: self.last_error=str(e)
    def publish(self,event):
        if not self.producer: return {"published":False,"reason":self.last_error or "Kafka not configured"}
        try:
            self.producer.send(KAFKA_TOPIC,key=event.get("order_id","unknown"),value=event).get(timeout=5)
            return {"published":True,"topic":KAFKA_TOPIC}
        except Exception as e:
            self.last_error=str(e); return {"published":False,"reason":self.last_error}
    def close(self):
        if self.producer: self.producer.flush(timeout=3); self.producer.close(timeout=3)
