import csv, io, json, uuid
from contextlib import asynccontextmanager
import pandas as pd
from fastapi import FastAPI, File, Header, HTTPException, Query, UploadFile
from fastapi.responses import StreamingResponse
from . import config
from .anomaly import AnomalyDetector
from .kafka_publisher import KafkaPublisher
from .risk_engine import RiskEngine
from .schemas import OrderRequest, StressTestRequest, DailyPnlRequest
from .storage import Store

store=Store(config.DATABASE_PATH)
detector=AnomalyDetector()
engine=RiskEngine(store,detector)
publisher=KafkaPublisher()

@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    publisher.close()

app=FastAPI(title="Real-Time AI Trading Risk Engine",version="1.0.0",
            description="Simulation-only risk engine with CSV/Excel batch processing, SQL audit and optional Kafka.",lifespan=lifespan)

def decoded(row):
    if not row: return None
    d=dict(row); d["risk_reasons"]=json.loads(d["risk_reasons"]); d["anomaly"]=json.loads(d["anomaly"])
    return d

def evaluate_and_store(raw,key=None):
    order=OrderRequest.model_validate(raw).model_dump()
    order["symbol"]=order["symbol"].upper()
    if not order.get("order_id"): order["order_id"]=f"demo-{uuid.uuid4().hex[:10]}"
    if key:
        prior=store.order_by_key(key)
        if prior: return {"order":decoded(prior),"idempotent_replay":True,"kafka":{"published":False,"reason":"Duplicate request not republished"}}
    decision=engine.evaluate(order)
    try: oid=store.insert_order(order,decision,key)
    except Exception as exc:
        if key:
            prior=store.order_by_key(key)
            if prior: return {"order":decoded(prior),"idempotent_replay":True}
        raise HTTPException(status_code=409,detail=f"Unable to store order: {exc}")
    event={"event_type":"risk.decision","order_id":oid,"symbol":decision["symbol"],"side":decision["side"],
           "quantity":decision["quantity"],"price":decision["price"],"status":decision["status"],
           "risk_reasons":decision["risk_reasons"],"anomaly_flagged":decision["anomaly"]["flagged"]}
    kafka=publisher.publish(event); store.audit("KAFKA_PUBLISH_ATTEMPT",oid,kafka)
    return {"order":decoded(store.order(oid)),"idempotent_replay":False,"kafka":kafka}

@app.get("/health")
def health(): return {"status":"ok","database":config.DATABASE_PATH,"kafka_configured":publisher.producer is not None}

@app.get("/risk-config")
def risk_config():
    return {"max_order_value":config.MAX_ORDER_VALUE,"max_position_value":config.MAX_POSITION_VALUE,
            "max_gross_exposure":config.MAX_GROSS_EXPOSURE,"account_equity":config.ACCOUNT_EQUITY,
            "max_leverage":config.MAX_LEVERAGE,"max_daily_loss":config.MAX_DAILY_LOSS,
            "max_orders_per_window":config.MAX_ORDERS_PER_WINDOW,"order_window_seconds":config.ORDER_WINDOW_SECONDS}

@app.get("/portfolio")
def portfolio(): return engine.metrics()

@app.post("/orders/evaluate")
def evaluate_order(order:OrderRequest,idempotency_key:str|None=Header(default=None,alias="Idempotency-Key")):
    return evaluate_and_store(order.model_dump(),idempotency_key)

@app.get("/orders")
def list_orders(limit:int=Query(default=100,ge=1,le=1000)):
    return [decoded(r) for r in store.orders(limit)]

@app.post("/orders/batch-evaluate")
def batch_evaluate(file:UploadFile=File(...)):
    name=(file.filename or "").lower()
    if not name.endswith((".csv",".xlsx",".xls")): raise HTTPException(400,"Upload a CSV or Excel file.")
    try:
        raw_bytes=file.file.read()
        df=pd.read_csv(io.BytesIO(raw_bytes)) if name.endswith(".csv") else pd.read_excel(io.BytesIO(raw_bytes))
    except Exception as exc: raise HTTPException(400,f"Could not read file: {exc}")
    df.columns=[str(c).strip().lower() for c in df.columns]
    missing=sorted({"symbol","side","quantity","price"}-set(df.columns))
    if missing: raise HTTPException(400,f"Missing required columns: {missing}")
    batch_id=uuid.uuid4().hex[:12]; results=[]
    for idx,row in df.iterrows():
        raw={k:row.get(k) for k in ["order_id","symbol","side","quantity","price"]}
        raw={k:v for k,v in raw.items() if v is not None and not (isinstance(v,float) and pd.isna(v))}
        try:
            raw["symbol"]=str(raw["symbol"]).strip().upper()
            raw["side"]=str(raw["side"]).strip().upper()
            raw["quantity"]=int(raw["quantity"]); raw["price"]=float(raw["price"])
            response=evaluate_and_store(raw); o=response["order"]
            result={"batch_id":batch_id,"row_number":int(idx)+2,"input_order_id":raw.get("order_id",""),
                "db_order_id":o["id"],"symbol":o["symbol"],"side":o["side"],"quantity":o["quantity"],
                "price":o["price"],"order_value":o["order_value"],"decision":o["status"],
                "risk_reasons":" | ".join(o["risk_reasons"]),"anomaly_flag":o["anomaly"]["flagged"],
                "anomaly_score":o["anomaly"]["anomaly_score"],"validation_error":""}
        except Exception as exc:
            result={"batch_id":batch_id,"row_number":int(idx)+2,"input_order_id":str(row.get("order_id","")),
                "db_order_id":"","symbol":str(row.get("symbol","")),"side":str(row.get("side","")),
                "quantity":row.get("quantity",""),"price":row.get("price",""),"order_value":"",
                "decision":"INVALID","risk_reasons":"","anomaly_flag":"","anomaly_score":"","validation_error":str(exc)}
        store.save_batch(batch_id,int(idx)+2,row.get("order_id",""),result); results.append(result)
    return {"batch_id":batch_id,"rows_processed":len(results),
        "approved":sum(r["decision"]=="APPROVED" for r in results),
        "rejected":sum(r["decision"]=="REJECTED" for r in results),
        "invalid":sum(r["decision"]=="INVALID" for r in results),
        "results":results,"download_csv":f"/batches/{batch_id}/results.csv"}


@app.get("/orders/{order_db_id}")
def get_order(order_db_id:int):
    row=store.order(order_db_id)
    if not row: raise HTTPException(404,"Order not found")
    return decoded(row)

@app.post("/orders/{order_db_id}/simulate-fill")
def simulate_fill(order_db_id:int):
    row,error=store.fill(order_db_id)
    if error: raise HTTPException(404 if error=="Order not found" else 409,error)
    return {"message":"Simulated fill completed","order":decoded(store.order(order_db_id)),"portfolio":engine.metrics()}

@app.post("/orders/{order_db_id}/cancel")
def cancel_order(order_db_id:int):
    row=store.order(order_db_id)
    if not row: raise HTTPException(404,"Order not found")
    if row["status"]!="APPROVED": raise HTTPException(409,f"Only APPROVED orders can be cancelled; status is {row['status']}")
    store.update_status(order_db_id,"CANCELLED","ORDER_CANCELLED",{"reason":"Manual demo cancellation"})
    return {"order":decoded(store.order(order_db_id)),"portfolio":engine.metrics()}

@app.get("/batches/{batch_id}/results.csv")
def download_batch(batch_id:str):
    rows=store.batch(batch_id)
    if not rows: raise HTTPException(404,"Batch not found")
    out=io.StringIO(); writer=csv.DictWriter(out,fieldnames=list(rows[0].keys()),extrasaction="ignore")
    writer.writeheader(); writer.writerows(rows); out.seek(0)
    return StreamingResponse(iter([out.getvalue()]),media_type="text/csv",
        headers={"Content-Disposition":f'attachment; filename="risk_results_{batch_id}.csv"'})

@app.post("/stress-test")
def stress_test(request:StressTestRequest):
    metrics=engine.metrics(); positions={p["symbol"]:p for p in metrics["positions"]}
    scenarios=[]; total=0.0
    for symbol,shock in request.shocks.items():
        symbol=symbol.upper(); p=positions.get(symbol); value=p["quantity"]*p["mark_price"] if p else 0.0
        pnl=value*shock; total+=pnl
        scenarios.append({"symbol":symbol,"position_value":round(value,2),"shock_pct":round(shock*100,2),
                          "estimated_pnl":round(pnl,2),"note":"" if p else "No current position"})
    return {"scenarios":scenarios,"total_estimated_pnl":round(total,2),
            "disclaimer":"Simplified spot-position scenario; excludes derivatives, fees and correlations."}

@app.post("/demo/set-daily-pnl")
def set_daily_pnl(request:DailyPnlRequest):
    store.set_state("daily_pnl",request.daily_pnl)
    store.audit("DAILY_PNL_SET_FOR_DEMO",None,{"daily_pnl":request.daily_pnl})
    return {"daily_pnl":request.daily_pnl,"note":"Demo injection only; P&L is not calculated automatically."}

@app.get("/audit")
def audit(limit:int=Query(default=100,ge=1,le=1000)):
    rows=store.audit_rows(limit)
    for row in rows: row["payload"]=json.loads(row["payload"])
    return rows

@app.post("/demo/reset")
def reset_demo():
    store.reset()
    return {"status":"reset","message":"Local demo records cleared."}
