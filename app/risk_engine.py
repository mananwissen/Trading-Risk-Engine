from datetime import datetime,timedelta,timezone
from . import config
class RiskEngine:
    def __init__(self,store,detector): self.store=store; self.detector=detector
    def recent_count(self):
        cutoff=datetime.now(timezone.utc)-timedelta(seconds=config.ORDER_WINDOW_SECONDS)
        n=0
        for o in self.store.orders():
            try:
                if datetime.fromisoformat(o["created_at"])>=cutoff: n+=1
            except Exception: pass
        return n
    def metrics(self):
        pos=self.store.positions()
        values={p["symbol"]:p["quantity"]*p["mark_price"] for p in pos}
        pending={}
        for o in self.store.orders():
            if o["status"]=="APPROVED":
                v=o["order_value"] if o["side"]=="BUY" else -o["order_value"]
                pending[o["symbol"]]=pending.get(o["symbol"],0)+v
        gross_pos=sum(abs(v) for v in values.values())
        gross_pending=sum(abs(v) for v in pending.values())
        gross=gross_pos+gross_pending
        equity=config.ACCOUNT_EQUITY
        return {"positions":pos,"position_values":values,"gross_positions":round(gross_pos,2),
                "gross_reserved":round(gross_pending,2),"gross_exposure":round(gross,2),
                "account_equity":equity,"leverage":round(gross/equity,4) if equity else None,
                "daily_pnl":float(self.store.state("daily_pnl","0"))}
    def evaluate(self,req):
        symbol=req["symbol"].upper(); side=req["side"]; qty=int(req["quantity"]); price=float(req["price"])
        value=qty*price; m=self.metrics(); reasons=[]
        if value>config.MAX_ORDER_VALUE: reasons.append(f"ORDER_VALUE_LIMIT: {value:.2f} > {config.MAX_ORDER_VALUE:.2f}")
        p=self.store.position(symbol); reserved=0.0
        for o in self.store.orders():
            if o["status"]=="APPROVED" and o["symbol"]==symbol:
                reserved += o["order_value"] if o["side"]=="BUY" else -o["order_value"]
        projected=p["quantity"]*price+reserved+(value if side=="BUY" else -value)
        if side=="BUY" and abs(projected)>config.MAX_POSITION_VALUE:
            reasons.append(f"POSITION_VALUE_LIMIT: projected {projected:.2f} > {config.MAX_POSITION_VALUE:.2f}")
        if side=="SELL" and p["quantity"]*price<value:
            reasons.append("INSUFFICIENT_POSITION: selling beyond simulated long position is disabled")
        if m["gross_exposure"]+value>config.MAX_GROSS_EXPOSURE:
            reasons.append(f"GROSS_EXPOSURE_LIMIT: projected {m['gross_exposure']+value:.2f} > {config.MAX_GROSS_EXPOSURE:.2f}")
        lev=(m["gross_exposure"]+value)/config.ACCOUNT_EQUITY
        if lev>config.MAX_LEVERAGE: reasons.append(f"LEVERAGE_LIMIT: projected {lev:.3f}x > {config.MAX_LEVERAGE:.3f}x")
        pnl=float(self.store.state("daily_pnl","0"))
        if pnl<=-config.MAX_DAILY_LOSS: reasons.append(f"DAILY_LOSS_STOP: {pnl:.2f} <= -{config.MAX_DAILY_LOSS:.2f}")
        recent=self.recent_count()
        if recent>=config.MAX_ORDERS_PER_WINDOW: reasons.append(f"ORDER_RATE_LIMIT: {recent} orders in {config.ORDER_WINDOW_SECONDS}s")
        anomaly=self.detector.score(value,recent,value/max(m["gross_exposure"],1.0)*100)
        return {"status":"REJECTED" if reasons else "APPROVED","symbol":symbol,"side":side,
                "quantity":qty,"price":price,"order_value":round(value,2),"risk_reasons":reasons,
                "anomaly":anomaly,"metrics_before":m}
