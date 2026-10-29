import json, sqlite3, threading
from datetime import datetime, timezone
from pathlib import Path
from .config import DATABASE_PATH
_LOCK=threading.RLock()
def now(): return datetime.now(timezone.utc).isoformat()
class Store:
    def __init__(self,path=None):
        self.path=path or DATABASE_PATH
        if self.path != ":memory:": Path(self.path).parent.mkdir(parents=True,exist_ok=True)
        self.init()
    def connect(self):
        c=sqlite3.connect(self.path,timeout=10,check_same_thread=False); c.row_factory=sqlite3.Row
        c.execute("PRAGMA foreign_keys=ON"); return c
    def init(self):
        with _LOCK,self.connect() as c:
            c.executescript("""
            CREATE TABLE IF NOT EXISTS orders(id INTEGER PRIMARY KEY AUTOINCREMENT, order_id TEXT,
              idempotency_key TEXT UNIQUE, symbol TEXT NOT NULL, side TEXT NOT NULL, quantity INTEGER NOT NULL,
              price REAL NOT NULL, order_value REAL NOT NULL, status TEXT NOT NULL, risk_reasons TEXT NOT NULL,
              anomaly TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS positions(symbol TEXT PRIMARY KEY,quantity INTEGER NOT NULL,mark_price REAL NOT NULL);
            CREATE TABLE IF NOT EXISTS audit_events(id INTEGER PRIMARY KEY AUTOINCREMENT,event_type TEXT NOT NULL,
              entity_id TEXT,payload TEXT NOT NULL,created_at TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS app_state(key TEXT PRIMARY KEY,value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS batch_results(id INTEGER PRIMARY KEY AUTOINCREMENT,batch_id TEXT NOT NULL,
              row_number INTEGER NOT NULL,input_order_id TEXT,result_json TEXT NOT NULL,created_at TEXT NOT NULL);
            INSERT OR IGNORE INTO app_state(key,value) VALUES('daily_pnl','0');
            """)
    def audit(self,typ,eid,payload):
        with _LOCK,self.connect() as c:
            c.execute("INSERT INTO audit_events(event_type,entity_id,payload,created_at) VALUES(?,?,?,?)",
                      (typ,str(eid) if eid is not None else None,json.dumps(payload,sort_keys=True),now()))
    def state(self,key,default="0"):
        with self.connect() as c:
            r=c.execute("SELECT value FROM app_state WHERE key=?",(key,)).fetchone()
            return r["value"] if r else default
    def set_state(self,key,value):
        with _LOCK,self.connect() as c:
            c.execute("INSERT INTO app_state(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",(key,str(value)))
    def order_by_key(self,key):
        with self.connect() as c:
            r=c.execute("SELECT * FROM orders WHERE idempotency_key=?",(key,)).fetchone()
            return dict(r) if r else None
    def order(self,oid):
        with self.connect() as c:
            r=c.execute("SELECT * FROM orders WHERE id=?",(oid,)).fetchone()
            return dict(r) if r else None
    def orders(self,limit=1000):
        with self.connect() as c:
            return [dict(r) for r in c.execute("SELECT * FROM orders ORDER BY id DESC LIMIT ?",(limit,)).fetchall()]
    def positions(self):
        with self.connect() as c:
            return [dict(r) for r in c.execute("SELECT * FROM positions ORDER BY symbol").fetchall()]
    def position(self,symbol):
        with self.connect() as c:
            r=c.execute("SELECT * FROM positions WHERE symbol=?",(symbol,)).fetchone()
            return dict(r) if r else {"symbol":symbol,"quantity":0,"mark_price":0.0}
    def insert_order(self,req,d,key=None):
        stamp=now()
        with _LOCK,self.connect() as c:
            cur=c.execute("""INSERT INTO orders(order_id,idempotency_key,symbol,side,quantity,price,order_value,status,risk_reasons,anomaly,created_at,updated_at)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",(req.get("order_id"),key,d["symbol"],d["side"],d["quantity"],d["price"],d["order_value"],d["status"],json.dumps(d["risk_reasons"]),json.dumps(d["anomaly"]),stamp,stamp))
            oid=cur.lastrowid
            c.execute("INSERT INTO audit_events(event_type,entity_id,payload,created_at) VALUES(?,?,?,?)",
              ("ORDER_EVALUATED",str(oid),json.dumps({"status":d["status"],"risk_reasons":d["risk_reasons"],"anomaly":d["anomaly"]},sort_keys=True),stamp))
            return oid
    def update_status(self,oid,status,typ,payload):
        with _LOCK,self.connect() as c:
            stamp=now(); c.execute("UPDATE orders SET status=?,updated_at=? WHERE id=?",(status,stamp,oid))
            c.execute("INSERT INTO audit_events(event_type,entity_id,payload,created_at) VALUES(?,?,?,?)",(typ,str(oid),json.dumps(payload),stamp))
    def fill(self,oid):
        stamp=now()
        with _LOCK,self.connect() as c:
            c.execute("BEGIN IMMEDIATE")
            r=c.execute("SELECT * FROM orders WHERE id=?",(oid,)).fetchone()
            if not r: return None,"Order not found"
            if r["status"]!="APPROVED": return dict(r),f"Only APPROVED orders can be filled; current status is {r['status']}"
            p=c.execute("SELECT * FROM positions WHERE symbol=?",(r["symbol"],)).fetchone()
            old=p["quantity"] if p else 0; new=old+(r["quantity"] if r["side"]=="BUY" else -r["quantity"])
            if new<0: return dict(r),"Short selling is disabled in this POC"
            c.execute("""INSERT INTO positions(symbol,quantity,mark_price) VALUES(?,?,?)
              ON CONFLICT(symbol) DO UPDATE SET quantity=excluded.quantity,mark_price=excluded.mark_price""",(r["symbol"],new,r["price"]))
            c.execute("UPDATE orders SET status='FILLED',updated_at=? WHERE id=?",(stamp,oid))
            c.execute("INSERT INTO audit_events(event_type,entity_id,payload,created_at) VALUES(?,?,?,?)",
                      ("SIMULATED_FILL",str(oid),json.dumps({"symbol":r["symbol"],"quantity":new}),stamp))
            return dict(r),None
    def save_batch(self,bid,rownum,orderid,result):
        with _LOCK,self.connect() as c:
            c.execute("INSERT INTO batch_results(batch_id,row_number,input_order_id,result_json,created_at) VALUES(?,?,?,?,?)",
                      (bid,rownum,str(orderid or ""),json.dumps(result,sort_keys=True),now()))
    def batch(self,bid):
        with self.connect() as c:
            return [json.loads(r["result_json"]) for r in c.execute("SELECT result_json FROM batch_results WHERE batch_id=? ORDER BY row_number",(bid,)).fetchall()]
    def audit_rows(self,limit=100):
        with self.connect() as c:
            return [dict(r) for r in c.execute("SELECT * FROM audit_events ORDER BY id DESC LIMIT ?",(limit,)).fetchall()]
    def reset(self):
        with _LOCK,self.connect() as c:
            for table in ("orders","positions","audit_events","batch_results"): c.execute(f"DELETE FROM {table}")
            c.execute("UPDATE app_state SET value='0' WHERE key='daily_pnl'")
