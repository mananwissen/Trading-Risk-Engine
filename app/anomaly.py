import numpy as np
from sklearn.ensemble import IsolationForest
class AnomalyDetector:
    def __init__(self):
        rng=np.random.default_rng(42)
        baseline=np.column_stack([rng.lognormal(8.0,.65,1800),rng.poisson(2.0,1800),np.abs(rng.normal(4.0,5.0,1800))])
        self.model=IsolationForest(n_estimators=100,contamination=.03,random_state=42).fit(baseline)
    def score(self,value,recent_count,exposure_change):
        x=np.array([[value,recent_count,abs(exposure_change)]])
        return {"flagged":bool(self.model.predict(x)[0]==-1),"anomaly_score":round(float(self.model.decision_function(x)[0]),5),
                "model":"IsolationForest","note":"Synthetic-baseline demo only; validate before operational use."}
