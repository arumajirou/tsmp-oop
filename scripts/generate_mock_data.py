import pandas as pd, numpy as np, os
os.makedirs("data/raw", exist_ok=True)
os.makedirs("data/processed", exist_ok=True)

idx = pd.date_range("2024-01-01", periods=60, freq="D")
df = pd.DataFrame({
  "unique_id": ["A"]*60 + ["B"]*60,
  "ds": list(idx)*2,
  "y": np.random.rand(60).tolist() + (np.random.rand(60)*2).tolist()
})
df.to_parquet("data/processed/features_demo.parquet", index=False)
print("Wrote data/processed/features_demo.parquet")
