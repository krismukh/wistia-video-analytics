"""
Copy the gold layer from ADLS Gen2 to dashboards/data/ for the Streamlit app.

Signs in interactively through the browser (no Azure CLI needed); the signed-in
account must hold Storage Blob Data Reader or Contributor on stwistiakcm.

    pip install azure-identity azure-storage-file-datalake pandas pyarrow
    python dashboards/sync_gold.py
"""
from pathlib import Path

from azure.identity import InteractiveBrowserCredential
from azure.storage.filedatalake import DataLakeServiceClient

TENANT_ID = "8a582c89-cbd9-43e5-a8f9-d704678fd315"   # Default Directory
STORAGE = "stwistiakcm"
CONTAINER = "gold"
PREFIX = "wistia"
DEST = Path(__file__).resolve().parent / "data"

TABLES = [
    "dim_media", "dim_date", "dim_visitor",
    "fact_media_daily", "fact_media_engagement", "fact_media_cumulative", "fact_engagement_curve",
]


def main() -> None:
    cred = InteractiveBrowserCredential(tenant_id=TENANT_ID)
    fs = DataLakeServiceClient(f"https://{STORAGE}.dfs.core.windows.net", credential=cred).get_file_system_client(CONTAINER)
    for table in TABLES:
        n = 0
        for p in fs.get_paths(path=f"{PREFIX}/{table}", recursive=True):
            if p.is_directory or not p.name.endswith(".parquet"):
                continue
            local = DEST / p.name.removeprefix(f"{PREFIX}/")
            local.parent.mkdir(parents=True, exist_ok=True)
            with open(local, "wb") as f:
                fs.get_file_client(p.name).download_file().readinto(f)
            n += 1
        print(f"{table}: {n} parquet file(s)")

    import pandas as pd
    for table in TABLES:
        df = pd.read_parquet(DEST / table)
        print(f"{table}: {len(df):,} rows")


if __name__ == "__main__":
    main()
