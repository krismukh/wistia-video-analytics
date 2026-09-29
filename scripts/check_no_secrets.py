"""Fail the build if anything resembling the Wistia API token or a storage key is committed."""
import re
import subprocess
import sys

PATTERNS = [
    (r"\b[0-9a-f]{64}\b", "64-hex token"),
    (r"AccountKey=[A-Za-z0-9+/=]{20,}", "storage account key"),
    (r"Authorization:\s*Bearer\s+[0-9a-f]{20,}", "bearer token literal"),
]
files = subprocess.run(["git", "ls-files"], capture_output=True, text=True, check=True).stdout.split()
bad = []
for f in files:
    if f.endswith((".png", ".jpg", ".pdf", ".docx", ".drawio", ".parquet")):
        continue
    try:
        text = open(f, encoding="utf-8", errors="ignore").read()
    except OSError:
        continue
    for pat, label in PATTERNS:
        for m in re.finditer(pat, text):
            bad.append(f"{f}: {label} ({m.group(0)[:12]}...)")
if bad:
    print("Secret-like strings found:\n  " + "\n  ".join(bad))
    sys.exit(1)
print("no secrets found in", len(files), "tracked files")
