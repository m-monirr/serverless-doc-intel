import json
import os
import time
from pathlib import Path

import requests

BASE_URL = "http://127.0.0.1:8001"
TESTS_DIR = Path(__file__).parent


def ensure_pdf(path: Path, label: str) -> None:
    if path.exists():
        return
    content = f"""%PDF-1.4
1 0 obj
<< /Type /Catalog /Pages 2 0 R >>
endobj
2 0 obj
<< /Type /Pages /Kids [3 0 R] /Count 1 >>
endobj
3 0 obj
<< /Type /Page /Parent 2 0 R /MediaBox [0 0 300 144] /Contents 4 0 R /Resources << /Font << /F1 5 0 R >> >> >>
endobj
4 0 obj
<< /Length 48 >>
stream
BT /F1 18 Tf 30 90 Td ({label}) Tj ET
endstream
endobj
5 0 obj
<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>
endobj
xref
0 6
0000000000 65535 f 
0000000009 00000 n 
0000000058 00000 n 
0000000115 00000 n 
0000000241 00000 n 
0000000338 00000 n 
trailer
<< /Size 6 /Root 1 0 R >>
startxref
408
%%EOF
"""
    path.write_text(content, encoding="utf-8")


def upload(pdf_path: Path) -> dict:
    with pdf_path.open("rb") as f:
        r = requests.post(f"{BASE_URL}/ingest", files={"file": (pdf_path.name, f, "application/pdf")}, timeout=60)
    try:
        return r.json()
    except Exception:
        return {"status_code": r.status_code, "raw": r.text}


def main() -> None:
    sample = TESTS_DIR / "sample.pdf"
    sample2 = TESTS_DIR / "sample2.pdf"
    sample3 = TESTS_DIR / "sample3.pdf"
    sample4 = TESTS_DIR / "sample4.pdf"

    ensure_pdf(sample, "Sample PDF One")
    ensure_pdf(sample2, "Sample PDF Two")
    ensure_pdf(sample3, "Sample PDF Three")
    ensure_pdf(sample4, "Sample PDF Four")

    out = []

    health = requests.get(f"{BASE_URL}/health", timeout=20).json()
    out.append(f"health: PASS status={health.get('status')}")

    ing1 = upload(sample)
    out.append(f"ingest1: {'PASS' if ing1.get('job_id') else 'FAIL'} {json.dumps(ing1)}")

    done = False
    status = {}
    if ing1.get("job_id"):
        for _ in range(30):
            status = requests.get(f"{BASE_URL}/status/{ing1['job_id']}", timeout=20).json()
            if status.get("status") == "done":
                done = True
                break
            time.sleep(0.25)
    out.append(f"status_done: {'PASS' if done else 'FAIL'} {json.dumps(status)}")

    if ing1.get("job_id"):
        res = requests.get(f"{BASE_URL}/result/{ing1['job_id']}", timeout=20).json()
        out.append(f"result: {'PASS' if res.get('abstract') else 'FAIL'} has_abstract={bool(res.get('abstract'))}")

    quota1 = requests.get(f"{BASE_URL}/quota", timeout=20).json()
    out.append(f"quota_after_first: PASS {json.dumps(quota1)}")

    cached = upload(sample)
    out.append(f"md5_cached: {'PASS' if cached.get('cached') is True else 'FAIL'} {json.dumps(cached)}")

    up2 = upload(sample2)
    out.append(f"upload_sample2: {'PASS' if up2.get('job_id') else 'FAIL'} {json.dumps(up2)}")

    up3 = upload(sample3)
    rate_limited = False
    if isinstance(up3.get("detail"), dict) and up3["detail"].get("error") == "Rate limit exceeded":
        rate_limited = True
    if up3.get("error") == "Rate limit exceeded":
        rate_limited = True
    out.append(f"rate_limit_third_distinct: {'PASS' if rate_limited else 'FAIL'} {json.dumps(up3)}")

    up4 = upload(sample4)
    out.append(f"upload_sample4_after_limit: PASS {json.dumps(up4)}")

    quota2 = requests.get(f"{BASE_URL}/quota", timeout=20).json()
    out.append(f"quota_final: PASS {json.dumps(quota2)}")

    print("\n".join(out))


if __name__ == "__main__":
    main()
