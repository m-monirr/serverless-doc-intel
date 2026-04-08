import json
import mimetypes
import time
import uuid
from pathlib import Path
from urllib import error, request

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


def get_json(path: str) -> dict:
    req = request.Request(f"{BASE_URL}{path}", method="GET")
    with request.urlopen(req, timeout=20) as resp:
        data = resp.read().decode("utf-8")
    return json.loads(data)


def post_pdf(pdf_path: Path) -> tuple[int, dict]:
    boundary = f"----WebKitFormBoundary{uuid.uuid4().hex}"
    mime = mimetypes.guess_type(str(pdf_path))[0] or "application/pdf"
    file_bytes = pdf_path.read_bytes()

    body = bytearray()
    body.extend(f"--{boundary}\r\n".encode())
    body.extend(
        (
            f'Content-Disposition: form-data; name="file"; filename="{pdf_path.name}"\r\n'
            f"Content-Type: {mime}\r\n\r\n"
        ).encode()
    )
    body.extend(file_bytes)
    body.extend(f"\r\n--{boundary}--\r\n".encode())

    req = request.Request(f"{BASE_URL}/ingest", method="POST", data=bytes(body))
    req.add_header("Content-Type", f"multipart/form-data; boundary={boundary}")

    try:
        with request.urlopen(req, timeout=60) as resp:
            text = resp.read().decode("utf-8")
            return resp.status, json.loads(text)
    except error.HTTPError as exc:
        text = exc.read().decode("utf-8")
        parsed = json.loads(text) if text else {"raw": text}
        return exc.code, parsed


def is_rate_limited(payload: dict) -> bool:
    if payload.get("error") == "Rate limit exceeded":
        return True
    detail = payload.get("detail")
    return isinstance(detail, dict) and detail.get("error") == "Rate limit exceeded"


def main() -> None:
    sample = TESTS_DIR / "sample.pdf"
    sample2 = TESTS_DIR / "sample2.pdf"
    sample3 = TESTS_DIR / "sample3.pdf"
    sample4 = TESTS_DIR / "sample4.pdf"

    ensure_pdf(sample, "Sample PDF One")
    ensure_pdf(sample2, "Sample PDF Two")
    ensure_pdf(sample3, "Sample PDF Three")
    ensure_pdf(sample4, "Sample PDF Four")

    checks = []

    health = get_json("/health")
    checks.append(f"health: PASS {health}")

    c1, first = post_pdf(sample)
    checks.append(f"ingest1: {'PASS' if c1 == 200 and first.get('job_id') else 'FAIL'} status={c1} payload={first}")

    done = False
    status_payload = {}
    if first.get("job_id"):
        for _ in range(30):
            status_payload = get_json(f"/status/{first['job_id']}")
            if status_payload.get("status") == "done":
                done = True
                break
            time.sleep(0.25)
    checks.append(f"status_done: {'PASS' if done else 'FAIL'} {status_payload}")

    if first.get("job_id"):
        result = get_json(f"/result/{first['job_id']}")
        checks.append(f"result_ready: {'PASS' if bool(result.get('abstract')) else 'FAIL'}")

    quota1 = get_json("/quota")
    checks.append(f"quota_after_first: PASS {quota1}")

    c2, cached = post_pdf(sample)
    checks.append(f"md5_cached: {'PASS' if c2 == 200 and cached.get('cached') is True else 'FAIL'} status={c2} payload={cached}")

    c3, second_distinct = post_pdf(sample2)
    checks.append(
        f"second_distinct_upload: {'PASS' if c3 == 200 and second_distinct.get('job_id') else 'FAIL'} status={c3} payload={second_distinct}"
    )

    c4, third_distinct = post_pdf(sample3)
    checks.append(
        f"third_distinct_rate_limited: {'PASS' if c4 == 429 and is_rate_limited(third_distinct) else 'FAIL'} status={c4} payload={third_distinct}"
    )

    c5, fourth_distinct = post_pdf(sample4)
    checks.append(
        f"fourth_distinct_rate_limited: {'PASS' if c5 == 429 and is_rate_limited(fourth_distinct) else 'FAIL'} status={c5} payload={fourth_distinct}"
    )

    quota2 = get_json("/quota")
    checks.append(f"quota_final: PASS {quota2}")

    print("\n".join(checks))


if __name__ == "__main__":
    main()
