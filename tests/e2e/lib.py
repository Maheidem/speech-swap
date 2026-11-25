"""Small helpers for the M2 e2e scripts (Python standard library only)."""
import json
import re
import subprocess
import threading
import time
import urllib.error
import urllib.request
import uuid


def request(method, url, key=None, body=None, headers=None, timeout=600):
    """Returns (status, headers, body bytes). A 4xx or 5xx answer is not an exception."""
    req = urllib.request.Request(url, data=body, method=method)
    if key:
        req.add_header("Authorization", "Bearer " + key)
    for k, v in (headers or {}).items():
        req.add_header(k, v)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, dict(r.headers), r.read()
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers), e.read()


def multipart(fields, file_path, file_name="audio.wav"):
    """Builds a multipart body. `fields` is a list of (name, value) pairs; a name can repeat."""
    boundary = uuid.uuid4().hex
    parts = []
    for name, value in fields:
        parts.append(f'--{boundary}\r\nContent-Disposition: form-data; name="{name}"\r\n\r\n{value}\r\n'.encode())
    with open(file_path, "rb") as f:
        data = f.read()
    parts.append(f'--{boundary}\r\nContent-Disposition: form-data; name="file"; filename="{file_name}"\r\n'
                 f'Content-Type: application/octet-stream\r\n\r\n'.encode() + data + b"\r\n")
    parts.append(f"--{boundary}--\r\n".encode())
    return b"".join(parts), {"Content-Type": f"multipart/form-data; boundary={boundary}"}


def post_audio(hub, key, path, fields, file_path, timeout=900):
    body, headers = multipart(fields, file_path)
    return request("POST", hub + path, key, body, headers, timeout)


def metric_sum(hub, key, name):
    """The sum of all samples of one metric family (for example ssw_swaps_total)."""
    _, _, text = request("GET", hub + "/metrics", key, timeout=20)
    total = 0.0
    for line in text.decode().splitlines():
        if line.startswith(name + "{") or line.startswith(name + " "):
            total += float(line.rsplit(" ", 1)[1])
    return total


def swap_used_mb():
    """`vm.swapusage` used value in MB. The M1 writes a decimal comma."""
    out = subprocess.run(["sysctl", "-n", "vm.swapusage"], capture_output=True, text=True).stdout
    m = re.search(r"used = ([\d.,]+)M", out)
    return float(m.group(1).replace(",", ".")) if m else None


def free_pct():
    out = subprocess.run(["memory_pressure"], capture_output=True, text=True).stdout
    m = re.search(r"free percentage: (\d+)%", out)
    return int(m.group(1)) if m else None


class MemWatch:
    """Samples the free memory percentage in a thread. `min` is the lowest value seen."""

    def __init__(self, every=2.0):
        self.every, self.min, self.samples = every, None, []
        self._stop = threading.Event()
        self._t = threading.Thread(target=self._run, daemon=True)

    def _run(self):
        while not self._stop.is_set():
            v = free_pct()
            if v is not None:
                self.samples.append(v)
                self.min = v if self.min is None else min(self.min, v)
            self._stop.wait(self.every)

    def __enter__(self):
        self._t.start()
        return self

    def __exit__(self, *a):
        self._stop.set()
        self._t.join()


def percentile(values, p):
    v = sorted(values)
    if not v:
        return None
    k = (len(v) - 1) * p
    lo, hi = int(k), min(int(k) + 1, len(v) - 1)
    return v[lo] + (v[hi] - v[lo]) * (k - lo)
