#!/usr/bin/env python3
"""
monitor_oaitun_http_fast.py

FIXES in this version:
1. --max now defaults to 100000 (was 2000 — was silently dropping all samples past that)
2. Non-blocking reads via select() — no more freezes on quiet periods
3. Auto-restart if tshark dies or stdout goes silent too long
4. Watchdog thread guards against frozen capture thread
"""

import subprocess
import sys
import argparse
import threading
import signal
import time
import csv
import re
import select
from collections import defaultdict, deque
from statistics import mean

# -----------------------
# Arguments
# -----------------------
parser = argparse.ArgumentParser(description="Fast HTTP RTT monitor via tshark -T fields")
parser.add_argument("--iface", "-i", default="auto")
parser.add_argument("--host", "-H", default="10.40.1.135")
parser.add_argument("--port", "-p", type=int, default=80)
# FIXED: was 2000 — silently capped samples at 2000 and discarded the rest
parser.add_argument("--max", type=int, default=100000,
                    help="Max RTT samples to keep in memory (default 100000)")
parser.add_argument("--log", "-l", default=None)
# How many seconds of silence before tshark is considered hung and restarted
parser.add_argument("--restart-timeout", type=int, default=30)
args = parser.parse_args()

USER_IFACE      = args.iface
HTTP_HOST       = args.host
HTTP_PORT       = args.port
MAX_RECENT      = args.max
LOG_PATH        = args.log
RESTART_TIMEOUT = args.restart_timeout

# -----------------------
# State
# -----------------------
running = True
lock    = threading.Lock()

outstanding     = defaultdict(deque)
rtt_samples     = deque(maxlen=MAX_RECENT)
total_requests  = 0
total_responses = 0

proc      = None
proc_lock = threading.Lock()
last_line_time = time.time()

# -----------------------
# Logging
# -----------------------
log_file   = None
log_writer = None
if LOG_PATH:
    log_file = open(LOG_PATH, "a", newline="", buffering=1)
    log_writer = csv.writer(log_file, delimiter="\t")
    if log_file.tell() == 0:
        log_writer.writerow(["timestamp_iso", "stream", "host", "uri", "response_code", "rtt_ms"])

# -----------------------
# Interface detection
# -----------------------
def detect_iface_for_host(host):
    try:
        p = subprocess.run(
            ["ip", "route", "get", host],
            capture_output=True, text=True, timeout=2
        )
        m = re.search(r"\bdev\s+(\S+)\b", p.stdout)
        return m.group(1) if m else None
    except Exception:
        return None

# -----------------------
# tshark fields
# -----------------------
FIELDS = [
    "frame.number",
    "frame.time_epoch",
    "ip.src",
    "ip.dst",
    "tcp.srcport",
    "tcp.dstport",
    "tcp.stream",
    "http.request.method",
    "http.request.uri",
    "http.response.code",
]
F_FNUM=0; F_TS=1; F_SRC=2;    F_DST=3;    F_SPORT=4
F_DPORT=5; F_STREAM=6; F_METHOD=7; F_URI=8; F_CODE=9

# -----------------------
# Packet handler
# -----------------------
def handle_line(cols):
    global total_requests, total_responses

    while len(cols) < len(FIELDS):
        cols.append("")

    fnum=cols[F_FNUM]; ts_str=cols[F_TS]; src=cols[F_SRC]; dst=cols[F_DST]
    sport=cols[F_SPORT]; dport=cols[F_DPORT]; stream=cols[F_STREAM]
    method=cols[F_METHOD]; uri=cols[F_URI]; code=cols[F_CODE]

    try:
        ts = float(ts_str)
    except ValueError:
        return

    tstr = time.strftime("%H:%M:%S", time.localtime(ts))

    if method:
        print(f"[{tstr}] FRAME={fnum} STREAM={stream} REQ  {src}:{sport} -> {dst}:{dport}  {method} {HTTP_HOST}{uri}")
        with lock:
            outstanding[stream].append({"ts": ts, "uri": uri})
            total_requests += 1
        return

    if code:
        print(f"[{tstr}] FRAME={fnum} STREAM={stream} RESP {src}:{sport} -> {dst}:{dport}  code={code}")
        with lock:
            if not outstanding[stream]:
                return
            req = outstanding[stream].popleft()
            rtt_ms = (ts - req["ts"]) * 1000
            if rtt_ms < 0:
                return
            rtt_samples.append(rtt_ms)
            total_responses += 1
            saved_uri = req["uri"]

        print(f"    -> MEASURE stream={stream} uri={saved_uri} code={code} RTT={rtt_ms:.2f} ms")

        if log_writer:
            iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ts))
            log_writer.writerow([iso, stream, HTTP_HOST, saved_uri, code, f"{rtt_ms:.2f}"])

# -----------------------
# Start tshark process
# -----------------------
def start_tshark(interface, bpf):
    global proc
    field_args = []
    for f in FIELDS:
        field_args += ["-e", f]

    cmd = [
        "tshark",
        "-i", interface,
        "-f", bpf,
#        "-B", "8"
	"-s", "200",
        "-T", "fields",
        "-E", "separator=\t",
        "-E", "occurrence=f",
        "-l",   # line-buffered stdout
        "-n",   # no name resolution (faster)
        "-q",   # suppress packet summary on stderr
    ] + field_args

    print(f"[capture] Starting tshark on '{interface}' filter='{bpf}'")
    p = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        bufsize=0,      # unbuffered at OS level — we handle line splitting ourselves
        text=False,     # raw bytes — avoids codec-level buffering
    )
    with proc_lock:
        proc = p
    return p

# -----------------------
# Capture loop — non-blocking reads via select()
# FIXED: was "for line in proc.stdout" which blocks indefinitely on silence
# -----------------------
def capture_loop(interface, bpf):
    global last_line_time

    buf = b""

    while running:
        p = start_tshark(interface, bpf)

        try:
            while running:
                # Wait up to 2s for data — never blocks indefinitely
                ready, _, _ = select.select([p.stdout], [], [], 2.0)

                if not ready:
                    # No data in 2s — check if tshark died
                    if p.poll() is not None:
                        print(f"[capture] tshark exited (code {p.returncode}), restarting...")
                        break
                    # Still alive, just quiet traffic — keep waiting
                    continue

                chunk = p.stdout.read(4096)
                if not chunk:
                    print("[capture] tshark stdout EOF, restarting...")
                    break

                last_line_time = time.time()
                buf += chunk

                # Process all complete lines accumulated in buffer
                while b"\n" in buf:
                    line_bytes, buf = buf.split(b"\n", 1)
                    line = line_bytes.decode("utf-8", errors="replace").rstrip("\r")
                    if not line:
                        continue
                    cols = line.split("\t")
                    if len(cols) > F_METHOD and (cols[F_METHOD] or (len(cols) > F_CODE and cols[F_CODE])):
                        handle_line(cols)

        except Exception as e:
            if running:
                print(f"[capture] exception: {e}, restarting...")
        finally:
            try:
                p.terminate()
                p.wait(timeout=3)
            except Exception:
                pass

        if running:
            print("[capture] restarting in 1s...")
            time.sleep(1)

# -----------------------
# Watchdog — restarts capture thread if it freezes or goes silent too long
# -----------------------
def watchdog_loop(interface, bpf, capture_thread_ref):
    global last_line_time
    while running:
        time.sleep(5)
        if not running:
            break

        if not capture_thread_ref[0].is_alive():
            print("[watchdog] capture thread died — restarting...")
            t = threading.Thread(target=capture_loop, args=(interface, bpf), daemon=True)
            t.start()
            capture_thread_ref[0] = t
            last_line_time = time.time()
            continue

        silence = time.time() - last_line_time
        if silence > RESTART_TIMEOUT:
            print(f"[watchdog] {silence:.0f}s silence — killing tshark to force restart...")
            with proc_lock:
                if proc:
                    try:
                        proc.kill()
                    except Exception:
                        pass
            last_line_time = time.time()

# -----------------------
# Shutdown
# -----------------------
def stop_and_exit(sig=None, frame=None):
    global running
    running = False

    with proc_lock:
        if proc:
            try:
                proc.terminate()
                proc.wait(timeout=3)
            except Exception:
                pass

    if log_file:
        log_file.flush()
        log_file.close()

    print("\n=== FINAL RTT SUMMARY ===")
    print(f"Requests : {total_requests}")
    print(f"Responses: {total_responses}")
    if rtt_samples:
        vals = list(rtt_samples)
        print(f"Samples  : {len(vals)}")
        print(f"Avg RTT  : {mean(vals):.2f} ms")
        print(f"Min RTT  : {min(vals):.2f} ms")
        print(f"Max RTT  : {max(vals):.2f} ms")
    else:
        print("No RTT samples collected.")

    sys.exit(0)

signal.signal(signal.SIGINT,  stop_and_exit)
signal.signal(signal.SIGTERM, stop_and_exit)

# -----------------------
# Main
# -----------------------
iface = (
    detect_iface_for_host(HTTP_HOST) if USER_IFACE == "auto"
    else USER_IFACE if USER_IFACE != "any"
    else "any"
) or "any"

bpf = f"tcp port {HTTP_PORT} and host {HTTP_HOST}"
print(f"Monitoring {HTTP_HOST}:{HTTP_PORT} on interface '{iface}'")
print(f"Max samples in memory : {MAX_RECENT}  (override with --max N)")
print(f"Silence restart timeout: {RESTART_TIMEOUT}s  (override with --restart-timeout N)")

capture_thread_ref = [None]
t = threading.Thread(target=capture_loop, args=(iface, bpf), daemon=True)
t.start()
capture_thread_ref[0] = t

threading.Thread(target=watchdog_loop, args=(iface, bpf, capture_thread_ref), daemon=True).start()

while running:
    time.sleep(1)