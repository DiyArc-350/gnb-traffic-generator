import os
import requests
import time
from concurrent.futures import ThreadPoolExecutor
import threading
import random
import socket
import struct
import fcntl
import argparse

# Lock for thread synchronization when calculating statistics
stats_lock = threading.Lock()
stats = {
    "total_rtt": 0,
    "total_up": 0,
    "total_ai": 0,
    "total_net": 0,
    "total_size_kb": 0,
    "count": 0
}

def parse_arguments():
    parser = argparse.ArgumentParser(description="URLLC Traffic Generator & Performance Analyzer")

    # Connection Settings
    parser.add_argument("--host", "-H", default="10.40.1.220")
    parser.add_argument("--endpoint", "-e", default="/upload/")
    parser.add_argument("--interface", "-i", default="oaitun_ue1")
    parser.add_argument("--input", default="./input_video",
                        help="Folder containing input videos")

    return parser.parse_args()

def get_interface_ip(interface):
    """Get the current IP address assigned to a network interface."""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    return socket.inet_ntoa(fcntl.ioctl(
        s.fileno(), 0x8915,  # SIOCGIFADDR
        struct.pack('256s', interface[:15].encode())
    )[20:24])

def upload_file(request_id, file_path, server_url, session, log_filename):
    filename = os.path.basename(file_path)
    file_size_kb = os.path.getsize(file_path) / 1024
    
    start_rtt = time.time()
    
    try:
        with open(file_path, 'rb') as f:
            files = {'file': (filename, f)}
            response = session.post(server_url, files=files, timeout=60)
        
        end_rtt = time.time()
        rtt_ms = (end_rtt - start_rtt) * 1000

        if response.status_code == 200:
            data = response.json()
            
            up_ms = data.get("upload_time", 0) * 1000
            ai_ms = data.get("inference_time", 0) * 1000
            net_ms = max(0, rtt_ms - up_ms - ai_ms)
            upload_speed_kbs = file_size_kb / (up_ms / 1000) if up_ms > 0 else 0
            detected = data.get('detected', [])

            log_entry = (f"[{request_id:03d}] [OK] {filename[:20]:<20} | {file_size_kb:8.1f} KB | "
                         f"RTT: {rtt_ms:7.1f}ms | Up: {up_ms:6.1f}ms | AI: {ai_ms:6.1f}ms | "
                         f"Speed: {upload_speed_kbs:8.2f} KB/s | Det: {detected}")
            
            with stats_lock:
                stats["total_rtt"] += rtt_ms
                stats["total_up"] += up_ms
                stats["total_ai"] += ai_ms
                stats["total_net"] += net_ms
                stats["total_size_kb"] += file_size_kb
                stats["count"] += 1
        else:
            log_entry = f"[{request_id:03d}] [ERR] {filename[:20]:<20} | Status: {response.status_code} | Msg: {response.text[:30]}"
            
    except Exception as e:
        log_entry = f"[{request_id:03d}] [FAIL] {filename[:20]:<20} | Error: {str(e)[:40]}"

    print(log_entry)
    with open(log_filename, "a") as f:
        f.write(log_entry + "\n")

def main():
    args = parse_arguments()

    print("=== URLLC TRAFFIC GENERATOR & PERFORMANCE ANALYZER ===")

    # Resolve IP from interface at runtime
    try:
        source_ip = get_interface_ip(args.interface)
    except OSError:
        print(f"Error: Could not get IP for interface '{args.interface}'. Is it up?")
        return

    print(f"🔗 Binding to {args.interface} ({source_ip})")

    # Prompt for required traffic settings
    try:
        num_requests = int(input("Total upload requests     : "))
        rps = float(input("Requests per second (RPS) : "))
        log_filename = input("Log file name (.txt)      : ") or "traffic_log.txt"
    except ValueError:
        print("Invalid input. Please enter numerical values.")
        return

    server_url = f"http://{args.host}{args.endpoint}"
    os.makedirs(args.input, exist_ok=True)

    video_extensions = ('.mp4', '.avi', '.mov')
    videos = [os.path.join(args.input, f) for f in os.listdir(args.input) if f.lower().endswith(video_extensions)]

    if not videos:
        print(f"Error: No videos found in {args.input}!")
        return

    # Initialize log file with header
    with open(log_filename, "w") as f:
        f.write(f"=== URLLC TEST LOG: {time.ctime()} ===\n")
        f.write(f"{'ID':<6} | {'FILENAME':<20} | {'SIZE (KB)':<10} | {'RTT':<9} | {'UP':<8} | {'AI':<8} | {'SPEED':<12} | DETECTED\n")
        f.write("-" * 155 + "\n")

    # Bind all outgoing connections to the resolved interface IP
    _orig_create_connection = socket.create_connection
    def _bound_create_connection(address, timeout=socket.getdefaulttimeout(), source_address=None):
        return _orig_create_connection(address, timeout, source_address=(source_ip, 0))
    socket.create_connection = _bound_create_connection

    session = requests.Session()
    executor = ThreadPoolExecutor(max_workers=5)

    print(f"🚀 Launching {num_requests} requests at {rps} RPS against: {server_url}")
    print("\n" + "="*155)
    print(f"{'ID':<6} | {'FILENAME':<20} | {'SIZE (KB)':<10} | {'RTT':<9} | {'UP':<8} | {'AI':<8} | {'SPEED (KB/s)':<12} | DETECTED")
    print("-" * 155)

    start_sim = time.time()
    for i in range(1, num_requests + 1):
        chosen_video = random.choice(videos)
        executor.submit(upload_file, i, chosen_video, server_url, session, log_filename)
        if rps > 0:
            time.sleep(1 / rps)

    executor.shutdown(wait=True)
    end_sim = time.time()

    # FINAL SUMMARY CALCULATION
    if stats["count"] > 0:
        avg_rtt = stats["total_rtt"] / stats["count"]
        avg_up = stats["total_up"] / stats["count"]
        avg_ai = stats["total_ai"] / stats["count"]
        avg_net = stats["total_net"] / stats["count"]
        total_up_sec = stats["total_up"] / 1000
        avg_speed = stats["total_size_kb"] / total_up_sec if total_up_sec > 0 else 0

        summary = (
            f"\n" + "="*60 + "\n"
            f" FINAL STATISTICAL SUMMARY (N={stats['count']})\n"
            f" " + "-"*58 + "\n"
            f" Average RTT              : {avg_rtt:10.2f} ms\n"
            f" Average Upload Delay     : {avg_up:10.2f} ms\n"
            f" Average AI Inference     : {avg_ai:10.2f} ms\n"
            f" Average Net Latency      : {avg_net:10.2f} ms\n"
            f" Average Upload Speed     : {avg_speed:10.2f} KB/s\n"
            f" Total Data Transferred   : {stats['total_size_kb']/1024:10.2f} MB\n"
            f" Simulation Duration      : {end_sim - start_sim:10.2f} seconds\n"
            f" " + "="*60 + "\n"
        )

        print(summary)
        with open(log_filename, "a") as f:
            f.write(summary)

if __name__ == "__main__":
    main()