import os
import time
import logging
import random
import socket
import struct
import fcntl
import argparse
import threading
from concurrent.futures import ThreadPoolExecutor
import cv2
import websocket  # pip install websocket-client

# ============================================================
# GLOBAL STATISTICS
# ============================================================
stats_lock = threading.Lock()

stats = {
    "total_rtt": 0.0,
    "total_ai_reported": 0.0,
    "total_network_transit": 0.0,
    "total_bytes_sent": 0.0,
    "count": 0
}

# ============================================================
# ARGUMENT PARSER
# ============================================================
def parse_arguments():
    parser = argparse.ArgumentParser(
        description="URLLC WebSocket Traffic Generator & Performance Analyzer"
    )
    parser.add_argument("--host", "-H", default="10.40.1.220")
    parser.add_argument("--port", "-p", default="80") # Routed directly via public Nginx Port 80
    parser.add_argument("--endpoint", "-e", default="/stream/yolo") # Explicit route to NanoDet Node
    parser.add_argument("--interface", "-i", default="oaitun_ue1")
    parser.add_argument("--input", default="../input_video", help="Folder containing input videos")
    parser.add_argument("--workers", "-w", type=int, default=1, help="Maximum concurrent streaming pipelines")
    return parser.parse_args()

# ============================================================
# GET INTERFACE IP
# ============================================================
def get_interface_ip(interface):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    return socket.inet_ntoa(
        fcntl.ioctl(
            s.fileno(),
            0x8915,  # SIOCGIFADDR
            struct.pack('256s', interface[:15].encode())
        )[20:24]
    )

# ============================================================
# CREATE SOURCE-BOUND CONNECTION
# ============================================================
def patch_socket_source_ip(source_ip):
    """Forces WebSocket underlying connections to bind cleanly to the URLLC interface"""
    original_create_connection = socket.create_connection
    def bound_create_connection(address, timeout=socket.getdefaulttimeout(), source_address=None):
        return original_create_connection(address, timeout, source_address=(source_ip, 0))
    socket.create_connection = bound_create_connection

# ============================================================
# STREAMING PIPELINE WORKER FUNCTION
# ============================================================
def stream_video_pipeline(worker_id, video_path, ws_url, log_filename, target_size_mode, max_frames=None):
    filename = os.path.basename(video_path)
    cap = cv2.VideoCapture(video_path)
    
    try:
        # Establish a single, persistent real-time socket link
        ws = websocket.create_connection(
            ws_url, 
            timeout=10
        )
    except Exception as e:
        err_msg = f"[{worker_id:03d}] [CONN_FAIL] Could not connect to stream socket: {e}"
        print(err_msg)
        return

    frame_index = 0
    
    try:
        while cap.isOpened():
            # If max_frames is set and we've reached it, break out
            if max_frames is not None and frame_index >= max_frames:
                break

            success, frame = cap.read()
            if not success:
                break
                
            frame_index += 1
            
            # Step A: Downscale raw resolution matrix to save memory space baseline
            resized_frame = cv2.resize(frame, (160, 160), interpolation=cv2.INTER_AREA)
            
            # ==============================================================================
            # CONSTANT COMPRESSION ENGINE: Strict Upper Ceiling Limit
            # ==============================================================================
            if target_size_mode == "random":
                chosen_kb = random.randint(1, 5)
            else:
                chosen_kb = int(target_size_mode)
                
            # Convert chosen target strictly to bytes limit
            strict_max_bytes = chosen_kb * 1024
            
            binary_bytes = b""
            byte_size = 0
            
            # Binary search style search across JPEG quality matrix (1 to 100)
            # to find the highest quality that stays underneath the strict byte limit.
            low_q, high_q = 1, 100
            best_quality = 50
            
            for attempt in range(6):  # 6 steps are mathematically enough to settle the quality factor
                mid_q = (low_q + high_q) // 2
                _, encoded_img = cv2.imencode('.jpg', resized_frame, [cv2.IMWRITE_JPEG_QUALITY, mid_q])
                temp_bytes = encoded_img.tobytes()
                temp_size = len(temp_bytes)
                
                if temp_size <= strict_max_bytes:
                    # It fits! Save it as our best option so far and try to increase quality
                    best_quality = mid_q
                    binary_bytes = temp_bytes
                    byte_size = temp_size
                    low_q = mid_q + 1
                else:
                    # Too large! Lower the quality range window
                    high_q = mid_q - 1
            
            # Fallback protection if even Quality=1 failed strict limit criteria
            if not binary_bytes:
                _, encoded_img = cv2.imencode('.jpg', resized_frame, [cv2.IMWRITE_JPEG_QUALITY, 1])
                binary_bytes = encoded_img.tobytes()
                byte_size = len(binary_bytes)
            # ==============================================================================
            
            # Start high-precision timing round-trip loop
            start_rtt = time.perf_counter()
            
            # Transmit raw binary byte block down the persistent pipe
            ws.send_binary(binary_bytes)
            
            # Block and await the instant response back from the server
            response_raw = ws.recv()
            end_rtt = time.perf_counter()
            
            rtt_ms = (end_rtt - start_rtt) * 1000
            
            try:
                import json
                data = json.loads(response_raw)
            except Exception:
                data = {}
                
            if data.get("status") == "success":
                ai_ms = data.get("inference_time_ms", 0.0)
                network_transit_ms = max(0.0, rtt_ms - ai_ms)
                detected = data.get("detected", [])
                
                log_entry = (
                    f"[{worker_id:03d}-F{frame_index:04d}] [OK] {filename[:12]:<12} | "
                    f"Payload: {byte_size:,} Bytes ({byte_size/1024:4.2f} KB) | "
                    f"Max Limit: {chosen_kb}KB | "
                    f"RTT: {rtt_ms:6.1f}ms | "
                    f"Server_AI: {ai_ms:5.1f}ms | "
                    f"Net_Transit: {network_transit_ms:6.1f}ms | "
                    f"Det: {detected}"
                )
                print(log_entry)
                
                with open(log_filename, "a") as f:
                    f.write(log_entry + "\n")
                    
                with stats_lock:
                    stats["total_rtt"] += rtt_ms
                    stats["total_ai_reported"] += ai_ms
                    stats["total_network_transit"] += network_transit_ms
                    stats["total_bytes_sent"] += byte_size
                    stats["count"] += 1
                    
            else:
                print(f"[{worker_id:03d}-F{frame_index:04d}] [SERVER_ERR] {data.get('message', 'Unknown Error')}")
                
    except Exception as e:
        print(f"[{worker_id:03d}] [PIPE_BREAK] Pipeline exception encountered: {e}")
    finally:
        cap.release()
        try:
            ws.close()
        except Exception:
            pass

# ============================================================
# MAIN
# ============================================================
def main():
    args = parse_arguments()
    print("\n=== URLLC REAL-TIME WEBSOCKET TRAFFIC GENERATOR (NANODET) ===\n")

    # Resolve interface IP
    try:
        source_ip = get_interface_ip(args.interface)
    except OSError:
        print(f"ERROR: Could not get IP for interface '{args.interface}'")
        return

    print(f"Binding to interface : {args.interface}")
    print(f"Source IP            : {source_ip}")

    # --- THREE INTERACTIVE PROMPTS ---
    try:
        num_sessions = int(input("\nTotal stream sessions to run       : "))
        
        print("\nFrame Size Configurations:")
        print("  - Choose a constant max value: 1, 2, 3, 4, or 5 (in KB)")
        print("  - Or type 'random' to choose a new constant limit per frame")
        target_size_mode = input("Select Frame Size Mode             : ").strip().lower()
        
        if target_size_mode not in ["1", "2", "3", "4", "5", "random"]:
            print("ERROR: Invalid selection. Choose 1-5 or 'random'.")
            return

        log_input = input("\nLog file name (.txt)               : ") or "nanodet_websocket_log.txt"
        # Define your target directory here (e.g., a folder named 'logs' in the current directory)
        log_dir = "../logs" 
        os.makedirs(log_dir, exist_ok=True) # Automatically creates the folder if it doesn't exist
        
        # Combine the directory and the filename safely
        log_filename = os.path.join(log_dir, log_input)
    except ValueError:
        print("ERROR: Invalid input configuration received.")
        return

    ws_url = f"ws://{args.host}:{args.port}{args.endpoint}"
    print(f"Server WebSocket URL : {ws_url}")

    os.makedirs(args.input, exist_ok=True)
    video_extensions = (".mp4", ".avi", ".mov", ".mkv")
    # Sort the files alphabetically so the sequence is predictable
    videos = sorted([
        os.path.join(args.input, f)
        for f in os.listdir(args.input)
        if f.lower().endswith(video_extensions)
    ])

    if not videos:
        print(f"ERROR: No source video files found inside target dir: {args.input}")
        return

    with open(log_filename, "w") as f:
        f.write(f"=== URLLC REAL-TIME NANODET WEBSOCKET TEST LOG ===\n")
        f.write(f"Timestamp   : {time.ctime()}\nServer      : {ws_url}\nInterface   : {args.interface}\n")
        f.write(f"Size Mode   : Strict Max Constant {target_size_mode} KB\n\n")
        f.write("-" * 130 + "\n")

    patch_socket_source_ip(source_ip)

    executor = ThreadPoolExecutor(max_workers=args.workers)
    print(f"\nSpawning {num_sessions} pipelines across {args.workers} workers...\n")

    start_sim = time.perf_counter()

    for i in range(1, num_sessions + 1):
        # LOOPING THROUGH FILE DIRECTORY PREDICTABLY (Wrapping around via modulo)
        chosen_video = videos[(i - 1) % len(videos)]
        executor.submit(
            stream_video_pipeline,
            i,
            chosen_video,
            ws_url,
            log_filename,
            target_size_mode,
            max_frames=None  # Explicitly set to None to process ALL frames in the video file
        )

    executor.shutdown(wait=True)
    end_sim = time.perf_counter()

    if stats["count"] > 0:
        avg_rtt = stats["total_rtt"] / stats["count"]
        avg_ai = stats["total_ai_reported"] / stats["count"]
        avg_net = stats["total_network_transit"] / stats["count"]
        total_kb = stats["total_bytes_sent"] / 1024
        duration = end_sim - start_sim

        summary = (
            "\n" + "=" * 70 + "\n"
            f" FINAL REAL-TIME WEBSOCKET METRIC SUMMARY (Total Frames Processed={stats['count']})\n"
            + "-" * 70 + "\n"
            f"Average Frame RTT         : {avg_rtt:10.2f} ms\n"
            f"Average Server AI Process : {avg_ai:10.2f} ms\n"
            f"Average Network Transit   : {avg_net:10.2f} ms (Wire Propagation + Protocol)\n"
            f"Total Data Transmitted    : {total_kb/1024:10.2f} MB\n"
            f"Total Duration            : {duration:10.2f} sec\n"
            f"Frame Processing Rate     : {stats['count']/duration:10.2f} frames/sec\n"
            + "=" * 70 + "\n"
        )
        print(summary)
        with open(log_filename, "a") as f:
            f.write(summary)
    else:
        print("\nNo successful streaming frames completed.")

if __name__ == "__main__":
    main()