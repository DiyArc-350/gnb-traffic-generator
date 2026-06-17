# ============================================================
# STREAMING PIPELINE WORKER FUNCTION (Updated for Dynamic Sizes)
# ============================================================
def stream_video_pipeline(worker_id, video_path, ws_url, log_filename, target_size_mode, max_frames=30):
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
        while cap.isOpened() and frame_index < max_frames:
            success, frame = cap.read()
            if not success:
                break
                
            frame_index += 1
            
            # Step A: Downscale raw resolution matrix to save memory space baseline
            resized_frame = cv2.resize(frame, (160, 160), interpolation=cv2.INTER_AREA)
            
            # ==============================================================================
            # DYNAMIC COMPRESSION ENGINE: Configured via prompt
            # ==============================================================================
            if target_size_mode == "random":
                # Pick a random integer target boundary between 1 KB and 5 KB for this specific frame
                chosen_kb = random.randint(1, 5)
                target_min_bytes = (chosen_kb - 0.5) * 1024
                target_max_bytes = (chosen_kb + 0.5) * 1024
            else:
                # Use the fixed KB size selected by the user
                chosen_kb = int(target_size_mode)
                target_min_bytes = (chosen_kb - 0.5) * 1024
                target_max_bytes = (chosen_kb + 0.5) * 1024
            
            jpeg_quality = 50  # Start balanced
            binary_bytes = b""
            byte_size = 0
            
            # Loop dynamically adjusts quantization to force payload into targeted limits
            for attempt in range(5):
                _, encoded_img = cv2.imencode('.jpg', resized_frame, [cv2.IMWRITE_JPEG_QUALITY, jpeg_quality])
                binary_bytes = encoded_img.tobytes()
                byte_size = len(binary_bytes)
                
                if byte_size > target_max_bytes:
                    jpeg_quality -= 15  # Compress harder
                    jpeg_quality = max(5, jpeg_quality)
                elif byte_size < target_min_bytes:
                    jpeg_quality += 15  # Increase details slightly
                    jpeg_quality = min(95, jpeg_quality)
                else:
                    break  # Success! Payload falls perfectly inside boundaries
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
                    f"[{worker_id:03d}-F{frame_index:02d}] [OK] {filename[:12]:<12} | "
                    f"Payload: {byte_size:,} Bytes ({byte_size/1024:4.2f} KB) | "
                    f"Target: {chosen_kb}KB | "
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
                print(f"[{worker_id:03d}-F{frame_index:02d}] [SERVER_ERR] {data.get('message', 'Unknown Error')}")
                
    except Exception as e:
        print(f"[{worker_id:03d}] [PIPE_BREAK] Pipeline exception encountered: {e}")
    finally:
        cap.release()
        try:
            ws.close()
        except Exception:
            pass

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
    parser.add_argument("--input", default="./input_video", help="Folder containing input videos")
    parser.add_argument("--workers", "-w", type=int, default=1, help="Maximum concurrent streaming pipelines")
    return parser.parse_args()


# ============================================================
# MAIN (Updated with 3rd Prompt)
# ============================================================
def main():
    args = parse_arguments()
    print("\n=== URLLC REAL-TIME WEBSOCKET TRAFFIC GENERATOR (NANODET) ===\n")

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
        
        # New 3rd Prompt for customizing file size constraint boundaries
        print("\nFrame Size Configurations:")
        print("  - Choose fixed value: 1, 2, 3, 4, or 5 (in KB)")
        print("  - Or type 'random' to fluctuate between 1KB-5KB per frame")
        target_size_mode = input("Select Frame Size Mode             : ").strip().lower()
        
        if target_size_mode not in ["1", "2", "3", "4", "5", "random"]:
            print("ERROR: Invalid selection. Choose 1-5 or 'random'.")
            return

        log_filename = input("\nLog file name (.txt)               : ") or "nanodet_websocket_log.txt"
    except ValueError:
        print("ERROR: Invalid input configuration received.")
        return

    ws_url = f"ws://{args.host}:{args.port}{args.endpoint}"
    print(f"Server WebSocket URL : {ws_url}")

    os.makedirs(args.input, exist_ok=True)
    video_extensions = (".mp4", ".avi", ".mov", ".mkv")
    videos = [
        os.path.join(args.input, f)
        for f in os.listdir(args.input)
        if f.lower().endswith(video_extensions)
    ]

    if not videos:
        print(f"ERROR: No source video files found inside target dir: {args.input}")
        return

    with open(log_filename, "w") as f:
        f.write(f"=== URLLC REAL-TIME NANODET WEBSOCKET TEST LOG ===\n")
        f.write(f"Timestamp   : {time.ctime()}\nServer      : {ws_url}\nInterface   : {args.interface}\n")
        f.write(f"Size Mode   : {target_size_mode} KB\n\n")
        f.write("-" * 130 + "\n")

    patch_socket_source_ip(source_ip)

    executor = ThreadPoolExecutor(max_workers=args.workers)
    print(f"\nSpawning {num_sessions} pipelines across {args.workers} workers...\n")

    start_sim = time.perf_counter()

    for i in range(1, num_sessions + 1):
        chosen_video = random.choice(videos)
        executor.submit(
            stream_video_pipeline,
            i,
            chosen_video,
            ws_url,
            log_filename,
            target_size_mode, # Passed down to worker compression loop
            max_frames=30
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