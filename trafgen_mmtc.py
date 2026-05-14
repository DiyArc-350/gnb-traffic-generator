import asyncio
import aiohttp
import random
import time
import argparse
from datetime import datetime
from tqdm import asyncio as tqdm_asyncio

def parse_arguments():
    parser = argparse.ArgumentParser(description="IoT Traffic Generator - Bound to Source IP")
    
    # Connection Settings
    parser.add_argument("--host", "-H", default="10.40.1.220")
    parser.add_argument("--endpoint", "-e", default="/mmtc/collect")
    parser.add_argument("--interface", "-i", default="oaitun_ue1")
    
    # BINDING SETTING
    parser.add_argument("--source", "-S", default=None, 
                        help="The local IP address to bind to (e.g., your OAI UE IP)")
    
    # Traffic Settings
    parser.add_argument("--num", "-n", type=int, default=10000)
    parser.add_argument("--concurrent", "-c", type=int, default=200)
    parser.add_argument("--timeout", "-t", type=int, default=5)
    
    return parser.parse_args()


async def send_sensor_data(session, target_url, stats, semaphore):
    payload = {
        "device_id": f"dev-{random.randint(1000, 9999)}",
        "data": f"Sensor Reading: {round(random.uniform(10.5, 55.5), 2)} Celsius", # Added this!
        "size_kb": 1.2, 
        "timestamp": datetime.utcnow().isoformat() + "Z"
    }

    async with semaphore:
        try:
            async with session.post(target_url, json=payload, timeout=5) as response:
                if response.status in [200, 201, 202, 204]:
                    stats['success'] += 1
                else:
                    stats['fail'] += 1
                    if stats['fail'] == 1:
                        error_body = await response.text()
                        print(f"\n⚠️ HTTP {response.status}: {error_body}")
                return response.status
        except asyncio.TimeoutError:
            stats['timeout'] += 1
        except Exception as e:
            stats['error'] += 1
            if stats['error'] == 1:
                print(f"\n🔌 CONNECTION ERROR: {e}")

async def main():
    args = parse_arguments()
    target_url = f"http://{args.host}:{args.endpoint}"
    stats = {'success': 0, 'fail': 0, 'timeout': 0, 'error': 0}
    semaphore = asyncio.Semaphore(args.concurrent)
    
    # --- BINDING LOGIC ---
    # If a source IP is provided, we tell the connector to use it.
    # local_addr is a tuple (ip, port). Port 0 lets the OS choose a random high port.
    local_bind = None
    if args.source:
        local_bind = (args.source, 0)
        print(f"🔗 Binding to source IP: {args.source}")

    connector = aiohttp.TCPConnector(
        limit=args.concurrent, 
        local_addr=local_bind  # This forces traffic through a specific interface/IP
    )

    async with aiohttp.ClientSession(connector=connector) as session:
        print(f"🚀 Launching stress test against: {target_url}")
        
        start_time = time.perf_counter()
        tasks = [send_sensor_data(session, target_url, stats, semaphore) for _ in range(args.num)]
        await tqdm_asyncio.tqdm.gather(*tasks, desc="Sending Traffic", unit="pkt")
        total_time = time.perf_counter() - start_time

        print("\n" + "="*40)
        print(f"Success:        {stats['success']} ✅")
        print(f"Timeouts:       {stats['timeout']} ⏱️")
        print(f"Avg Throughput: {args.num / total_time:.2f} req/s")
        print("="*40)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass