import asyncio
import aiohttp
import random
import time
import argparse
import json
from datetime import datetime
from tqdm.asyncio import tqdm

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
    parser.add_argument("--rps", "-r", type=float, default=100.0,
                        help="Target requests per second (default: 100)")
    parser.add_argument("--timeout", "-t", type=int, default=5)
    
    return parser.parse_args()


# Define a concurrency cap to protect file descriptor allocations
MAX_CONCURRENT_CONNECTIONS = 500
sem = asyncio.Semaphore(MAX_CONCURRENT_CONNECTIONS)

async def send_sensor_data(session, target_url, stats, timeout):
    device_id = f"dev-{random.randint(1000, 9999)}"
    data_reading = f"Sensor Reading: {round(random.uniform(10.5, 55.5), 2)} Celsius"
    timestamp = datetime.utcnow().isoformat() + "Z"
    
    target_size = random.randint(500, 1000)
    
    base_payload = {
        "device_id": device_id,
        "data": data_reading,
        "size_kb": round(target_size / 1024.0, 3),
        "timestamp": timestamp,
        "padding": ""
    }
    
    base_encoded_len = len(json.dumps(base_payload).encode('utf-8'))
    padding_needed = max(0, target_size - base_encoded_len)
    base_payload["padding"] = "x" * padding_needed

    # Enforce the Semaphore barrier right before firing the request
    async with sem:
        try:
            async with session.post(target_url, json=base_payload, timeout=aiohttp.ClientTimeout(total=timeout)) as response:
                # Completely read response content to immediately recycle the connection back to the pool
                await response.read()
                
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


async def rate_limited_dispatcher(session, target_url, stats, num, rps, timeout, pbar):
    interval = 1.0 / rps
    next_send = time.perf_counter()
    pending = set()

    for _ in range(num):
        now = time.perf_counter()
        wait = next_send - now
        if wait > 0:
            await asyncio.sleep(wait)

        task = asyncio.create_task(send_sensor_data(session, target_url, stats, timeout))
        task.add_done_callback(lambda t: (pending.discard(t), pbar.update(1)))
        pending.add(task)

        next_send += interval

    if pending:
        await asyncio.gather(*pending, return_exceptions=True)


async def main():
    args = parse_arguments()
    
    # Construct endpoint correctly. Added conditional slash check to prevent bad pathings
    endpoint_path = args.endpoint if args.endpoint.startswith('/') else f"/{args.endpoint}"
    target_url = f"http://{args.host}{endpoint_path}"
    
    stats = {'success': 0, 'fail': 0, 'timeout': 0, 'error': 0}

    local_bind = None
    if args.source:
        local_bind = (args.source, 0)
        print(f"🔗 Binding to source IP: {args.source}")

    # FIX: Configured connector limit constraints and cached DNS tracking to drastically reduce file utilization
    connector = aiohttp.TCPConnector(
        limit=MAX_CONCURRENT_CONNECTIONS,
        use_dns_cache=True,
        ttl_dns_cache=300,
        local_addr=local_bind
    )

    async with aiohttp.ClientSession(connector=connector) as session:
        print(f"🚀 Launching stress test against: {target_url}")
        print(f"📈 Target rate: {args.rps} req/s  |  Total: {args.num} requests  |  Timeout: {args.timeout}s")

        start_time = time.perf_counter()

        # Custom bar formatting tracks live progress rate
        with tqdm(
            total=args.num, 
            desc="Sending Traffic", 
            unit="pkt",
            bar_format="{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, Live Avg RPS: {rate_fmt}]"
        ) as pbar:
            await rate_limited_dispatcher(session, target_url, stats, args.num, args.rps, args.timeout, pbar)

        total_time = time.perf_counter() - start_time
        
        # Calculations for both metrics
        actual_rps = args.num / total_time
        average_rps = stats['success'] / total_time if total_time > 0 else 0

        print("\n" + "=" * 40)
        print(f"Success:        {stats['success']} ✅")
        print(f"Failures:       {stats['fail']} ❌")
        print(f"Timeouts:       {stats['timeout']} ⏱️")
        print(f"Errors:         {stats['error']} 🔌")
        print(f"Total time:     {total_time:.2f}s")
        print(f"Target RPS:     {args.rps:.1f}")
        print(f"Actual RPS:     {actual_rps:.2f} 🚀")
        print(f"Average RPS:    {average_rps:.2f} 📊")
        print("=" * 40)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass