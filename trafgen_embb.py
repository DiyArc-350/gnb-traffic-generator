import requests
import numpy as np
import time
from concurrent.futures import ThreadPoolExecutor
from bs4 import BeautifulSoup
from urllib.parse import urljoin

class SourceIPAdapter(requests.adapters.HTTPAdapter):
    def __init__(self, source_address, **kwargs):
        self.source_address = source_address
        super().__init__(**kwargs)

    def init_poolmanager(self, *args, **kwargs):
        kwargs['source_address'] = (self.source_address, 0)
        super().init_poolmanager(*args, **kwargs)

def zipf_mandelbrot(N, q, s):
    ranks = np.arange(1, N + 1)
    weights = (ranks + q) ** -s
    return weights / weights.sum()

def fetch_resources(html, base_url):
    resources = []
    soup = BeautifulSoup(html, "html.parser")

    # Common resource tags
    tags = [
        ("img", "src"),
        ("script", "src"),
        ("link", "href")
    ]

    for tag, attr in tags:
        for t in soup.find_all(tag):
            url = t.get(attr)
            if url:
                full_url = urljoin(base_url, url)
                resources.append(full_url)

    return resources

def make_request(url, session):
    try:
        response = session.get(url, timeout=5)

        # If HTML → fetch embedded resources
        content_type = response.headers.get("Content-Type", "")
        if "text/html" in content_type:
            resources = fetch_resources(response.text, url)

            for res_url in resources:
                try:
                    session.get(res_url, timeout=5)
                except requests.exceptions.RequestException:
                    pass

    except requests.exceptions.RequestException:
        pass

def generate_urls(base_ip, num_contents):
    return [f"http://{base_ip}/index{i}.html" for i in range(1, num_contents + 1)]

def generate_traffic(base_ip, num_contents, num_requests, rps, zipf_q, zipf_s, source_ip):
    urls = generate_urls(base_ip, num_contents)
    probabilities = zipf_mandelbrot(len(urls), zipf_q, zipf_s)

    session = requests.Session()
    session.mount('http://', SourceIPAdapter(source_ip.strip()))

    executor = ThreadPoolExecutor(max_workers=100)

    for _ in range(num_requests):
        url = np.random.choice(urls, p=probabilities)
        executor.submit(make_request, url, session)
        time.sleep(1 / rps)

    executor.shutdown(wait=True)

def main():
    print("=== Zipf Traffic Generator (with media requests) ===")

    base_ip = "10.40.1.135"

    num_contents = int(input("Number of contents (e.g., 100): "))
    num_requests = int(input("Total number of requests: "))
    rps = float(input("Requests per second: "))
    zipf_q = float(input("Zipf parameter q: "))
    zipf_s = float(input("Zipf parameter s: "))
    source_ip = input("Source IP (e.g., 10.45.0.x): ")

    generate_traffic(base_ip, num_contents, num_requests, rps, zipf_q, zipf_s, source_ip)

    print("Traffic generation completed.")

if __name__ == "__main__":
    main()