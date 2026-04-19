import requests
import time
import string
import os
from multiprocessing import Pool, cpu_count
from collections import Counter

# Configuration
BASE_URL = "http://72.60.221.150:8080"
STUDENT_ID = "MDS202527"

# Process-local cache for the secret key.
# This prevents workers from spamming the /login endpoint for every file.
_process_secret_key = None

def get_cached_secret_key(student_id):
    """Logs in and retrieves a key, caching it for the current worker process."""
    global _process_secret_key
    pid = os.getpid()  # Get the Process ID for cleaner logging

    if _process_secret_key:
        return _process_secret_key

    print(f"[Worker {pid}] Attempting to get secret key from /login...")
    url = f"{BASE_URL}/login"
    payload = {"student_id": student_id}

    while True:
        try:
            # Added timeout to prevent hanging on dropped network connections
            response = requests.post(url, json=payload, timeout=10)

            if response.status_code == 200:
                _process_secret_key = response.json().get("secret_key")
                print(f"[Worker {pid}] Successfully obtained secret key.")
                return _process_secret_key
            elif response.status_code == 429:
                print(f"[Worker {pid}] Login rate-limited (429). Waiting 1s...")
                time.sleep(1)
            else:
                print(f"[Worker {pid}] Login failed with status: {response.status_code}")
                response.raise_for_status()

        except requests.exceptions.RequestException as e:
            print(f"[Worker {pid}] Network error during login: {e}. Retrying in 2s...")
            time.sleep(2)


def get_publication_title(student_id, filename):
    """Retrieves the title via RPC, using exponential backoff."""
    pid = os.getpid()
    secret_key = get_cached_secret_key(student_id)
    url = f"{BASE_URL}/lookup"
    payload = {"secret_key": secret_key, "filename": filename}

    wait_time = 0.5
    max_retries = 7

    for attempt in range(1, max_retries + 1):
        try:
            response = requests.post(url, json=payload, timeout=10)

            if response.status_code == 200:
                return response.json().get("title", "")
            elif response.status_code == 429:
                print(f"[Worker {pid}] Throttled on {filename} (Attempt {attempt}). Waiting {wait_time}s...")
                time.sleep(wait_time)
                wait_time *= 2
            elif response.status_code == 404:
                print(f"[Worker {pid}] 404 Not Found: {filename}")
                return ""
            else:
                print(f"[Worker {pid}] Error {response.status_code} on {filename}")
                response.raise_for_status()

        except requests.exceptions.RequestException as e:
            print(f"[Worker {pid}] Network error on {filename}: {e}. Retrying...")
            time.sleep(wait_time)
            wait_time *= 2

    print(f"[Worker {pid}] FAILED to retrieve {filename} after {max_retries} retries.")
    return ""


def mapper(filename_chunk):
    """The Map Phase."""
    pid = os.getpid()
    print(f"[Worker {pid}] Started processing chunk of {len(filename_chunk)} files...")

    word_counts = Counter()
    processed_count = 0

    for filename in filename_chunk:
        title = get_publication_title(STUDENT_ID, filename)
        if title:
            words = title.split()
            if words:
                first_word = words[0].lower().strip(string.punctuation)
                if first_word:
                    word_counts[first_word] += 1

        processed_count += 1
        # Print progress every 10 files so we know the worker isn't dead
        if processed_count % 10 == 0:
            print(f"[Worker {pid}] Progress: {processed_count}/{len(filename_chunk)} files done.")

    print(f"[Worker {pid}] Finished chunk!")
    return word_counts


def verify_top_10(student_id, top_10_list):
    """Submits the final Top 10 list."""
    print(f"\n--- Submitting Top 10 for verification ---")
    secret_key = get_cached_secret_key(student_id)
    url = f"{BASE_URL}/verify"
    payload = {"secret_key": secret_key, "top_10": top_10_list}

    try:
        response = requests.post(url, json=payload, timeout=10)
        if response.status_code == 200:
            result = response.json()
            print(f"✅ Verification Success!")
            print(f"Score: {result.get('score')} / {result.get('total')}")
            print(f"Message: {result.get('message')}")
        else:
            print(f"❌ Verification failed (Status Code: {response.status_code})")
            print(response.text)
    except requests.exceptions.RequestException as e:
        print(f"Error during verification: {e}")


if __name__ == "__main__":
    total_files = 1000
    filenames = [f"pub_{i}.txt" for i in range(total_files)]

    num_workers = min(cpu_count(), 5)
    chunk_size = (total_files + num_workers - 1) // num_workers
    chunks = [filenames[i:i + chunk_size] for i in range(0, total_files, chunk_size)]

    print(f"MAIN: Starting Map-Reduce task with {num_workers} workers...")
    print(f"MAIN: Total files: {total_files}, Chunk size: {chunk_size}")

    try:
        with Pool(processes=num_workers) as pool:
            print("MAIN: Pool created. Distributing tasks...")
            mapped_results = pool.map(mapper, chunks)
            print("MAIN: All workers finished! Reducing results...")

        total_counts = Counter()
        for result in mapped_results:
            total_counts.update(result)

        top_10_tuples = total_counts.most_common(10)
        top_10 = [word.capitalize() for word, count in top_10_tuples]

        if top_10:
            verify_top_10(STUDENT_ID, top_10)
        else:
            print("MAIN: No words were computed.")

    except Exception as e:
        print(f"MAIN ERROR: The script crashed with exception: {e}")