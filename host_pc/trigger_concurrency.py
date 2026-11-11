import requests
import threading

NODE1_URL = "http://192.168.56.104:7001"

def submit_task(pid, cpu, mem):
    requests.post(f"{NODE1_URL}/api/execute_concurrent", json={"pid": pid, "cpu": cpu, "mem": mem})

def main():
    print("=== MILESTONE 6: Concurrency and Resource Contention ===")
    threads = []
    
    # We have 4 CPU cores. We send 5 concurrent tasks requiring 1 CPU each.
    # One task will be rejected due to resource contention.
    for i in range(5):
        t = threading.Thread(target=submit_task, args=(f"T{i}", 1, 200))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    print("All concurrent requests sent. Check Node 1 logs for rejection/acceptance.")

if __name__ == "__main__":
    main()