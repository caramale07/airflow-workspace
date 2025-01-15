import subprocess
import logging
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("kubernetes_stats.log"),
        logging.StreamHandler()
    ]
)

def parse_kubectl_stats(output):
    """
    Parse the output of `kubectl top pods` to extract CPU and memory usage for all pods.
    """
    stats = []
    lines = output.split("\n")
    for line in lines:
        parts = line.split()
        if len(parts) >= 3 and parts[0] != "NAME":  # Skip header line
            pod_name = parts[0]
            cpu = parts[1]  # CPU usage in cores
            memory = parts[2]  # Memory usage in MiB
            try:
                cpu_value = float(cpu[:-1]) / 1000   # Convert mCPU to cores
                memory_value = float(memory[:-2])  # Strip "Mi"
                stats.append((pod_name, cpu_value, memory_value))
            except ValueError as e:
                logging.error(f"Error parsing resource values for pod {pod_name}: {e}")
    return stats

def log_kubernetes_stats(target_namespace):
    """
    Logs CPU and memory usage for all pods in a namespace.
    """
    while True:
        try:
            result = subprocess.run(
                ["kubectl", "top", "pods", "-n", target_namespace],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )

            if result.returncode == 0:
                stats = parse_kubectl_stats(result.stdout)
                for pod_name, cpu, memory in stats:
                    logging.info(f"Pod: {pod_name}, CPU: {cpu} cores, Memory: {memory} MiB")
            else:
                logging.error(f"Error running kubectl top pods: {result.stderr}")

        except Exception as e:
            logging.error(f"Error logging Kubernetes stats: {e}")

        time.sleep(5)  # Adjust the interval as needed

if __name__ == "__main__":
    TARGET_NAMESPACE = "airflow"  # Change to your Airflow namespace

    log_kubernetes_stats(TARGET_NAMESPACE)
