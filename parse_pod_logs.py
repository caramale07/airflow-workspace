import re
from collections import defaultdict

# Log file path
log_file = "kubernetes_stats.log"

def parse_logs(file_path):
    """
    Parses the log file and computes mean CPU and memory usage for each pod.
    """
    pod_usage = defaultdict(lambda: {"cpu": [], "memory": []})

    # Define the regex to extract pod information
    log_pattern = re.compile(
        r"Pod: (?P<pod_name>\S+), CPU: (?P<cpu>[\d.]+) cores, Memory: (?P<memory>[\d.]+) MiB"
    )

    with open(file_path, "r") as log:
        for line in log:
            match = log_pattern.search(line)
            if match:
                pod_name = match.group("pod_name")
                cpu = float(match.group("cpu"))
                memory = float(match.group("memory"))

                pod_usage[pod_name]["cpu"].append(cpu)
                pod_usage[pod_name]["memory"].append(memory)

    # Calculate mean usage
    mean_usage = {}
    for pod, usage in pod_usage.items():
        mean_cpu = sum(usage["cpu"]) / len(usage["cpu"])
        mean_memory = sum(usage["memory"]) / len(usage["memory"])
        mean_usage[pod] = {"mean_cpu": mean_cpu, "mean_memory": mean_memory}

    return mean_usage

def display_mean_usage(mean_usage):
    """
    Displays the mean CPU and memory usage for each pod.
    """
    print(f"{'Pod Name':<40} {'Mean CPU (cores)':<20} {'Mean Memory (MiB)'}")
    print("-" * 80)
    for pod, usage in mean_usage.items():
        print(f"{pod:<40} {usage['mean_cpu']:<20.3f} {usage['mean_memory']:<20.3f}")

if __name__ == "__main__":
    mean_usage = parse_logs(log_file)
    display_mean_usage(mean_usage)
