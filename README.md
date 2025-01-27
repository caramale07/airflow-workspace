# README: Analysis and Benchmarking of Airflow Executors

## Overview
This repository benchmarks Airflow's **Celery Executor** and **Kubernetes Executor** to evaluate performance, scalability, and suitability for production. The project focuses on resource utilization, fault tolerance, and deployment readiness.

## Key Findings
### Feature Comparison
- **Celery Executor**: Dedicated workers, fixed resources, easier setup, but less cost-effective.
- **Kubernetes Executor**: Per-task resource allocation, scales to zero, requires Kubernetes expertise.

### Benchmark Results
- **1M Rows (~15 MB)**:
  - Mean Duration: 5s | Max Duration: 18s
  - Scheduler: 76m CPU, 443 MiB Memory
  - Worker: 68–78m CPU, ~1222 MiB Memory
- **2M Rows (~30 MB)**:
  - Mean Duration: 55s | Max Duration: 2m 40s
  - Scheduler: 154m CPU, 462 MiB Memory
  - Worker: 450m CPU, ~320 MiB Memory
  - **Note**: Worker memory exceeded 2GB limits in larger datasets.

## Best Practices
1. **DAG Design**:
   - Use atomic tasks and macros (e.g., `{{ ds }}`).
   - Avoid top-level code and separate configuration files.
2. **XComs**:
   - Pass only small payloads; use external storage for large data.
3. **Executor Choice**:
   - Use **Kubernetes Executor** for scalability and cost-efficiency.
   - Use **Celery Executor** for simpler setups.

## Recommendation
- **Kubernetes Executor** is ideal for production due to scalability and resource efficiency.
- **Celery Executor** is better for simpler, low-cost environments.

## References
- [Airflow DAG Best Practices](https://www.astronomer.io/docs/learn/dag-best-practices)
- [Airflow Executors](https://www.astronomer.io/docs/airflow/architecture/executors)
- [Airflow Guides](https://github.com/astronomer/airflow-guides)
