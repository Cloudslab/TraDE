# TraDE: Network and Traffic-aware Adaptive Scheduling for Microservices Under Dynamics

[![Paper](https://img.shields.io/badge/IEEE%20TPDS-2026-blue)](https://ieeexplore.ieee.org/document/11219337)
[![License](https://img.shields.io/badge/License-MIT-green)](LICENSE)

**Authors:** Ming Chen, Muhammed Tawfiqul Islam, Maria Rodriguez Read, and Rajkumar Buyya  
**Affiliation:** The University of Melbourne  
**Journal:** _IEEE Transactions on Parallel and Distributed Systems, vol. 37, no. 1, 2026_  
**Repository:** [https://github.com/Cloudslab/TraDE](https://github.com/Cloudslab/TraDE)



---

## Overview

Modern microservice applications run as collections of containerized services across a cluster. While this improves modularity and scalability, it also exposes applications to performance degradation when:

- Workloads change over time (request mix, QPS, and call-paths)
- Network delays between nodes drift due to congestion or topology changes
- Initial placements become suboptimal as conditions evolve   

TraDE addresses this by:

- Continuously analysing bidirectional traffic between microservices (including all replicas)
- Monitoring cross-node communication delays
- Mapping stressed microservices and slow communication paths to better placements
- Migrating microservice instances with zero downtime when QoS targets are violated   
![!\[alt text\](TraDE_framework.png)](Evaluations/TraDE_framework.png)

---


## Results at a glance

TraDE was evaluated on a 10-node Kubernetes cluster using the DeathStarBench Social Network application under changing request patterns and controlled cross-node delays.

| Metric                | Result in the evaluated scenarios                                                 |
| --------------------- | --------------------------------------------------------------------------------- |
| Average response time | Up to **48.3% lower** than NetMARKS                                               |
| Throughput            | **1.2-1.5x** that of NetMARKS across evaluated workloads                          |
| Goodput               | **95.36%**, compared with 71.99% for NetMARKS and 65.43% for Kubernetes Burstable |
| Placement computation | Usually about **0.3 seconds** and below 1 second in the reported overhead study   |



## Why TraDE

A placement that works well at deployment time can become inefficient when:

- request types, QPS and service call paths change;
- traffic becomes concentrated on different upstream/downstream pairs;
- cross-node communication latency changes between cluster nodes; or
- a previously acceptable placement begins violating an application QoS target.

TraDE measures those changes and recomputes selected service placements instead of assuming the initial schedule remains suitable.

## Control loop at a glance

![!\[alt text\](TraDE_control_loop.png)](Evaluations/TraDE_control_loop.png)


## Components

| Component          | Purpose                                                                                    | Main repository area                                             |
| ------------------ | ------------------------------------------------------------------------------------------ | ---------------------------------------------------------------- |
| Traffic Analyzer   | Builds a bidirectional traffic-stress graph from Istio service metrics                     | `Taffic_Analyzer/`                                               |
| Dynamics Manager   | Generates controlled delays for experiments and measures the current node-delay matrix     | `Dynamics_Manager/`                                              |
| PGA Mapper         | Searches capacity-constrained placements that reduce traffic-weighted communication cost   | `PGA_Mapper/`                                                    |
| Adaptive Scheduler | Watches QoS, invokes measurement and mapping, and migrates selected deployments            | `PGA_Mapper/TraDE_v3.py` and evaluation-specific implementations |
| Workloads          | Deploys and drives the DeathStarBench Social Network benchmark                             | `Workloads/`                                                     |
| Evaluations        | Contains experiment drivers, stored measurements and analysis notebooks used for the paper | `Evaluations/`                                                   |


## Evaluation environment

- Kubernetes 1.27.4
- Calico 3.26.1
- Istio 1.20.3
- CRI-O 1.27.1
- Ubuntu 22.04.2 LTS, Linux kernel 5.15.0
- one 32-core control-plane node and nine 4-core worker nodes
- 32 GiB RAM and 16 Gbps networking per node
- Prometheus and Jaeger telemetry
- DeathStarBench Social Network benchmark
- wrk2 workload generation

Other versions may work, but they have not been validated against the published results.

## Repository guide

```text
TraDE/
├── PGA_Mapper/             # Placement, scheduling and pod migration
├── Taffic_Analyzer/        # Istio metric collection and traffic graphs
├── Dynamics_Manager/       # Delay injection and latency measurement artefacts
├── Workloads/              # DeathStarBench benchmark and workload configurations
├── Evaluations/            # Baselines, raw measurements and analysis notebooks
├── Motivation_Exp/         # Smaller experiments from the paper motivation
└── K8s_cluster_setUp/      # Cluster setup notes and manifests
```

The repository contains several historical scheduler versions used during the research process; these are retained for traceability, but a future release may identify one versioned, canonical entry point.

## Reproduction status

This repository is an open research artefact, not yet a one-command deployment package. Reproduction currently requires a Kubernetes cluster, cluster-admin familiarity and manual configuration of experiment-specific values.

Before running the scheduler, inspect and update:

- the Prometheus endpoint;
- target namespace and HTTP response code;
- QoS target and look-back window;
- cluster node names and latency-measurement namespace;
- the DeathStarBench Helm-chart path; and
- the workload and delay scenario selected for the experiment.



## Reproducing the study

At a high level, the paper workflow is:
1. Provision of the Kubernetes, CNI, Istio, Prometheus and Jaeger environment.
2. Deploy the DeathStarBench Social Network benchmark and initialise its data.
3. Deploy the latency-measurement agents on the worker nodes.
4. Configure the target namespace, QoS threshold, Prometheus endpoint and workload.
5. Run Kubernetes Burstable, NetMARKS and TraDE as separate comparison conditions.
6. Apply the request-mix and cross-node-delay scenarios.
7. Collect response time, throughput, goodput, placement time and overhead measurements.
8. Use the notebooks under `Evaluations/` to reproduce the reported analyses and figures.

## Design notes

### Traffic analysis

TraDE uses bidirectional Istio counters between dependent workloads and their replicas. The resulting graph captures both the call structure and the relative traffic stress of service pairs.

### Delay measurement

Cluster-level agents measure the node-to-node latency matrix. Delay injection is an evaluation feature used to create controlled heterogeneous network conditions; it can be disabled outside experiments.

### Placement

The Parallel Greedy Algorithm combines the traffic graph, delay matrix, current placement, pod resource requests and node capacity. It prioritises high-stress pairs and evaluates candidate placements in parallel.

### Migration

TraDE launches replacement pods on target nodes and waits for readiness before removing old instances. This avoids an intentional service gap during rescheduling.

## Limitations

- The implementation is a research prototype and has not been evaluated as a multi-tenant production scheduler.
- Several configuration values and cluster assumptions are currently embedded in scripts.
- Historical scripts and notebooks remain for traceability.

## Citation

```bibtex
@article{chen2026trade,
  author  = {Chen, Ming and Islam, Muhammed Tawfiqul and {Rodriguez Read}, Maria and Buyya, Rajkumar},
  title   = {{TraDE}: Network and Traffic-Aware Adaptive Scheduling for Microservices Under Dynamics},
  journal = {IEEE Transactions on Parallel and Distributed Systems},
  volume  = {37},
  number  = {1},
  pages   = {76--89},
  year    = {2026},
  doi     = {10.1109/TPDS.2025.3626424}
}
```

---
