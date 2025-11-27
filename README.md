# TraDE: Network and Traffic-aware Adaptive Scheduling for Microservices Under Dynamics

[![Paper](https://img.shields.io/badge/IEEE%20TPDS-2026-blue)](https://ieeexplore.ieee.org/document/11219337)
[![License](https://img.shields.io/badge/License-MIT-green)](LICENSE)

**Authors:** Ming Chen, Muhammed Tawfiqul Islam, Maria Rodriguez Read, and Rajkumar Buyya  
**Affiliation:** The University of Melbourne  
**Paper:** _IEEE Transactions on Parallel and Distributed Systems, vol. 37, no. 1, 2026_  
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

---

## Key Components

TraDE’s design is built around four logical components:   

1. **Traffic Analyzer**
   - Uses a service mesh (Istio) to obtain fine-grained, per-edge, bidirectional traffic metrics between upstream and downstream microservices (and their replicas).
   - Builds a *traffic stress graph* that captures both the call graph and per-edge traffic intensity.

2. **Dynamics Manager**
   - Provides a *delay generator* for controllably injecting heterogeneous cross-node delays (used mainly for evaluation).
   - Provides a lightweight *delay measurer* implemented as cluster-level agents to monitor the current node-to-node delay matrix.

3. **PGA Mapper (Parallel Greedy Algorithm)**
   - Formulates a cost function that combines traffic intensity and inter-node delay.
   - Searches for a new service-to-node placement that minimises total communication cost while respecting node capacities (CPU, memory, etc.).
   - Runs in parallel over microservice pairs to keep mapping latency low.   

4. **Adaptive Scheduler**
   - Watches QoS metrics (e.g., average response time) via Prometheus/Istio.
   - When a QoS target is violated over a sliding time window, triggers the rescheduling pipeline:
     1. Construct traffic graph
     2. Obtain delay matrix
     3. Run PGA to compute a new placement
     4. Migrate microservice instances with *asynchronous launching* (start new pods before evicting old ones) to avoid downtime.   

---

## Repository Layout

The repository is organised around these components and the evaluation pipeline:

- `K8s_cluster_setUp/`  
  Example manifests, scripts, and notes for preparing the Kubernetes cluster and baseline dependencies (Kubernetes, CNI, Istio, Prometheus, Jaeger, etc.).

- `Taffic_Analyzer/`  
  Implementation of the **Traffic Analyzer**: service-mesh integration, metric collection, and traffic graph builder logic.

- `Dynamics_Manager/`  
  Implementation of the **Dynamics Manager**: delay generator and delay measurer (agent-based cross-node delay monitoring).

- `PGA_Mapper/`  
  Implementation of the **PGA Mapper** and supporting code for cost calculation, placement search, and resource-constraint handling.

- `Motivation_Exp/`  
  Scripts and notebooks for the smaller “motivation” experiments and microbenchmarks discussed in the paper (e.g., impact of cross-node delays and message sizes).

- `Workloads/`  
  Workload definitions and helpers (e.g., wrk2 settings, request mixes) for generating traffic against the benchmark application.   

- `Evaluations/`  
  Experiment drivers and analysis scripts used to produce the main evaluation figures (response time, throughput, goodput, and adaptive behaviour under changing delays).

You can treat `K8s_cluster_setUp/`, `Taffic_Analyzer/`, `Dynamics_Manager/`, and `PGA_Mapper/` as the core framework, and `Motivation_Exp/`, `Workloads/`, and `Evaluations/` as the experiment layer.

---
