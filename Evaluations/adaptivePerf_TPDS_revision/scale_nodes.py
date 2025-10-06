#!/usr/bin/env python3
"""
scale_nodes.py — Cordon/Drain or Uncordon Kubernetes worker nodes.

Usage examples:
  # (1) Drain extra workers 10..15 (cordon+drain, safe flags)
  python scale_nodes.py drain --prefix k8s-worker- --start 10 --end 15

  # (2) Roll back (bring capacity back) for workers 10..15
  python scale_nodes.py rollback --prefix k8s-worker- --start 10 --end 15

Common flags:
  --context <name>        : kubectl context override (optional)
  --yes                   : skip interactive confirm
  --dry-run               : show commands, do not execute
Advanced drain flags:
  --grace 30              : pod termination grace period seconds (default 30)
  --timeout 10m           : per-node drain timeout (default 10m)
  --force                 : pass --force to kubectl drain (use only if needed)
Notes:
  - Requires kubectl on PATH and a working kubeconfig.
  - Drain uses: --ignore-daemonsets --delete-emptydir-data by default.
"""

import argparse, subprocess, sys, shlex

def run(cmd: str, dry_run: bool = False) -> subprocess.CompletedProcess:
    print(f"$ {cmd}")
    if dry_run:
        # Simulate success in dry-run mode
        class Dummy: 
            returncode=0; stdout=b""; stderr=b""
        return Dummy()
    return subprocess.run(cmd, shell=True, check=False, capture_output=True)

def die(msg: str) -> None:
    print(f"[ERROR] {msg}", file=sys.stderr)
    sys.exit(1)

def build_kubectl_base(context: str | None) -> str:
    return f"kubectl --context {shlex.quote(context)}" if context else "kubectl"

def confirm_or_exit(skip: bool, prompt: str) -> None:
    if skip: 
        return
    ans = input(f"{prompt} [y/N]: ").strip().lower()
    if ans not in ("y","yes"):
        print("Aborted.")
        sys.exit(0)

def node_list(prefix: str, start: int, end: int) -> list[str]:
    if start > end:
        die(f"--start {start} must be <= --end {end}")
    return [f"{prefix}{i}" for i in range(start, end+1)]

def check_kubectl(kubectl: str, dry_run: bool) -> None:
    cp = run(f"{kubectl} version --client", dry_run)
    if not dry_run and cp.returncode != 0:
        die("kubectl not found or not working")

def get_pods_on_node(kubectl: str, node: str, dry_run: bool) -> str:
    cmd = f"{kubectl} get pods -A -o wide --field-selector spec.nodeName={shlex.quote(node)}"
    cp = run(cmd, dry_run)
    if cp.returncode != 0:
        return ""
    return (cp.stdout or b"").decode(errors="ignore")

def cordon_and_drain_node(
    kubectl: str,
    node: str,
    grace: int,
    timeout: str,
    force: bool,
    dry_run: bool
) -> bool:
    # 1) Cordon: stop new pods from scheduling here
    cp = run(f"{kubectl} cordon {shlex.quote(node)}", dry_run)
    if cp.returncode != 0:
        print(cp.stderr.decode(errors="ignore"))
        return False

    # 2) Drain: evict existing workload pods safely
    flags = [
        "--ignore-daemonsets",
        "--delete-emptydir-data",
        f"--grace-period={int(grace)}",
        f"--timeout={shlex.quote(timeout)}",
    ]
    if force:
        flags.append("--force")
    cmd = f"{kubectl} drain {shlex.quote(node)} " + " ".join(flags)
    cp = run(cmd, dry_run)
    if cp.returncode != 0:
        # Show helpful diagnostics
        print(cp.stderr.decode(errors="ignore"))
        print("\nHint: If blocked by PDBs, temporarily scale up replicas or relax the PDB, then retry.")
        return False

    # 3) Verify: list pods remaining on the node (should be none or only DS during shutdown)
    print(f"\n[VERIFY] Pods still on {node}:")
    print(get_pods_on_node(kubectl, node, dry_run))
    return True

def uncordon_node(kubectl: str, node: str, dry_run: bool) -> bool:
    cp = run(f"{kubectl} uncordon {shlex.quote(node)}", dry_run)
    if cp.returncode != 0:
        print(cp.stderr.decode(errors="ignore"))
        return False
    return True

def do_drain(args):
    kubectl = build_kubectl_base(args.context)
    check_kubectl(kubectl, args.dry_run)
    nodes = node_list(args.prefix, args.start, args.end)
    confirm_or_exit(args.yes, f"Drain (cordon+drain) nodes: {', '.join(nodes)}")

    failures = []
    for n in nodes:
        print(f"\n=== Working on {n} ===")
        ok = cordon_and_drain_node(
            kubectl=kubectl,
            node=n,
            grace=args.grace,
            timeout=args.timeout,
            force=args.force,
            dry_run=args.dry_run,
        )
        if not ok:
            failures.append(n)

    print("\n=== Summary ===")
    if failures:
        print("Failed nodes:", ", ".join(failures))
        sys.exit(2)
    else:
        print("All nodes drained successfully.")

def do_rollback(args):
    kubectl = build_kubectl_base(args.context)
    check_kubectl(kubectl, args.dry_run)
    nodes = node_list(args.prefix, args.start, args.end)
    confirm_or_exit(args.yes, f"Uncordon nodes: {', '.join(nodes)}")

    failures = []
    for n in nodes:
        print(f"\n=== Uncordon {n} ===")
        ok = uncordon_node(kubectl, n, args.dry_run)
        if not ok:
            failures.append(n)

    print("\n=== Summary ===")
    if failures:
        print("Failed nodes:", ", ".join(failures))
        sys.exit(2)
    else:
        print("All nodes uncordoned successfully.")

def main():
    p = argparse.ArgumentParser(description="Cordon/drain or uncordon Kubernetes nodes.")
    sub = p.add_subparsers(dest="cmd", required=True)

    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--context", default=None, help="kubectl context name")
    common.add_argument("--prefix", default="k8s-worker-", help="Node name prefix (default: k8s-worker-)")
    common.add_argument("--start", type=int, required=True, help="Start index (inclusive)")
    common.add_argument("--end", type=int, required=True, help="End index (inclusive)")
    common.add_argument("--yes", action="store_true", help="Skip interactive confirmation")
    common.add_argument("--dry-run", action="store_true", help="Print commands without executing")

    p_drain = sub.add_parser("drain", parents=[common], help="Cordon + drain nodes")
    p_drain.add_argument("--grace", type=int, default=30, help="Grace period seconds (default 30)")
    p_drain.add_argument("--timeout", default="10m", help="Drain timeout per node (default 10m)")
    p_drain.add_argument("--force", action="store_true", help="Pass --force to kubectl drain")

    p_rb = sub.add_parser("rollback", parents=[common], help="Uncordon nodes")

    args = p.parse_args()
    if args.cmd == "drain":
        do_drain(args)
    elif args.cmd == "rollback":
        do_rollback(args)
    else:
        die("Unknown command")

if __name__ == "__main__":
    main()


'''
`Example output for draining worker nodes 10 to 15:

ubuntu@k8s-master:~$ python3 /home/ubuntu/TraDE/Evaluations/adaptivePerf_TPDS_revision/scale_nodes.py drain --prefix k8s-worker- --start 10 --end 15
$ kubectl version --client
Drain (cordon+drain) nodes: k8s-worker-10, k8s-worker-11, k8s-worker-12, k8s-worker-13, k8s-worker-14, k8s-worker-15 [y/N]: y

=== Working on k8s-worker-10 ===
$ kubectl cordon k8s-worker-10
$ kubectl drain k8s-worker-10 --ignore-daemonsets --delete-emptydir-data --grace-period=30 --timeout=10m

[VERIFY] Pods still on k8s-worker-10:
$ kubectl get pods -A -o wide --field-selector spec.nodeName=k8s-worker-10
NAMESPACE          NAME                                           READY   STATUS    RESTARTS   AGE   IP               NODE            NOMINATED NODE   READINESS GATES
calico-system      calico-node-5htsh                              1/1     Running   0          94d   172.26.134.133   k8s-worker-10   <none>           <none>
calico-system      csi-node-driver-5frbm                          2/2     Running   0          94d   10.85.0.3        k8s-worker-10   <none>           <none>
kube-system        kube-proxy-lpqlc                               1/1     Running   0          94d   172.26.134.133   k8s-worker-10   <none>           <none>
kube-system        node-exporter-prometheus-node-exporter-jqxv4   1/1     Running   0          94d   172.26.134.133   k8s-worker-10   <none>           <none>
measure-nodes-bd   bandwidth-measurement-ds-2lb65                 1/1     Running   0          94d   10.85.0.2        k8s-worker-10   <none>           <none>
measure-nodes      latency-measurement-ds-mjctt                   1/1     Running   0          61m   192.168.87.76    k8s-worker-10   <none>           <none>


=== Working on k8s-worker-11 ===
$ kubectl cordon k8s-worker-11
$ kubectl drain k8s-worker-11 --ignore-daemonsets --delete-emptydir-data --grace-period=30 --timeout=10m

[VERIFY] Pods still on k8s-worker-11:
$ kubectl get pods -A -o wide --field-selector spec.nodeName=k8s-worker-11
NAMESPACE          NAME                                           READY   STATUS    RESTARTS   AGE   IP                NODE            NOMINATED NODE   READINESS GATES
calico-system      calico-node-kh9qz                              1/1     Running   0          94d   172.26.128.34     k8s-worker-11   <none>           <none>
calico-system      csi-node-driver-kc75x                          2/2     Running   0          94d   10.85.0.3         k8s-worker-11   <none>           <none>
kube-system        kube-proxy-7qwq8                               1/1     Running   0          94d   172.26.128.34     k8s-worker-11   <none>           <none>
kube-system        node-exporter-prometheus-node-exporter-fdtdw   1/1     Running   0          94d   172.26.128.34     k8s-worker-11   <none>           <none>
measure-nodes-bd   bandwidth-measurement-ds-wwc5g                 1/1     Running   0          94d   10.85.0.2         k8s-worker-11   <none>           <none>
measure-nodes      latency-measurement-ds-tz2hh                   1/1     Running   0          61m   192.168.189.207   k8s-worker-11   <none>           <none>


=== Working on k8s-worker-12 ===
$ kubectl cordon k8s-worker-12
$ kubectl drain k8s-worker-12 --ignore-daemonsets --delete-emptydir-data --grace-period=30 --timeout=10m

[VERIFY] Pods still on k8s-worker-12:
$ kubectl get pods -A -o wide --field-selector spec.nodeName=k8s-worker-12
NAMESPACE          NAME                                           READY   STATUS    RESTARTS   AGE   IP                NODE            NOMINATED NODE   READINESS GATES
calico-system      calico-node-s7rl5                              1/1     Running   0          94d   172.26.132.228    k8s-worker-12   <none>           <none>
calico-system      csi-node-driver-v928b                          2/2     Running   0          94d   10.85.0.4         k8s-worker-12   <none>           <none>
kube-system        kube-proxy-rk9q9                               1/1     Running   0          94d   172.26.132.228    k8s-worker-12   <none>           <none>
kube-system        node-exporter-prometheus-node-exporter-98d62   1/1     Running   0          94d   172.26.132.228    k8s-worker-12   <none>           <none>
measure-nodes-bd   bandwidth-measurement-ds-f2l68                 1/1     Running   0          94d   10.85.0.2         k8s-worker-12   <none>           <none>
measure-nodes      latency-measurement-ds-v5b9p                   1/1     Running   0          58m   192.168.214.208   k8s-worker-12   <none>           <none>


=== Working on k8s-worker-13 ===
$ kubectl cordon k8s-worker-13
$ kubectl drain k8s-worker-13 --ignore-daemonsets --delete-emptydir-data --grace-period=30 --timeout=10m

[VERIFY] Pods still on k8s-worker-13:
$ kubectl get pods -A -o wide --field-selector spec.nodeName=k8s-worker-13
NAMESPACE          NAME                                           READY   STATUS    RESTARTS   AGE   IP               NODE            NOMINATED NODE   READINESS GATES
calico-system      calico-node-5nnrs                              1/1     Running   0          94d   172.26.133.157   k8s-worker-13   <none>           <none>
calico-system      csi-node-driver-9llcz                          2/2     Running   0          94d   10.85.0.3        k8s-worker-13   <none>           <none>
kube-system        kube-proxy-rk68q                               1/1     Running   0          94d   172.26.133.157   k8s-worker-13   <none>           <none>
kube-system        node-exporter-prometheus-node-exporter-n4swj   1/1     Running   0          94d   172.26.133.157   k8s-worker-13   <none>           <none>
measure-nodes-bd   bandwidth-measurement-ds-bvztz                 1/1     Running   0          94d   10.85.0.2        k8s-worker-13   <none>           <none>
measure-nodes      latency-measurement-ds-b98fh                   1/1     Running   0          56m   192.168.27.83    k8s-worker-13   <none>           <none>


=== Working on k8s-worker-14 ===
$ kubectl cordon k8s-worker-14
$ kubectl drain k8s-worker-14 --ignore-daemonsets --delete-emptydir-data --grace-period=30 --timeout=10m

[VERIFY] Pods still on k8s-worker-14:
$ kubectl get pods -A -o wide --field-selector spec.nodeName=k8s-worker-14
NAMESPACE          NAME                                           READY   STATUS    RESTARTS   AGE   IP                NODE            NOMINATED NODE   READINESS GATES
calico-system      calico-node-z6mhn                              1/1     Running   0          94d   172.26.128.40     k8s-worker-14   <none>           <none>
calico-system      csi-node-driver-k484l                          2/2     Running   0          94d   10.85.0.4         k8s-worker-14   <none>           <none>
kube-system        kube-proxy-hdmw5                               1/1     Running   0          94d   172.26.128.40     k8s-worker-14   <none>           <none>
kube-system        node-exporter-prometheus-node-exporter-v5sd6   1/1     Running   0          94d   172.26.128.40     k8s-worker-14   <none>           <none>
measure-nodes-bd   bandwidth-measurement-ds-gxxvw                 1/1     Running   0          94d   10.85.0.3         k8s-worker-14   <none>           <none>
measure-nodes      latency-measurement-ds-mppcm                   1/1     Running   0          55m   192.168.252.124   k8s-worker-14   <none>           <none>


=== Working on k8s-worker-15 ===
$ kubectl cordon k8s-worker-15
$ kubectl drain k8s-worker-15 --ignore-daemonsets --delete-emptydir-data --grace-period=30 --timeout=10m

[VERIFY] Pods still on k8s-worker-15:
$ kubectl get pods -A -o wide --field-selector spec.nodeName=k8s-worker-15
NAMESPACE          NAME                                           READY   STATUS    RESTARTS   AGE   IP               NODE            NOMINATED NODE   READINESS GATES
calico-system      calico-node-gc24h                              1/1     Running   0          84d   172.26.131.224   k8s-worker-15   <none>           <none>
calico-system      csi-node-driver-zmfb4                          2/2     Running   0          84d   10.85.0.3        k8s-worker-15   <none>           <none>
kube-system        kube-proxy-hcb92                               1/1     Running   0          84d   172.26.131.224   k8s-worker-15   <none>           <none>
kube-system        node-exporter-prometheus-node-exporter-mzb7c   1/1     Running   0          84d   172.26.131.224   k8s-worker-15   <none>           <none>
measure-nodes-bd   bandwidth-measurement-ds-p4z99                 1/1     Running   0          84d   10.85.0.4        k8s-worker-15   <none>           <none>
measure-nodes      latency-measurement-ds-59tmg                   1/1     Running   0          56m   192.168.96.38    k8s-worker-15   <none>           <none>


=== Summary ===
All nodes drained successfully.

'''