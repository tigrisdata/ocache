#!/usr/bin/env python3
"""Measure HTTP PutObject forwarding through a warmed three-node cluster."""

import argparse
import base64
import http.client
import json
import pathlib
import socket
import subprocess
import sys
import tempfile
import time
import urllib.parse


HOST = "127.0.0.1"


def free_ports(count):
    sockets = []
    ports = []
    try:
        for _ in range(count):
            sock = socket.socket()
            sock.bind((HOST, 0))
            sockets.append(sock)
            ports.append(sock.getsockname()[1])
    finally:
        for sock in sockets:
            sock.close()
    return ports


def fetch_json(port, path):
    conn = http.client.HTTPConnection(HOST, port, timeout=2)
    try:
        conn.request("GET", path)
        response = conn.getresponse()
        body = response.read()
        if response.status != 200:
            raise RuntimeError(f"GET {path} on {port}: HTTP {response.status}: {body[:200]!r}")
        return json.loads(body)
    finally:
        conn.close()


def wait_for_http(port, path, predicate=None, timeout=60):
    deadline = time.monotonic() + timeout
    last_error = None
    while time.monotonic() < deadline:
        try:
            value = fetch_json(port, path)
            if predicate is None or predicate(value):
                return value
        except (OSError, RuntimeError, ValueError) as error:
            last_error = error
        time.sleep(0.1)
    raise RuntimeError(f"timed out waiting for http://{HOST}:{port}{path}: {last_error}")


def active_nodes(topology):
    payload = topology.get("topology", topology)
    nodes = payload.get("nodes", [])
    return [node for node in nodes if "ACTIVE" in str(node.get("status", "")) or node.get("status") == 0]


def topology_ready(value):
    return len(active_nodes(value)) == 3


def node_id(node):
    return node.get("id", node.get("nodeId", ""))


def node_tokens(topology):
    payload = topology.get("topology", topology)
    ring_config = payload.get("ringConfig", payload.get("ring_config", {}))
    assignments = ring_config.get("nodeTokens", ring_config.get("node_tokens", []))
    tokens = []
    for assignment in assignments:
        owner = assignment.get("nodeId", assignment.get("node_id", ""))
        for token in assignment.get("tokens", []):
            tokens.append((int(token), owner))
    if not tokens:
        raise RuntimeError("topology did not contain ring tokens")
    return sorted(tokens)


def fnv1a32(value):
    result = 2166136261
    for byte in value.encode():
        result ^= byte
        result = (result * 16777619) & 0xFFFFFFFF
    return result


def owner_for(key, tokens):
    token = fnv1a32(key)
    for ring_token, owner in tokens:
        if ring_token >= token:
            return owner
    return tokens[0][1]


def scalar_metric(text, name):
    for line in text.splitlines():
        if line.startswith(name + " "):
            return float(line.split()[1])
    return 0.0


def metric_value(text, method, status):
    metric = "ocache_rpc_requests_total"
    wanted = {"method": method, "status": status}
    for line in text.splitlines():
        if not line.startswith(metric + "{"):
            continue
        labels, _, value = line.partition("} ")
        if not value:
            continue
        found = {}
        for item in labels[len(metric) + 1 :].split(","):
            name, raw = item.split("=", 1)
            found[name] = raw.strip('"')
        if all(found.get(name) == expected for name, expected in wanted.items()):
            return float(value.split()[0])
    return 0.0


def rpc_count(port, method, statuses=("success",)):
    text = fetch_text(port, "/metrics")
    return sum(metric_value(text, method, status) for status in statuses)


def fetch_text(port, path):
    conn = http.client.HTTPConnection(HOST, port, timeout=2)
    try:
        conn.request("GET", path)
        response = conn.getresponse()
        body = response.read()
        if response.status != 200:
            raise RuntimeError(f"GET {path} on {port}: HTTP {response.status}")
        return body.decode()
    finally:
        conn.close()


def put_requests(http_port, key, body, count):
    path = "/v1/cache/" + urllib.parse.quote(key, safe="")
    conn = http.client.HTTPConnection(HOST, http_port, timeout=30)
    latencies = []
    try:
        for _ in range(count):
            start = time.perf_counter()
            conn.request(
                "POST",
                path,
                body=body,
                headers={"Content-Type": "application/json", "Content-Length": str(len(body))},
            )
            response = conn.getresponse()
            response_body = response.read()
            elapsed = time.perf_counter() - start
            if response.status != 200:
                raise RuntimeError(f"PUT {key}: HTTP {response.status}: {response_body[:200]!r}")
            result = json.loads(response_body)
            if not result.get("success", False):
                raise RuntimeError(f"PUT {key}: unsuccessful response: {result}")
            latencies.append(elapsed)
    finally:
        conn.close()
    return latencies


def start_cluster(binary, root):
    for attempt in range(5):
        ports = free_ports(9)
        grpc_ports = ports[0:3]
        http_ports = ports[3:6]
        gossip_ports = ports[6:9]
        seeds = ",".join(f"{HOST}:{port}" for port in gossip_ports)
        attempt_root = root / f"attempt-{attempt}"
        attempt_root.mkdir()
        processes = []
        try:
            for index in range(3):
                node_root = attempt_root / f"node{index + 1}"
                node_root.mkdir()
                log = (attempt_root / f"node{index + 1}.log").open("w")
                command = [
                    binary,
                    "-cluster-enabled",
                    "-node-id",
                    f"node{index + 1}",
                    "-disk",
                    str(node_root),
                    "-listen-addr",
                    f"{HOST}:{grpc_ports[index]}",
                    "-listen-http",
                    f"{HOST}:{http_ports[index]}",
                    "-cluster-addr",
                    f"{HOST}:{gossip_ports[index]}",
                    "-seeds",
                    seeds,
                ]
                processes.append(subprocess.Popen(command, stdout=log, stderr=log))
                wait_for_http(http_ports[index], "/ready")
                log.close()
            topology = wait_for_http(http_ports[0], "/v1/topology", topology_ready, timeout=90)
            return processes, grpc_ports, http_ports, topology
        except Exception:
            stop_cluster(processes)
            if attempt == 4:
                raise
            time.sleep(0.2)


def stop_cluster(processes):
    for process in processes:
        if process.poll() is None:
            process.terminate()
    for process in processes:
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()


def emit(metric, value):
    print(json.dumps({"metric": metric, "value": value}, separators=(",", ":")))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", required=True)
    parser.add_argument("--size", type=int, required=True)
    parser.add_argument("--requests", type=int, default=30)
    args = parser.parse_args()
    if args.size <= 0 or args.requests <= 0:
        raise ValueError("size and requests must be positive")

    processes = []
    with tempfile.TemporaryDirectory(prefix="ocache-http-gateway-bench-") as temporary:
        try:
            processes, _, http_ports, topology = start_cluster(args.binary, pathlib.Path(temporary))
            tokens = node_tokens(topology)
            key = None
            owner = None
            for candidate in (f"http-gateway-{args.size}-{index}" for index in range(10000)):
                candidate_owner = owner_for(candidate, tokens)
                if candidate_owner != "node1":
                    key = candidate
                    owner = candidate_owner
                    break
            if key is None:
                raise RuntimeError("could not find a key owned by a node other than node1")
            owner_index = int(owner.removeprefix("node")) - 1

            body = json.dumps(
                {"data": base64.b64encode(b"x" * args.size).decode()},
                separators=(",", ":"),
            ).encode()
            put_requests(http_ports[0], key, body, 10)

            before_gateway_put = rpc_count(http_ports[0], "PutObject")
            before_owner_put_object = rpc_count(http_ports[owner_index], "PutObject")
            before_owner_put_stream = rpc_count(http_ports[owner_index], "Put", ("success", "forwarded"))
            before_cpu = sum(scalar_metric(fetch_text(port, "/metrics"), "process_cpu_seconds_total") for port in http_ports)

            latencies = put_requests(http_ports[0], key, body, args.requests)

            after_cpu = sum(scalar_metric(fetch_text(port, "/metrics"), "process_cpu_seconds_total") for port in http_ports)
            gateway_put = rpc_count(http_ports[0], "PutObject") - before_gateway_put
            owner_put_object = rpc_count(http_ports[owner_index], "PutObject") - before_owner_put_object
            owner_put_stream = rpc_count(http_ports[owner_index], "Put", ("success", "forwarded")) - before_owner_put_stream
            cpu_us_per_request = (after_cpu - before_cpu) * 1_000_000 / args.requests
            ordered = sorted(latencies)
            p50_us = ordered[len(ordered) // 2] * 1_000_000

            emit("http-put-p50-us", p50_us)
            emit("server-cpu-us-per-request", cpu_us_per_request)
            emit("gateway-putobject-per-request", gateway_put / args.requests)
            emit("owner-putobject-per-request", owner_put_object / args.requests)
            emit("owner-putstream-per-request", owner_put_stream / args.requests)
        finally:
            stop_cluster(processes)


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(f"benchmark failed: {error}", file=sys.stderr)
        raise
