"""Integration checks for this project's Docker cluster; restores nodes in finally."""
import concurrent.futures
import json
import os
import subprocess
import time
import urllib.error
import urllib.request
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
URLS = {f"node{i}": f"http://127.0.0.1:{18080+i}" for i in range(1, 4)}
CLIENT = "verify-" + uuid.uuid4().hex
KEY = CLIENT
SEQ = 0
TOKEN = os.environ.get("API_TOKEN", "")

def docker(*args):
    return subprocess.check_output(["docker", *args], cwd=ROOT, text=True).strip()

def request(node, method, path, body=None, timeout=8):
    data = None if body is None else json.dumps(body).encode()
    headers = {"Content-Type": "application/json"}
    if TOKEN:
        headers["Authorization"] = "Bearer " + TOKEN
    req = urllib.request.Request(URLS[node] + path, data=data, method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            return response.status, json.load(response)
    except urllib.error.HTTPError as error:
        return error.code, json.load(error)

def leader(exclude=()):
    end = time.monotonic() + 30
    while time.monotonic() < end:
        for node in URLS:
            if node in exclude:
                continue
            try:
                code, state = request(node, "GET", "/readyz", timeout=2)
                if code == 200:
                    return node
            except (OSError, ValueError):
                pass
        time.sleep(.3)
    raise AssertionError("no ready leader")

def write(method, suffix="", **fields):
    global SEQ
    SEQ += 1
    body = dict(client_id=CLIENT, sequence=SEQ, **fields)
    path = "/v1/kv/" + KEY + suffix
    node = leader()
    code, result = request(node, method, path, body)
    assert code == 200, (code, result)
    return node, body, result

def read(expected):
    code, result = request(leader(), "GET", "/v1/kv/" + KEY)
    assert code == 200 and result["value"] == expected and result["exists"], result

def main():
    global SEQ
    isolated = None
    containers = {node: docker("compose", "ps", "-q", node) for node in URLS}
    assert all(containers.values()), "start all three nodes first"
    inspection = json.loads(docker("inspect", containers["node1"]))[0]
    network = next(iter(inspection["NetworkSettings"]["Networks"]))
    try:
        node, _, _ = write("PUT", value="a")
        read("a")
        node, body, result = write("POST", "/cas", expected="a", expected_exists=True, value="b")
        assert result["applied"]
        code, duplicate = request(node, "POST", "/v1/kv/" + KEY + "/cas", body)
        assert code == 200 and duplicate == result, duplicate
        conflict = dict(body, value="bad")
        code, problem = request(node, "POST", "/v1/kv/" + KEY + "/cas", conflict)
        assert code == 409 and problem["error"] == "sequence_conflict", problem
        print("PASS: put/get, CAS, retry dedup, sequence conflict", flush=True)

        # Concurrent contenders must produce exactly one successful CAS.
        def contender(i):
            return request(node, "POST", "/v1/kv/" + KEY + "/cas", dict(client_id=CLIENT+str(i), sequence=1, expected="b", expected_exists=True, value="c"))
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            outcomes = list(pool.map(contender, range(8)))
        assert all(code == 200 for code, _ in outcomes)
        assert sum(result["applied"] for _, result in outcomes) == 1
        read("c")
        print("PASS: concurrent CAS has exactly one winner", flush=True)

        # Partition the leader without modifying the host firewall.
        isolated = leader()
        docker("network", "disconnect", network, containers[isolated])
        new = leader(exclude=(isolated,))
        assert new != isolated
        try:
            code, _ = request(isolated, "GET", "/v1/kv/" + KEY, timeout=7)
            assert code != 200, "isolated node served a strong read"
        except OSError:
            pass  # Docker Desktop may make its published port unreachable.
        read("c")
        docker("network", "connect", "--alias", isolated, network, containers[isolated])
        isolated = None
        print("PASS: leader partition, majority failover, no isolated strong read", flush=True)

        old = leader()
        docker("compose", "kill", "-s", "SIGKILL", old)
        # restart policy is disabled temporarily by Compose kill; start explicitly later.
        replacement = leader(exclude=(old,))
        assert replacement != old
        read("c")
        docker("compose", "start", old)
        print("PASS: leader crash and majority recovery", flush=True)

        # Capture both data and last-request dedup, then restart the whole cluster.
        node, body, result = write("POST", "/cas", expected="c", expected_exists=True, value="durable")
        code, snap = request(node, "POST", "/v1/admin/snapshot")
        assert code == 200, snap
        docker("compose", "restart")
        node = leader()
        read("durable")
        code, duplicate = request(node, "POST", "/v1/kv/" + KEY + "/cas", body)
        assert code == 200 and duplicate == result, duplicate
        print("PASS: snapshot, full restart, durable data and dedup", flush=True)

        # With two nodes stopped, acknowledged writes must be unavailable.
        node = leader()
        stopped = [n for n in URLS if n != node]
        docker("compose", "stop", *stopped)
        SEQ += 1
        try:
            code, _ = request(node, "PUT", "/v1/kv/"+KEY, dict(client_id=CLIENT, sequence=SEQ, value="uncertain"))
            assert code != 200, "write succeeded without quorum"
        except OSError:
            pass
        docker("compose", "start", *stopped)
        leader()
        # Failed writes can have an unknown outcome; deliberately do not assert their absence.
        write("DELETE")
        code, missing = request(leader(), "GET", "/v1/kv/" + KEY)
        assert code == 200 and not missing["exists"], missing
        print("PASS: no-quorum write rejection and delete", flush=True)
    finally:
        if isolated:
            docker("network", "connect", "--alias", isolated, network, containers[isolated])
        docker("compose", "start")

if __name__ == "__main__":
    main()
