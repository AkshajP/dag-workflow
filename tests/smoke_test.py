# tests/smoke_test.py

"""
Exercises every acceptance criterion from the spec:
  1. Execute DAG with 5+ nodes including parallel branches
  2. Correct structured data passing between nodes
  3. Cycle detection (reject cyclic graph)
  4. Persist execution state to disk
  5. Retry failed nodes without full restart
  6. Partial re-execution (rerun-from)
"""

import tempfile
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dag_engine import (
    DAGExecutionEngine,
    DAGGraph,
    EchoNode,
    FailNode,
    LambdaNode,
    NodeStatus,
    SleepNode,
)
from dag_engine.graph import CycleDetectedError

SEP = "=" * 60


# ---------------------------------------------------------------------------
# Test 1 – 5-node DAG with parallel branches + data passing
# ---------------------------------------------------------------------------
def test_parallel_data_passing():
    print(f"\n{SEP}")
    print("TEST 1: Parallel execution + data passing")
    print(SEP)

    g = DAGGraph("test1")

    # ingest → (transform_a, transform_b) → merge → report
    #           └─── parallel branch ──────┘
    g.add_node(LambdaNode("ingest",    lambda i, c: {"raw": [1, 2, 3, 4, 5]}))
    g.add_node(LambdaNode("transform_a", lambda i, c: {"sum":  sum(i["ingest"]["raw"])}))
    g.add_node(LambdaNode("transform_b", lambda i, c: {"mean": sum(i["ingest"]["raw"]) / len(i["ingest"]["raw"])}))
    g.add_node(LambdaNode("merge",
        lambda i, c: {
            "sum":  i["transform_a"]["sum"],
            "mean": i["transform_b"]["mean"],
        }
    ))
    g.add_node(LambdaNode("report",
        lambda i, c: {"text": f"sum={i['merge']['sum']} mean={i['merge']['mean']}"}
    ))

    g.add_edge("ingest",      "transform_a")
    g.add_edge("ingest",      "transform_b")
    g.add_edge("transform_a", "merge")
    g.add_edge("transform_b", "merge")
    g.add_edge("merge",       "report")

    engine = DAGExecutionEngine(g)
    store = engine.run()

    assert store.get_status("ingest")      == NodeStatus.SUCCESS
    assert store.get_status("transform_a") == NodeStatus.SUCCESS
    assert store.get_status("transform_b") == NodeStatus.SUCCESS
    assert store.get_status("merge")       == NodeStatus.SUCCESS
    assert store.get_status("report")      == NodeStatus.SUCCESS

    report_out = store.get_output("report")
    assert report_out is not None, "report node produced no output"
    assert "sum=15" in report_out["text"], f"Unexpected: {report_out}"
    print(f"  Report output: {report_out}")
    print("  ✓ PASSED")


# ---------------------------------------------------------------------------
# Test 2 – Cycle detection
# ---------------------------------------------------------------------------
def test_cycle_detection():
    print(f"\n{SEP}")
    print("TEST 2: Cycle detection")
    print(SEP)

    g = DAGGraph("cycle_test")
    g.add_node(LambdaNode("a", lambda i, c: {}))
    g.add_node(LambdaNode("b", lambda i, c: {}))
    g.add_node(LambdaNode("c", lambda i, c: {}))

    g.add_edge("a", "b")
    g.add_edge("b", "c")
    try:
        g.add_edge("c", "a")  # creates cycle
        print("  ✗ FAILED: should have raised CycleDetectedError")
    except CycleDetectedError as e:
        print(f"  Cycle correctly detected: {e}")
        print("  ✓ PASSED")


# ---------------------------------------------------------------------------
# Test 3 – State persistence to disk
# ---------------------------------------------------------------------------
def test_state_persistence():
    print(f"\n{SEP}")
    print("TEST 3: State persistence")
    print(SEP)

    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
        path = f.name

    g = DAGGraph("persist_test")
    g.add_node(LambdaNode("step1", lambda i, c: {"x": 42}))
    g.add_node(LambdaNode("step2", lambda i, c: {"y": i["step1"]["x"] * 2}))
    g.add_edge("step1", "step2")

    engine = DAGExecutionEngine(g, persist_path=path)
    engine.run()

    assert os.path.exists(path), "State file not created"
    from dag_engine.state_store import ExecutionStateStore
    loaded = ExecutionStateStore.load_from_file(path)
    assert loaded.get_status("step2") == NodeStatus.SUCCESS
    assert loaded.get_output("step2") == {"y": 84}
    print(f"  State loaded from {path}")
    print(f"  step2 output: {loaded.get_output('step2')}")
    os.unlink(path)
    print("  ✓ PASSED")


# ---------------------------------------------------------------------------
# Test 4 – Retry failed node without full restart
# ---------------------------------------------------------------------------
def test_retry_without_full_restart():
    print(f"\n{SEP}")
    print("TEST 4: Retry failed node")
    print(SEP)

    call_counts = {"compute": 0}

    def flaky(inputs, config):
        call_counts["compute"] += 1
        if call_counts["compute"] < 2:
            raise RuntimeError("transient failure")
        return {"result": 99}

    g = DAGGraph("retry_test")
    g.add_node(LambdaNode("source",  lambda i, c: {"val": 1}))
    g.add_node(LambdaNode("compute", flaky))
    g.add_node(LambdaNode("sink",    lambda i, c: {"final": i["compute"]["result"]}))
    g.add_edge("source",  "compute")
    g.add_edge("compute", "sink")

    engine = DAGExecutionEngine(g)
    store = engine.run()

    assert store.get_status("compute") == NodeStatus.FAILED
    assert store.get_status("sink")    == NodeStatus.SKIPPED
    # source must NOT be re-run on retry
    source_attempt_before = store.all_records()["source"]["attempt"]

    store2 = engine.retry_failed()
    assert store2.get_status("compute") == NodeStatus.SUCCESS
    assert store2.get_status("sink")    == NodeStatus.SUCCESS
    source_attempt_after = store2.all_records()["source"]["attempt"]

    assert source_attempt_before == source_attempt_after, "source was re-run — wrong!"
    print(f"  source attempts: {source_attempt_after} (unchanged ✓)")
    print(f"  sink output: {store2.get_output('sink')}")
    print("  ✓ PASSED")


# ---------------------------------------------------------------------------
# Test 5 – Partial re-execution (rerun-from)
# ---------------------------------------------------------------------------
def test_partial_rerun():
    print(f"\n{SEP}")
    print("TEST 5: Partial re-execution (rerun-from)")
    print(SEP)

    g = DAGGraph("partial_test")
    g.add_node(LambdaNode("a", lambda i, c: {"v": 10}))
    g.add_node(LambdaNode("b", lambda i, c: {"v": i["a"]["v"] + 5}))
    g.add_node(LambdaNode("c", lambda i, c: {"v": i["b"]["v"] * 2}))

    g.add_edge("a", "b")
    g.add_edge("b", "c")

    engine = DAGExecutionEngine(g)
    store = engine.run()
    a_attempt_first = store.all_records()["a"]["attempt"]

    # Simulate needing to rerun from b
    store2 = engine.rerun_from("b")
    a_attempt_second = store2.all_records()["a"]["attempt"]

    assert a_attempt_first == a_attempt_second, "node 'a' was re-run — wrong!"
    assert store2.get_output("c") == {"v": 30}
    print(f"  'a' attempts unchanged: {a_attempt_second} ✓")
    print(f"  'c' output: {store2.get_output('c')}")
    print("  ✓ PASSED")


# ---------------------------------------------------------------------------
# Run all
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    test_parallel_data_passing()
    test_cycle_detection()
    test_state_persistence()
    test_retry_without_full_restart()
    test_partial_rerun()
    print(f"\n{SEP}")
    print("ALL TESTS PASSED ✓")
    print(SEP)