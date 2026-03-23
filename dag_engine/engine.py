# dag_engine/engine.py

from __future__ import annotations

import threading
import time
import traceback
import uuid
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Set

from dag_engine.graph import DAGGraph
from dag_engine.logger import engine_logger
from dag_engine.node import BaseNode
from dag_engine.state_store import ExecutionStateStore, NodeStatus


class ExecutionError(Exception):
    pass


class DAGExecutionEngine:
    """
    Orchestrates parallel, stateful execution of a DAGGraph.

    - Nodes whose dependencies are all SUCCESS are submitted to a thread pool.
    - A failed node marks itself FAILED and all strict descendants SKIPPED.
    - Supports partial re-execution: reset a subset of nodes and rerun.
    """

    def __init__(
        self,
        graph: DAGGraph,
        max_workers: int = 8,
        persist_path: str | None = None,
    ):
        self.graph = graph
        self.max_workers = max_workers
        self._persist_path = persist_path
        self._state: Optional[ExecutionStateStore] = None
        self._execution_id: Optional[str] = None

    @property
    def state(self) -> ExecutionStateStore:
        """Returns the current execution state. Raises if run() has not been called yet."""
        if self._state is None:
            raise ExecutionError("No execution state available. Call run() first.")
        return self._state

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(self, execution_id: str | None = None) -> ExecutionStateStore:
        """Full execution from scratch."""
        self.graph.validate()
        eid = execution_id or str(uuid.uuid4())
        self._execution_id = eid
        engine_logger.info(f"[{eid}] Starting full execution of graph '{self.graph.graph_id}'")

        store = ExecutionStateStore(eid, persist_path=self._persist_path)
        store.register_nodes(self.graph.node_ids())
        self._state = store

        self._execute_subgraph(self.graph.node_ids(), store)

        self._log_summary(store)
        return store

    def retry_failed(self) -> ExecutionStateStore:
        """
        Re-run all FAILED nodes and their downstream dependents.
        Preserves SUCCESS outputs from prior run.
        """
        if self._state is None:
            raise ExecutionError("No prior execution. Call run() first.")

        store = self._state
        failed = store.failed_nodes()
        if not failed:
            engine_logger.info("No failed nodes — nothing to retry")
            return store

        engine_logger.info(f"Retrying failed nodes: {failed}")
        nodes_to_reset: Set[str] = set()
        for node_id in failed:
            nodes_to_reset |= self.graph.downstream_of(node_id)

        for nid in nodes_to_reset:
            store.reset_node(nid)

        self._execute_subgraph(self.graph.node_ids(), store)
        self._log_summary(store)
        return store

    def rerun_from(self, node_id: str) -> ExecutionStateStore:
        """
        Reset node_id + all its descendants and re-execute the full graph
        (already-success nodes upstream are NOT re-run).
        """
        if self._state is None:
            raise ExecutionError("No prior execution. Call run() first.")

        store = self._state
        nodes_to_reset = self.graph.downstream_of(node_id)
        engine_logger.info(f"Partial re-run: resetting {nodes_to_reset}")
        for nid in nodes_to_reset:
            store.reset_node(nid)

        self._execute_subgraph(self.graph.node_ids(), store)
        self._log_summary(store)
        return store

    # ------------------------------------------------------------------
    # Internal execution loop
    # ------------------------------------------------------------------

    def _execute_subgraph(
        self,
        all_node_ids: List[str],
        store: ExecutionStateStore,
    ) -> None:
        """
        Wave-based parallel execution.
        Continuously finds nodes whose deps are SUCCESS and submits them.
        Stops when nothing is runnable or all are done.
        """
        submitted: Set[str] = set()
        futures: Dict[Future, str] = {}

        with ThreadPoolExecutor(max_workers=self.max_workers) as pool:
            while True:
                # collect completed futures
                done_futures = [f for f in list(futures) if f.done()]
                for fut in done_futures:
                    del futures[fut]

                # find newly eligible nodes
                eligible = self._find_eligible(all_node_ids, store, submitted)
                for nid in eligible:
                    submitted.add(nid)
                    fut = pool.submit(self._run_node, nid, store)
                    futures[fut] = nid
                    engine_logger.debug(f"Submitted node '{nid}'")

                if not futures:
                    # nothing running — check if we're stuck or done
                    remaining = [
                        n for n in all_node_ids
                        if store.get_status(n) == NodeStatus.PENDING
                        and n not in submitted
                    ]
                    if remaining:
                        # remaining nodes are blocked by failed upstream — skip them
                        for nid in remaining:
                            engine_logger.warning(f"Skipping '{nid}' — upstream failed")
                            store.mark_skipped(nid)
                    break

                # wait for at least one future to finish before next wave
                next(as_completed(futures), None)

    def _find_eligible(
        self,
        all_node_ids: List[str],
        store: ExecutionStateStore,
        submitted: Set[str],
    ) -> List[str]:
        eligible = []
        for nid in all_node_ids:
            if store.get_status(nid) != NodeStatus.PENDING or nid in submitted:
                continue
            deps = self.graph.dependencies_of(nid)
            if all(store.get_status(dep) == NodeStatus.SUCCESS for dep in deps):
                eligible.append(nid)
        return eligible

    def _run_node(self, node_id: str, store: ExecutionStateStore) -> None:
        node: BaseNode = self.graph.get_node(node_id)
        store.mark_running(node_id)
        engine_logger.info(f"  ▶ Running  '{node_id}'")

        try:
            inputs = self._collect_inputs(node_id, store)
            output = node.execute(inputs)
            if not isinstance(output, dict):
                raise TypeError(f"Node '{node_id}' must return dict, got {type(output).__name__}")
            store.mark_success(node_id, output)
            engine_logger.info(f"  ✓ Success  '{node_id}' → {list(output.keys())}")
        except Exception as exc:  # noqa: BLE001
            err_msg = traceback.format_exc()
            store.mark_failed(node_id, str(exc))
            engine_logger.error(f"  ✗ Failed   '{node_id}': {exc}")
            engine_logger.debug(f"Traceback for '{node_id}':\n{err_msg}")
            # mark strict downstream as SKIPPED
            for downstream in self.graph.downstream_of(node_id) - {node_id}:
                if store.get_status(downstream) == NodeStatus.PENDING:
                    store.mark_skipped(downstream)

    def _collect_inputs(
        self, node_id: str, store: ExecutionStateStore
    ) -> Dict[str, Any]:
        inputs: Dict[str, Any] = {}
        for dep_id in self.graph.dependencies_of(node_id):
            output = store.get_output(dep_id)
            inputs[dep_id] = output if output is not None else {}
        return inputs

    # ------------------------------------------------------------------
    # Logging helpers
    # ------------------------------------------------------------------

    def _log_summary(self, store: ExecutionStateStore) -> None:
        records = store.all_records()
        lines = ["", f"  {'Node':<30} {'Status':<10} {'Duration':>10}  Attempt"]
        lines.append("  " + "-" * 62)
        for nid, rec in records.items():
            dur = f"{rec['duration_s']:.3f}s" if rec.get("duration_s") is not None else "—"
            lines.append(f"  {nid:<30} {rec['status']:<10} {dur:>10}  #{rec['attempt']}")
        engine_logger.info("\n".join(lines))