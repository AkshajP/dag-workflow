# dag_engine/graph.py

from __future__ import annotations

from collections import defaultdict, deque
from typing import Dict, List, Set

from dag_engine.logger import graph_logger
from dag_engine.node import BaseNode


class CycleDetectedError(Exception):
    pass


class NodeNotFoundError(KeyError):
    pass


class DAGGraph:
    """
    Manages the DAG structure: nodes, directed edges (dependency arrows),
    cycle detection, and topological ordering.

    Edge semantics: edge (A → B) means "B depends on A" — A must run before B.
    """

    def __init__(self, graph_id: str):
        self.graph_id = graph_id
        self._nodes: Dict[str, BaseNode] = {}
        # adjacency list: _deps[node_id] = {set of upstream node_ids this node depends on}
        self._deps: Dict[str, Set[str]] = defaultdict(set)
        # reverse map: _rdeps[node_id] = {set of downstream nodes that depend on this}
        self._rdeps: Dict[str, Set[str]] = defaultdict(set)

    # ------------------------------------------------------------------
    # Node management
    # ------------------------------------------------------------------

    def add_node(self, node: BaseNode) -> None:
        if node.node_id in self._nodes:
            graph_logger.warning(f"[{self.graph_id}] Node '{node.node_id}' already exists — overwriting")
        self._nodes[node.node_id] = node
        self._deps.setdefault(node.node_id, set())
        self._rdeps.setdefault(node.node_id, set())
        graph_logger.debug(f"[{self.graph_id}] Added node '{node.node_id}'")

    def remove_node(self, node_id: str) -> None:
        self._require_node(node_id)
        # clean up edges
        for upstream in list(self._deps[node_id]):
            self._rdeps[upstream].discard(node_id)
        for downstream in list(self._rdeps[node_id]):
            self._deps[downstream].discard(node_id)
        del self._nodes[node_id]
        del self._deps[node_id]
        del self._rdeps[node_id]
        graph_logger.debug(f"[{self.graph_id}] Removed node '{node_id}'")

    # ------------------------------------------------------------------
    # Edge management
    # ------------------------------------------------------------------

    def add_edge(self, upstream_id: str, downstream_id: str) -> None:
        """upstream_id → downstream_id  (downstream depends on upstream)"""
        self._require_node(upstream_id)
        self._require_node(downstream_id)
        if upstream_id == downstream_id:
            raise CycleDetectedError(f"Self-loop on '{upstream_id}'")
        self._deps[downstream_id].add(upstream_id)
        self._rdeps[upstream_id].add(downstream_id)
        graph_logger.debug(f"[{self.graph_id}] Edge: '{upstream_id}' → '{downstream_id}'")
        self._validate_no_cycle()

    def remove_edge(self, upstream_id: str, downstream_id: str) -> None:
        self._deps[downstream_id].discard(upstream_id)
        self._rdeps[upstream_id].discard(downstream_id)
        graph_logger.debug(f"[{self.graph_id}] Removed edge: '{upstream_id}' → '{downstream_id}'")

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def _validate_no_cycle(self) -> None:
        """Kahn's algorithm — O(V+E)."""
        in_degree: Dict[str, int] = {n: len(deps) for n, deps in self._deps.items()}
        queue: deque[str] = deque(n for n, d in in_degree.items() if d == 0)
        visited = 0
        while queue:
            node = queue.popleft()
            visited += 1
            for downstream in self._rdeps[node]:
                in_degree[downstream] -= 1
                if in_degree[downstream] == 0:
                    queue.append(downstream)
        if visited != len(self._nodes):
            raise CycleDetectedError(
                f"[{self.graph_id}] Cycle detected — graph is not a valid DAG"
            )

    def validate(self) -> None:
        """Public validation call. Raises CycleDetectedError if invalid."""
        graph_logger.info(f"[{self.graph_id}] Validating graph ({len(self._nodes)} nodes)")
        self._validate_no_cycle()
        graph_logger.info(f"[{self.graph_id}] Graph is valid")

    # ------------------------------------------------------------------
    # Topological ordering
    # ------------------------------------------------------------------

    def topological_sort(self) -> List[str]:
        """Returns a valid execution order (Kahn's). Raises on cycle."""
        self._validate_no_cycle()
        in_degree: Dict[str, int] = {n: len(deps) for n, deps in self._deps.items()}
        queue: deque[str] = deque(sorted(n for n, d in in_degree.items() if d == 0))
        order: List[str] = []
        while queue:
            node = queue.popleft()
            order.append(node)
            for downstream in sorted(self._rdeps[node]):
                in_degree[downstream] -= 1
                if in_degree[downstream] == 0:
                    queue.append(downstream)
        graph_logger.debug(f"[{self.graph_id}] Topo order: {order}")
        return order

    # ------------------------------------------------------------------
    # Downstream subgraph (for partial re-execution)
    # ------------------------------------------------------------------

    def downstream_of(self, node_id: str) -> Set[str]:
        """BFS from node_id following rdeps. Returns node_id + all descendants."""
        self._require_node(node_id)
        visited: Set[str] = set()
        queue: deque[str] = deque([node_id])
        while queue:
            current = queue.popleft()
            if current in visited:
                continue
            visited.add(current)
            for child in self._rdeps[current]:
                queue.append(child)
        return visited

    # ------------------------------------------------------------------
    # Accessors
    # ------------------------------------------------------------------

    def get_node(self, node_id: str) -> BaseNode:
        self._require_node(node_id)
        return self._nodes[node_id]

    def node_ids(self) -> List[str]:
        return list(self._nodes.keys())

    def dependencies_of(self, node_id: str) -> Set[str]:
        return set(self._deps[node_id])

    def dependents_of(self, node_id: str) -> Set[str]:
        return set(self._rdeps[node_id])

    def _require_node(self, node_id: str) -> None:
        if node_id not in self._nodes:
            raise NodeNotFoundError(f"Node '{node_id}' not found in graph '{self.graph_id}'")

    def summary(self) -> dict:
        return {
            "graph_id": self.graph_id,
            "nodes": list(self._nodes.keys()),
            "edges": [
                {"from": upstream, "to": downstream}
                for downstream, upstreams in self._deps.items()
                for upstream in upstreams
            ],
        }