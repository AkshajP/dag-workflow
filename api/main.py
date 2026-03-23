# api/main.py

from __future__ import annotations

import uuid
from typing import Any, Callable, Dict, List, Optional

from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, Field

from dag_engine import (
    DAGExecutionEngine,
    DAGGraph,
    EchoNode,
    FailNode,
    LambdaNode,
    NodeStatus,
    SleepNode,
)
from dag_engine.graph import CycleDetectedError, NodeNotFoundError
from dag_engine.logger import api_logger

app = FastAPI(title="DAG Execution Engine", version="1.0.0")

# ---------------------------------------------------------------------------
# In-memory registry
# ---------------------------------------------------------------------------
# graph_id  → DAGGraph
_graphs: Dict[str, DAGGraph] = {}
# graph_id  → DAGExecutionEngine  (one engine per graph, carries state)
_engines: Dict[str, DAGExecutionEngine] = {}


# ---------------------------------------------------------------------------
# Request / Response models
# ---------------------------------------------------------------------------

class CreateGraphRequest(BaseModel):
    graph_id: Optional[str] = Field(default=None, description="Optional; auto-generated if omitted")


class AddNodeRequest(BaseModel):
    node_id: str
    node_type: str = Field(
        description="One of: lambda | echo | sleep | fail",
        examples=["lambda", "sleep"],
    )
    config: Dict[str, Any] = Field(default_factory=dict)
    # For lambda nodes, supply a simple Python expression evaluated with inputs + config
    lambda_expr: Optional[str] = Field(
        default=None,
        description=(
            "Python expression string. Available vars: `inputs` (dict), `config` (dict). "
            "Must evaluate to a dict. Example: \"{'result': sum(v.get('value',0) "
            "for v in inputs.values())}\""
        ),
    )


class AddEdgeRequest(BaseModel):
    upstream_id: str
    downstream_id: str


class GraphResponse(BaseModel):
    graph_id: str
    nodes: List[str]
    edges: List[Dict[str, str]]


class RunRequest(BaseModel):
    execution_id: Optional[str] = None


class RerunFromRequest(BaseModel):
    node_id: str


class NodeStatusResponse(BaseModel):
    node_id: str
    status: str
    output: Optional[Dict[str, Any]]
    error: Optional[str]
    duration_s: Optional[float]
    attempt: int


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_graph(graph_id: str) -> DAGGraph:
    if graph_id not in _graphs:
        raise HTTPException(status_code=404, detail=f"Graph '{graph_id}' not found")
    return _graphs[graph_id]


def _get_engine(graph_id: str) -> DAGExecutionEngine:
    if graph_id not in _engines:
        raise HTTPException(
            status_code=404,
            detail=f"No execution found for graph '{graph_id}'. Run it first.",
        )
    return _engines[graph_id]


def _build_node(req: AddNodeRequest):
    nt = req.node_type.lower()
    if nt == "echo":
        return EchoNode(req.node_id, req.config)
    if nt == "sleep":
        return SleepNode(req.node_id, req.config)
    if nt == "fail":
        return FailNode(req.node_id, req.config)
    if nt == "lambda":
        if not req.lambda_expr:
            raise HTTPException(
                status_code=422, detail="lambda_expr required for node_type='lambda'"
            )
        expr = req.lambda_expr

        def _fn(inputs: dict, config: dict) -> dict:
            result = eval(expr, {"inputs": inputs, "config": config})  # noqa: S307
            if not isinstance(result, dict):
                raise TypeError(f"lambda_expr must evaluate to dict, got {type(result)}")
            return result

        return LambdaNode(req.node_id, _fn, req.config)
    raise HTTPException(status_code=422, detail=f"Unknown node_type '{req.node_type}'")


# ---------------------------------------------------------------------------
# Routes — Graph Management
# ---------------------------------------------------------------------------

@app.post("/graphs", status_code=status.HTTP_201_CREATED)
def create_graph(req: CreateGraphRequest) -> GraphResponse:
    graph_id = req.graph_id or str(uuid.uuid4())
    if graph_id in _graphs:
        raise HTTPException(status_code=409, detail=f"Graph '{graph_id}' already exists")
    _graphs[graph_id] = DAGGraph(graph_id)
    api_logger.info(f"Created graph '{graph_id}'")
    return GraphResponse(graph_id=graph_id, nodes=[], edges=[])


@app.get("/graphs/{graph_id}")
def get_graph(graph_id: str) -> GraphResponse:
    g = _get_graph(graph_id)
    s = g.summary()
    return GraphResponse(graph_id=graph_id, nodes=s["nodes"], edges=s["edges"])


@app.delete("/graphs/{graph_id}", status_code=204)
def delete_graph(graph_id: str):
    _get_graph(graph_id)
    del _graphs[graph_id]
    _engines.pop(graph_id, None)
    api_logger.info(f"Deleted graph '{graph_id}'")


@app.post("/graphs/{graph_id}/nodes", status_code=201)
def add_node(graph_id: str, req: AddNodeRequest):
    g = _get_graph(graph_id)
    node = _build_node(req)
    try:
        g.add_node(node)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    api_logger.info(f"[{graph_id}] Added node '{req.node_id}' ({req.node_type})")
    return {"node_id": req.node_id, "node_type": req.node_type}


@app.delete("/graphs/{graph_id}/nodes/{node_id}", status_code=204)
def remove_node(graph_id: str, node_id: str):
    g = _get_graph(graph_id)
    try:
        g.remove_node(node_id)
    except NodeNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))


@app.post("/graphs/{graph_id}/edges", status_code=201)
def add_edge(graph_id: str, req: AddEdgeRequest):
    g = _get_graph(graph_id)
    try:
        g.add_edge(req.upstream_id, req.downstream_id)
    except CycleDetectedError as e:
        raise HTTPException(status_code=422, detail=str(e))
    except NodeNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    api_logger.info(f"[{graph_id}] Edge {req.upstream_id} → {req.downstream_id}")
    return {"upstream": req.upstream_id, "downstream": req.downstream_id}


@app.delete("/graphs/{graph_id}/edges", status_code=204)
def remove_edge(graph_id: str, req: AddEdgeRequest):
    g = _get_graph(graph_id)
    g.remove_edge(req.upstream_id, req.downstream_id)


@app.get("/graphs/{graph_id}/validate")
def validate_graph(graph_id: str):
    g = _get_graph(graph_id)
    try:
        g.validate()
        return {"valid": True}
    except CycleDetectedError as e:
        return {"valid": False, "detail": str(e)}


# ---------------------------------------------------------------------------
# Routes — Execution
# ---------------------------------------------------------------------------

@app.post("/graphs/{graph_id}/run")
def run_graph(graph_id: str, req: RunRequest = RunRequest()):
    g = _get_graph(graph_id)
    engine = DAGExecutionEngine(g)
    _engines[graph_id] = engine
    try:
        store = engine.run(execution_id=req.execution_id)
    except CycleDetectedError as e:
        raise HTTPException(status_code=422, detail=str(e))
    api_logger.info(f"[{graph_id}] Execution '{store.execution_id}' complete")
    return store.snapshot()


@app.post("/graphs/{graph_id}/retry")
def retry_failed(graph_id: str):
    engine = _get_engine(graph_id)
    store = engine.retry_failed()
    return store.snapshot()


@app.post("/graphs/{graph_id}/rerun-from")
def rerun_from(graph_id: str, req: RerunFromRequest):
    engine = _get_engine(graph_id)
    try:
        store = engine.rerun_from(req.node_id)
    except NodeNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    return store.snapshot()


# ---------------------------------------------------------------------------
# Routes — State inspection
# ---------------------------------------------------------------------------

@app.get("/graphs/{graph_id}/state")
def get_state(graph_id: str):
    engine = _get_engine(graph_id)
    return engine.state.snapshot()


@app.get("/graphs/{graph_id}/state/{node_id}")
def get_node_state(graph_id: str, node_id: str) -> NodeStatusResponse:
    engine = _get_engine(graph_id)
    store = engine.state
    try:
        rec = store.all_records()[node_id]
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Node '{node_id}' not in execution state")
    return NodeStatusResponse(
        node_id=node_id,
        status=rec["status"],
        output=rec["output"],
        error=rec["error"],
        duration_s=rec.get("duration_s"),
        attempt=rec["attempt"],
    )