# dag_engine/visualizer.py

from __future__ import annotations

import base64
import io
from typing import TYPE_CHECKING, Dict, Optional

import matplotlib
matplotlib.use("Agg")  # non-interactive backend — no display required
import matplotlib.patches as mpatches
from matplotlib.figure import Figure
from matplotlib.axes import Axes
import matplotlib.pyplot as plt
import networkx as nx

if TYPE_CHECKING:
    from dag_engine.graph import DAGGraph
    from dag_engine.state_store import ExecutionStateStore, NodeStatus

from dag_engine.logger import get_logger

viz_logger = get_logger("dag.visualizer")

# ---------------------------------------------------------------------------
# Colour palette
# ---------------------------------------------------------------------------
_STATUS_COLOURS: Dict[str, str] = {
    "PENDING":  "#B0BEC5",   # blue-grey
    "RUNNING":  "#FFD54F",   # amber
    "SUCCESS":  "#66BB6A",   # green
    "FAILED":   "#EF5350",   # red
    "SKIPPED":  "#CE93D8",   # purple
    "UNKNOWN":  "#EEEEEE",   # fallback
}
_EDGE_COLOUR        = "#546E7A"
_GRAPH_BG           = "#FAFAFA"
_NODE_BORDER        = "#37474F"
_TITLE_COLOUR       = "#212121"
_LABEL_COLOUR       = "#FFFFFF"
_SUBTITLE_COLOUR    = "#757575"


def _build_nx_graph(dag: "DAGGraph") -> nx.DiGraph:
    G = nx.DiGraph()
    for nid in dag.node_ids():
        G.add_node(nid)
    for edge in dag.summary()["edges"]:
        G.add_edge(edge["from"], edge["to"])
    return G


def _fig_to_base64(fig: Figure) -> str:
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=150, bbox_inches="tight", facecolor=fig.get_facecolor())
    buf.seek(0)
    encoded = base64.b64encode(buf.read()).decode("utf-8")
    plt.close(fig)
    return encoded


def _layout(G: nx.DiGraph) -> dict:
    """
    Hierarchical top-down layout using topological generations.
    Falls back to spring layout if the graph has cycles (shouldn't happen, but safe).
    """
    try:
        # assign each node a (generation, index_within_generation) position
        generations = list(nx.topological_generations(G))
        pos = {}
        max_width = max(len(gen) for gen in generations)
        for depth, gen in enumerate(generations):
            gen = sorted(gen)
            width = len(gen)
            for i, node in enumerate(gen):
                # centre nodes horizontally within each layer
                x = (i - (width - 1) / 2) * (max_width / max(width, 1)) * 1.8
                y = -depth * 1.6
                pos[node] = (x, y)
        return pos
    except nx.NetworkXUnfeasible:
        viz_logger.warning("Cycle detected during layout — falling back to spring layout")
        return nx.spring_layout(G, seed=42)


def _draw_legend(ax: Axes, statuses: list[str]) -> None:
    patches = [
        mpatches.Patch(facecolor=_STATUS_COLOURS.get(s, _STATUS_COLOURS["UNKNOWN"]),
                       edgecolor=_NODE_BORDER, linewidth=0.8, label=s)
        for s in statuses
    ]
    ax.legend(
        handles=patches,
        loc="lower center",
        ncol=len(patches),
        framealpha=0.9,
        fontsize=7,
        handlelength=1.2,
        borderpad=0.6,
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def render_graph_structure(dag: "DAGGraph") -> str:
    """
    Renders the bare DAG topology (no execution state).
    Returns a base64-encoded PNG string.
    """
    viz_logger.info(f"Rendering structure for graph '{dag.graph_id}'")
    G = _build_nx_graph(dag)
    pos = _layout(G)

    node_colours = [_STATUS_COLOURS["PENDING"]] * len(G.nodes)
    node_list    = list(G.nodes)

    fig, ax = plt.subplots(figsize=(10, 6))
    fig.patch.set_facecolor(_GRAPH_BG)
    ax.set_facecolor(_GRAPH_BG)

    nx.draw_networkx_nodes(
        G, pos, ax=ax,
        nodelist=node_list,
        node_color=node_colours,
        node_size=2200,
        edgecolors=_NODE_BORDER,
        linewidths=1.5,
    )
    nx.draw_networkx_labels(
        G, pos, ax=ax,
        font_size=8,
        font_color=_TITLE_COLOUR,
        font_weight="bold",
    )
    nx.draw_networkx_edges(
        G, pos, ax=ax,
        edge_color=_EDGE_COLOUR,
        arrows=True,
        arrowstyle="-|>",
        arrowsize=18,
        width=1.8,
        connectionstyle="arc3,rad=0.05",
        min_source_margin=28,
        min_target_margin=28,
    )

    ax.set_title(
        f"DAG Structure — {dag.graph_id}",
        fontsize=13, fontweight="bold", color=_TITLE_COLOUR, pad=14,
    )
    ax.axis("off")
    fig.tight_layout()

    encoded = _fig_to_base64(fig)
    viz_logger.debug(f"Structure render complete ({len(encoded)} base64 chars)")
    return encoded


def render_execution_state(dag: "DAGGraph", store: "ExecutionStateStore") -> str:
    """
    Renders the DAG with each node coloured by its execution status.
    Node labels include status + duration. Returns a base64-encoded PNG string.
    """
    viz_logger.info(f"Rendering execution state for '{dag.graph_id}' / '{store.execution_id}'")
    G         = _build_nx_graph(dag)
    pos       = _layout(G)
    records   = store.all_records()
    node_list = list(G.nodes)

    node_colours = []
    for nid in node_list:
        status = records.get(nid, {}).get("status", "UNKNOWN")
        node_colours.append(_STATUS_COLOURS.get(status, _STATUS_COLOURS["UNKNOWN"]))

    # two-line labels: name + "STATUS · 0.003s"
    labels: Dict[str, str] = {}
    for nid in node_list:
        rec    = records.get(nid, {})
        status = rec.get("status", "?")
        dur    = rec.get("duration_s")
        dur_str = f"{dur:.3f}s" if dur is not None else "—"
        labels[nid] = f"{nid}\n{status} · {dur_str}"

    fig, ax = plt.subplots(figsize=(11, 7))
    fig.patch.set_facecolor(_GRAPH_BG)
    ax.set_facecolor(_GRAPH_BG)

    nx.draw_networkx_nodes(
        G, pos, ax=ax,
        nodelist=node_list,
        node_color=node_colours,
        node_size=2800,
        edgecolors=_NODE_BORDER,
        linewidths=1.5,
    )
    nx.draw_networkx_labels(
        G, pos, ax=ax,
        labels=labels,
        font_size=7,
        font_color=_TITLE_COLOUR,
        font_weight="bold",
    )
    nx.draw_networkx_edges(
        G, pos, ax=ax,
        edge_color=_EDGE_COLOUR,
        arrows=True,
        arrowstyle="-|>",
        arrowsize=18,
        width=1.8,
        connectionstyle="arc3,rad=0.05",
        min_source_margin=32,
        min_target_margin=32,
    )

    # title + subtitle
    present_statuses = sorted({r.get("status", "UNKNOWN") for r in records.values()})
    ax.set_title(
        f"Execution State — {dag.graph_id}\n"
        f"run: {store.execution_id[:16]}…",
        fontsize=12, fontweight="bold", color=_TITLE_COLOUR, pad=14,
        linespacing=1.6,
    )
    ax.axis("off")

    _draw_legend(ax, present_statuses)
    fig.tight_layout()

    encoded = _fig_to_base64(fig)
    viz_logger.debug(f"Execution render complete ({len(encoded)} base64 chars)")
    return encoded


def base64_to_img_tag(encoded: str, alt: str = "DAG visualization") -> str:
    return f'<img src="data:image/png;base64,{encoded}" alt="{alt}">'