# dag_engine/node.py

import time
from abc import ABC, abstractmethod
from typing import Any

from dag_engine.logger import node_logger


class BaseNode(ABC):
    """
    All nodes must subclass this. execute() must be pure — no side effects on shared state.
    Receives a dict of {dep_node_id: output} and returns a JSON-serialisable dict.
    """

    def __init__(self, node_id: str, config: dict | None = None):
        self.node_id = node_id
        self.config = config or {}

    @abstractmethod
    def execute(self, inputs: dict[str, Any]) -> dict[str, Any]:
        """
        inputs  : {upstream_node_id: upstream_output_dict}
        returns : dict (must be JSON-serialisable)
        """

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(id={self.node_id!r})"


# ---------------------------------------------------------------------------
# Built-in node implementations (useful for tests & demos)
# ---------------------------------------------------------------------------

class LambdaNode(BaseNode):
    """Wraps an arbitrary callable. Great for quick one-off nodes."""

    def __init__(self, node_id: str, fn, config: dict | None = None):
        super().__init__(node_id, config)
        self._fn = fn

    def execute(self, inputs: dict[str, Any]) -> dict[str, Any]:
        node_logger.debug(f"[{self.node_id}] LambdaNode executing")
        result = self._fn(inputs, self.config)
        if not isinstance(result, dict):
            raise TypeError(
                f"Node '{self.node_id}' execute() must return a dict, got {type(result)}"
            )
        return result


class EchoNode(BaseNode):
    """Returns all upstream outputs merged into one dict. Handy for fanin."""

    def execute(self, inputs: dict[str, Any]) -> dict[str, Any]:
        node_logger.debug(f"[{self.node_id}] EchoNode merging {list(inputs.keys())}")
        merged: dict[str, Any] = {}
        for upstream_id, output in inputs.items():
            for k, v in output.items():
                merged[f"{upstream_id}.{k}"] = v
        return merged


class SleepNode(BaseNode):
    """Sleeps for config['seconds'] then echoes inputs. Useful for parallelism demos."""

    def execute(self, inputs: dict[str, Any]) -> dict[str, Any]:
        seconds = self.config.get("seconds", 1)
        node_logger.debug(f"[{self.node_id}] SleepNode sleeping {seconds}s")
        time.sleep(seconds)
        return {"slept_seconds": seconds, **{k: v for d in inputs.values() for k, v in d.items()}}


class FailNode(BaseNode):
    """Always raises. Used to test retry / error isolation."""

    def execute(self, inputs: dict[str, Any]) -> dict[str, Any]:
        node_logger.error(f"[{self.node_id}] FailNode intentionally raising")
        raise RuntimeError(f"FailNode '{self.node_id}' deliberately failed")