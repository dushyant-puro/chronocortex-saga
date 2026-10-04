"""
runtime/task_graph.py

Long-Horizon Autonomy: TaskGraph layer (Phase 10).

Orchestrates multiple Task objects with cross-task dependencies.
Thin orchestration layer over existing Task / TaskManager / SpeculativeSagaManager machinery.
Graph nodes are task_ids; node work runs entirely through TaskManager.

Key principles:
  GRAPH-INV-1: A downstream task does not dispatch its first step until ALL its
               upstream dependencies have reached TaskState.COMPLETED.
  GRAPH-INV-2: Cascading replan with precise negative discrimination — if an
               upstream task replans, only downstream tasks declaring dependency
               on the SPECIFIC changed fields cascade into replanning.
  GRAPH-INV-3: Bounded replan cascades — tracks per-originating-event cascade
               history, refusing to replan the same task twice for the same event
               to prevent infinite loops.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from enum import Enum, auto
from typing import Any, Optional

from runtime.task_engine import StepState, Task, TaskManager, TaskState, TaskStep

logger = logging.getLogger("ccs.task_graph")


class TaskGraphError(Exception):
    """Base exception for task graph errors."""


class TaskGraphCycleError(TaskGraphError):
    """Raised when registering a task or ordering would create a cyclic dependency."""


class GraphState(Enum):
    """Lifecycle states for a task graph."""
    CREATED = auto()
    RUNNING = auto()
    PARTIALLY_COMPLETED = auto()
    COMPLETED = auto()
    CANCELLED = auto()
    FAILED = auto()


class TaskGraph:
    """
    Orchestrates multiple interdependent Task objects.

    Attributes:
        graph_id: Unique identifier for this graph.
        nodes: dict[str, Task] mapping task_id to Task instance.
        edges: dict[str, list[str]] mapping downstream task_id to list of upstream task_ids.
        field_dependencies: dict[str, dict[str, list[str]]] mapping
            downstream task_id -> {upstream_task_id: [consumed_field_names]}.
    """

    def __init__(self, graph_id: Optional[str] = None) -> None:
        self.graph_id = graph_id or f"graph-{uuid.uuid4().hex[:8]}"
        self.nodes: dict[str, Task] = {}
        self.edges: dict[str, list[str]] = {}
        self.field_dependencies: dict[str, dict[str, list[str]]] = {}
        self._task_manager: Optional[TaskManager] = None
        self._cascade_history: dict[str, set[str]] = {}
        self._replan_event_counter: int = 0

    @property
    def state(self) -> GraphState:
        """
        Dynamically derives the GraphState from constituent task states.
        No independently drifting graph-level state.
        """
        if not self.nodes:
            return GraphState.CREATED

        states: list[TaskState] = []
        for tid, task in self.nodes.items():
            t = task
            if self._task_manager and tid in self._task_manager._tasks:
                t = self._task_manager._tasks[tid]
            states.append(t.state)

        if any(s == TaskState.FAILED for s in states):
            return GraphState.FAILED
        if any(s == TaskState.CANCELLED for s in states):
            return GraphState.CANCELLED
        if all(s == TaskState.COMPLETED for s in states):
            return GraphState.COMPLETED
        if any(s == TaskState.COMPLETED for s in states):
            return GraphState.PARTIALLY_COMPLETED
        if any(s in (TaskState.RUNNING, TaskState.WAITING_FOR_GROUNDING, TaskState.REPLANNING) for s in states):
            return GraphState.RUNNING
        return GraphState.CREATED

    def add_task(
        self,
        task_id: str,
        task: Task,
        depends_on: Optional[dict[str, list[str]]] = None,
    ) -> None:
        """
        Add a Task to the graph.

        Args:
            task_id: Unique task identifier.
            task: Task instance.
            depends_on: dict mapping upstream task_id -> list of field names
                this task consumes from that upstream task's results.

        Raises:
            TaskGraphCycleError: If adding this task introduces a dependency cycle.
        """
        deps_dict = depends_on or {}
        upstream_ids = list(deps_dict.keys())

        # Construct candidate edges for cycle detection
        candidate_edges: dict[str, list[str]] = {tid: list(deps) for tid, deps in self.edges.items()}
        candidate_edges[task_id] = upstream_ids

        # 3-color DFS cycle detection (adapted from ToolRegistry)
        # 0 = unvisited (WHITE), 1 = visiting (GRAY), 2 = visited (BLACK)
        visit_state: dict[str, int] = {}

        def dfs(node: str) -> None:
            visit_state[node] = 1
            for dep in candidate_edges.get(node, []):
                dep_state = visit_state.get(dep, 0)
                if dep_state == 1:
                    raise TaskGraphCycleError(
                        f"Cyclic dependency detected: task '{node}' depends on '{dep}', "
                        f"which is currently active in the dependency chain."
                    )
                if dep_state == 0:
                    dfs(dep)
            visit_state[node] = 2

        for node in candidate_edges:
            if visit_state.get(node, 0) == 0:
                dfs(node)

        # Commit graph changes once cycle detection passes
        self.nodes[task_id] = task
        self.edges[task_id] = upstream_ids
        self.field_dependencies[task_id] = {}
        for up_id, fields in deps_dict.items():
            if isinstance(fields, dict):
                self.field_dependencies[task_id][up_id] = dict(fields)
            elif isinstance(fields, (list, tuple, set)):
                self.field_dependencies[task_id][up_id] = {f: f for f in fields}
            else:
                self.field_dependencies[task_id][up_id] = {str(fields): str(fields)}

        logger.info(
            "Graph %s added task %s (depends on: %s)",
            self.graph_id, task_id, self.field_dependencies[task_id],
        )

    def dispatch_order(self) -> list[str]:
        """
        Returns registered task IDs in dependency-respecting topological order
        (prerequisites before dependents). Deterministic tie-breaking using
        alphabetical sorting for equal-priority nodes (adapted from ToolRegistry).

        Raises:
            TaskGraphCycleError: If an unresolved cycle exists.
        """
        if not self.nodes:
            return []

        # in_degree: number of unsatisfied upstream dependencies
        in_degree: dict[str, int] = {tid: 0 for tid in self.nodes}
        dependents: dict[str, list[str]] = {tid: [] for tid in self.nodes}

        for tid, upstream_list in self.edges.items():
            for up in upstream_list:
                if up in self.nodes:
                    in_degree[tid] += 1
                    dependents[up].append(tid)

        ready = sorted([tid for tid, deg in in_degree.items() if deg == 0])
        order: list[str] = []

        while ready:
            curr = ready.pop(0)
            order.append(curr)
            for nxt in dependents[curr]:
                in_degree[nxt] -= 1
                if in_degree[nxt] == 0:
                    ready.append(nxt)
            ready.sort()

        if len(order) < len(self.nodes):
            raise TaskGraphCycleError("Task graph contains an unresolved cycle.")

        return order

    async def run(self, task_manager: TaskManager) -> GraphState:
        """
        Dispatches tasks via task_manager in dependency order.
        GRAPH-INV-1: A downstream task does not dispatch its first step until
        ALL its upstream dependencies have reached TaskState.COMPLETED.
        """
        self._task_manager = task_manager
        # Wire replan notification callback
        task_manager.on_task_replanned = self.on_upstream_replanned

        order = self.dispatch_order()
        for task_id in order:
            task = self.nodes[task_id]
            # GRAPH-INV-1 assertion: ensure all upstream dependencies completed
            for up_id in self.edges.get(task_id, []):
                up_task = task_manager._tasks.get(up_id) or self.nodes.get(up_id)
                if up_task is None or up_task.state != TaskState.COMPLETED:
                    logger.error(
                        "GRAPH-INV-1 violation: task %s cannot dispatch; upstream %s is in state %s",
                        task_id, up_id, up_task.state if up_task else None,
                    )
                    return self.state

            # Dispatch task
            await task_manager.run_task(task_id)
            if task.state == TaskState.FAILED:
                logger.error("Task %s failed; stopping graph execution", task_id)
                break

        return self.state

    async def on_upstream_replanned(
        self,
        upstream_task_id: str,
        changed_fields: dict[str, Any],
        event_id: Optional[str] = None,
    ) -> list[str]:
        """
        Core cascade mechanism (wired to TaskManager.on_task_replanned).

        Enforces EXACT-MATCH DISCIPLINE:
        A downstream step only replans if its own recorded dispatched_args
        genuinely contains the specific upstream value that changed, under
        whatever argument key that step actually used it under.
        """
        if self._task_manager is None:
            logger.warning("on_upstream_replanned called without TaskManager")
            return []

        if event_id is None:
            event_id = changed_fields.get("_event_id")
        if event_id is None:
            self._replan_event_counter += 1
            event_id = f"replan-{upstream_task_id}-{self._replan_event_counter}"

        if event_id not in self._cascade_history:
            self._cascade_history[event_id] = {upstream_task_id}

        cascaded_tasks: list[str] = []

        for downstream_id, upstream_mapping in self.field_dependencies.items():
            if upstream_task_id not in upstream_mapping:
                continue

            field_map = upstream_mapping[upstream_task_id]  # {upstream_field: downstream_arg_key}
            evicted_vals = changed_fields.get("_evicted_values", {})

            # Check which declared upstream fields changed
            matched_pairs: list[tuple[str, str, Any]] = []
            for up_field, down_arg in field_map.items():
                if up_field in changed_fields or up_field in evicted_vals:
                    evicted_v = evicted_vals.get(up_field)
                    matched_pairs.append((up_field, down_arg, evicted_v))

            # Negative-case discrimination: do nothing if changed fields are not consumed
            if not matched_pairs:
                logger.debug(
                    "Task %s depends on %s, but not for changed fields %s (declared: %s); no cascade",
                    downstream_id, upstream_task_id, list(changed_fields.keys()), list(field_map.keys()),
                )
                continue

            # GRAPH-INV-3: Replan-storm bound
            if downstream_id in self._cascade_history[event_id]:
                logger.warning(
                    "Replan-storm bound: task %s already replanned for event %s; suppressing loop",
                    downstream_id, event_id,
                )
                continue

            task = self._task_manager._tasks.get(downstream_id) or self.nodes.get(downstream_id)
            if task is None:
                continue

            # Exact-match check across committed steps:
            # Step must have recorded dispatched_args containing down_arg matching evicted_v
            rewind_to: Optional[int] = None
            evicted_down_arg: str = ""
            evicted_down_val: str = ""

            for i in range(len(task.steps)):
                if task.step_states[i] != StepState.COMMITTED:
                    continue
                dispatched = task.step_dispatched_args[i]
                if not dispatched:
                    continue

                for up_field, down_arg, evicted_v in matched_pairs:
                    if down_arg in dispatched:
                        actual_val = dispatched[down_arg]
                        # Exact match check:
                        if evicted_v is not None:
                            is_match = (actual_val == evicted_v)
                        else:
                            new_val = changed_fields.get(up_field)
                            is_match = (actual_val != new_val)

                        if is_match:
                            if rewind_to is None or i < rewind_to:
                                rewind_to = i
                                evicted_down_arg = down_arg
                                evicted_down_val = str(actual_val)

            if rewind_to is not None:
                self._cascade_history[event_id].add(downstream_id)
                logger.info(
                    "Cascading replan to task %s (rewind to step %d) due to upstream %s change: %s=%r",
                    downstream_id, rewind_to, upstream_task_id, evicted_down_arg, evicted_down_val,
                )
                task.state = TaskState.REPLANNING
                task._replan_event_id = event_id
                if not hasattr(task, "_replanning_fields"):
                    task._replanning_fields = set()
                task._replanning_fields.add(evicted_down_arg)

                await self._task_manager._replan_from_step(
                    task,
                    rewind_to,
                    field_name=evicted_down_arg,
                    evicted_value=evicted_down_val,
                )
                cascaded_tasks.append(downstream_id)

        return cascaded_tasks

    def checkpoint(self) -> dict[str, Any]:
        """
        Serialize graph structure without duplicating individual task state.
        Each task's state is persisted separately via TaskManager.checkpoint.
        """
        if self._task_manager is not None:
            for task in self.nodes.values():
                t = self._task_manager._tasks.get(task.task_id, task)
                self._task_manager._save_checkpoint(t)

        return {
            "graph_id": self.graph_id,
            "node_ids": list(self.nodes.keys()),
            "edges": {tid: list(deps) for tid, deps in self.edges.items()},
            "field_dependencies": {
                tid: {up_id: dict(fields) for up_id, fields in deps.items()}
                for tid, deps in self.field_dependencies.items()
            },
            "state": self.state.name,
        }

    @classmethod
    def restore(
        cls,
        graph_checkpoint: dict[str, Any],
        task_manager: TaskManager,
        step_definitions: Optional[dict[str, list[TaskStep]]] = None,
    ) -> "TaskGraph":
        """
        Restore TaskGraph from a graph checkpoint dict.
        Reconstructs nodes via task_manager.restore_task.
        """
        graph = cls(graph_id=graph_checkpoint["graph_id"])
        graph._task_manager = task_manager

        step_defs = step_definitions or {}
        for task_id in graph_checkpoint["node_ids"]:
            steps = step_defs.get(task_id) or []
            task = task_manager.restore_task(task_id, steps=steps)
            if task is None:
                task = task_manager.create_task(task_id, steps=steps)
                task.task_id = task_id
                task_manager._tasks[task_id] = task
            graph.nodes[task_id] = task

        graph.edges = {tid: list(deps) for tid, deps in graph_checkpoint["edges"].items()}
        graph.field_dependencies = {}
        for tid, deps in graph_checkpoint.get("field_dependencies", {}).items():
            graph.field_dependencies[tid] = {}
            for up_id, fields in deps.items():
                if isinstance(fields, dict):
                    graph.field_dependencies[tid][up_id] = dict(fields)
                elif isinstance(fields, (list, tuple, set)):
                    graph.field_dependencies[tid][up_id] = {f: f for f in fields}
                else:
                    graph.field_dependencies[tid][up_id] = {str(fields): str(fields)}

        task_manager.on_task_replanned = graph.on_upstream_replanned
        return graph
