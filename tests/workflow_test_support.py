"""Explicit contracts for SIMULATED legacy fixtures testing other boundaries.

Only construction/restore metadata is supplied here. Production run, policy,
workflow checks, saga dispatch and journal writes remain unchanged.
"""
from dataclasses import replace
from runtime.workflow_contract import WorkflowVersion, StepVersions


TEST_WORKFLOW = WorkflowVersion("SIMULATED:boundary-fixture", "v1")


def versioned_test_steps(steps, registry):
    return [replace(step, versions=step.versions or StepVersions(
        step_id=f"step-{idx}", arguments="SIMULATED:arguments:v1",
        executor="SIMULATED:executor:v1", provider="SIMULATED:provider:v1",
        provider_scope="SIMULATED:local-test",
        compensation="SIMULATED:undo:v1" if step.compensate is not None else None,
        reconciliation="SIMULATED:status:v1" if step.status_check is not None else None,
        validator="SIMULATED:validator:v1" if registry.get(step.tool_name) and registry.get(step.tool_name).validate_args else None,
    )) for idx, step in enumerate(steps)]


class VersionedTestTasks:
    def create_task(self, name, steps, **kwargs):
        if self._saga.journal is not None:
            steps = versioned_test_steps(steps, self._saga.registry)
            kwargs.setdefault("workflow", TEST_WORKFLOW)
        return super().create_task(name, steps, **kwargs)

    def restore_task(self, task_id, steps=None, **kwargs):
        if self._saga.journal is not None and steps is not None:
            steps = versioned_test_steps(steps, self._saga.registry)
            kwargs.setdefault("workflow", TEST_WORKFLOW)
        return super().restore_task(task_id, steps=steps, **kwargs)
