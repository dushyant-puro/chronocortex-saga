"""Explicit simulation credentials for legacy non-policy state-machine tests.

Synthetic manifests describe fake executors only. The real commit_write and
PolicyEngine are inherited unchanged. Security boundary tests import the real
manager directly and receive neither default grants nor synthetic manifests.
Never import this module into production runtime code.
"""
from runtime.policy_engine import AuthorizationContext
from runtime.speculative_saga import SpeculativeSagaManager as ProductionSaga
from runtime.tool_contract import ToolManifest


class SimulatedAuthorizedSaga(ProductionSaga):
    def __init__(self, *args, **kwargs):
        kwargs.setdefault("auth_context", AuthorizationContext(["fleet:write"]))
        super().__init__(*args, **kwargs)

    def stage_write(self, tool_name, args, **kwargs):
        if self.registry is None and kwargs.get("registry") is None and kwargs.get("manifest") is None:
            kwargs["manifest"] = ToolManifest(tool_name, "write")
        return super().stage_write(tool_name, args, **kwargs)
