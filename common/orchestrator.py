import json
import uuid
import logging
import redis as redis_lib

logger = logging.getLogger(__name__)


class StepFailed(Exception):
    """Raise from a step to abort the workflow and trigger compensation."""


def suspend():
    """Call from inside a step after publishing an async message.
    The engine pauses here; call orchestrator.resume() when the response arrives."""
    raise _AsyncSuspend()


class Workflow:
    """Describes a durable sequence of steps.

    steps        list of callables  step(ctx) → dict | None
                 Raise StepFailed to abort. Call suspend() to wait for an async response.

    compensation list of callables  comp(ctx) → None  (index-aligned with steps)
                 comp[i] undoes step[i]; run in reverse from the step before the one that failed.

    on_complete  optional callable(ctx) — invoked once all steps succeed.
    on_failed    optional callable(ctx) — invoked once all compensations complete.
    """

    def __init__(
        self, name, steps, compensation=None, on_complete=None, on_failed=None
    ):
        self.name = name
        self.steps = steps
        self.compensation = compensation or []
        self.on_complete = on_complete
        self.on_failed = on_failed


class Orchestrator:
    _PREFIX = "wf:"
    _TERMINAL = {"completed", "failed"}

    RUNNING = "running"
    WAITING = "waiting"
    COMPENSATING = "compensating"
    WAITING_COMP = "waiting_comp"
    COMPLETED = "completed"
    FAILED = "failed"

    def __init__(self, redis_pool):
        self._pool = redis_pool

    def _r(self):
        return redis_lib.Redis(connection_pool=self._pool)

    def start(self, workflow: Workflow, context: dict) -> tuple:
        """Create and run a new workflow instance.

        Returns (wf_id, status, error) so synchronous callers (TPC) can skip
        a follow-up get_status() round-trip. Async callers (SAGA) may ignore
        the return value.
        """
        wf_id = str(uuid.uuid4())
        context = dict(context)
        context["wf_id"] = wf_id
        r = self._r()
        r.hset(
            f"{self._PREFIX}{wf_id}",
            mapping={
                "name": workflow.name,
                "step": "0",
                "status": self.RUNNING,
                "context": json.dumps(context),
            },
        )
        status, error = self._execute_forward(workflow, wf_id, r, 0, context)
        return wf_id, status, error

    def resume(self, workflow: Workflow, wf_id: str, result: dict = None):
        """Signal that an async forward step completed successfully.

        Advances the step pointer, merges result into context, and continues.
        Silently ignored if the workflow is not in 'waiting' state — guards
        against duplicate responses arriving after recovery re-published.
        """
        r = self._r()
        raw = r.hgetall(f"{self._PREFIX}{wf_id}")
        if not raw or raw.get("status") != self.WAITING:
            return
        ctx = json.loads(raw["context"])
        next_step = int(raw["step"]) + 1
        mapping: dict = {"step": str(next_step), "status": self.RUNNING}
        if result:
            ctx.update(result)
            mapping["context"] = json.dumps(ctx)
        r.hset(f"{self._PREFIX}{wf_id}", mapping=mapping)
        self._execute_forward(workflow, wf_id, r, next_step, ctx)

    def fail(self, workflow: Workflow, wf_id: str, error: str = ""):
        """Signal that an async forward step failed.

        Stores the error and begins compensation in reverse from the step
        before the one that failed.
        Silently ignored if the workflow is not in 'waiting' or 'running' state.
        """
        r = self._r()
        raw = r.hgetall(f"{self._PREFIX}{wf_id}")
        if not raw or raw.get("status") not in (self.WAITING, self.RUNNING):
            return
        step = int(raw.get("step", "0"))
        ctx = json.loads(raw.get("context", "{}"))
        ctx["_error"] = error
        r.hset(
            f"{self._PREFIX}{wf_id}",
            mapping={
                "status": self.COMPENSATING,
                "error": error,
                "context": json.dumps(ctx),
            },
        )
        self._execute_compensation(workflow, wf_id, r, ctx, failed_at=step)

    def resume_comp(self, workflow: Workflow, wf_id: str):
        """Signal that an async compensation step completed.

        Continues the compensation chain toward index 0, then marks the
        instance 'failed' and calls workflow.on_failed.
        Silently ignored if the workflow is not in 'waiting_comp' state.
        """
        r = self._r()
        raw = r.hgetall(f"{self._PREFIX}{wf_id}")
        if not raw or raw.get("status") != self.WAITING_COMP:
            return
        comp_step = int(raw.get("comp_step", "0"))
        ctx = json.loads(raw.get("context", "{}"))
        r.hset(f"{self._PREFIX}{wf_id}", "status", self.COMPENSATING)
        self._execute_compensation(workflow, wf_id, r, ctx, failed_at=comp_step)

    def get_status(self, wf_id: str) -> tuple:
        """Return (status, error, context) for a workflow instance."""
        r = self._r()
        raw = r.hgetall(f"{self._PREFIX}{wf_id}")
        if not raw:
            return "unknown", "", {}
        return (
            raw.get("status", "unknown"),
            raw.get("error", ""),
            json.loads(raw.get("context", "{}")),
        )

    def recover(self, workflow: Workflow):
        """Scan for non-terminal instances of this workflow and resume them.

        Safe to call at every startup. For each instance found:
          - sync steps are re-called (must be idempotent)
          - async steps re-publish their messages (participants use idempotency keys)
          - compensation steps re-run in reverse from where they left off
        """
        r = self._r()
        cursor = 0
        recovered = 0
        while True:
            cursor, keys = r.scan(cursor, match=f"{self._PREFIX}*", count=100)
            for key in keys:
                raw = r.hgetall(key)
                if not raw or raw.get("name") != workflow.name:
                    continue
                if raw.get("status") in self._TERMINAL:
                    continue
                wf_id = key[len(self._PREFIX) :]
                logger.info(
                    "RECOVERY: wf=%s status=%s step=%s",
                    wf_id,
                    raw.get("status"),
                    raw.get("step"),
                )
                recovered += 1
                self._recover_one(workflow, wf_id, raw, r)
            if cursor == 0:
                break
        if recovered == 0:
            logger.info("RECOVERY: no incomplete '%s' workflows found", workflow.name)
        else:
            logger.info(
                "RECOVERY: resumed %d '%s' workflow(s)", recovered, workflow.name
            )

    def _recover_one(self, workflow, wf_id, raw, r):
        status = raw.get("status", "")
        ctx = json.loads(raw.get("context", "{}"))
        comp_step = int(raw.get("comp_step", "-1"))

        if status in (self.RUNNING, self.WAITING):
            r.hset(f"{self._PREFIX}{wf_id}", "status", self.RUNNING)
            self._execute_forward(workflow, wf_id, r, int(raw.get("step", "0")), ctx)

        elif status in (self.COMPENSATING, self.WAITING_COMP):
            r.hset(f"{self._PREFIX}{wf_id}", "status", self.COMPENSATING)
            if comp_step >= 0:
                self._execute_compensation(
                    workflow, wf_id, r, ctx, failed_at=comp_step + 1
                )
            else:
                step = int(raw.get("step", "0"))
                self._execute_compensation(workflow, wf_id, r, ctx, failed_at=step)

    def _execute_forward(self, workflow, wf_id, r, step_idx, ctx) -> tuple:
        for idx in range(step_idx, len(workflow.steps)):
            r.hset(
                f"{self._PREFIX}{wf_id}",
                mapping={"step": str(idx), "status": self.WAITING},
            )
            try:
                result = workflow.steps[idx](ctx)
                r.hset(f"{self._PREFIX}{wf_id}", "status", self.RUNNING)
                if result:
                    ctx.update(result)
                    r.hset(f"{self._PREFIX}{wf_id}", "context", json.dumps(ctx))

            except _AsyncSuspend:
                return self.WAITING, ""

            except StepFailed as exc:
                ctx["_error"] = str(exc)
                r.hset(
                    f"{self._PREFIX}{wf_id}",
                    mapping={
                        "status": self.COMPENSATING,
                        "error": str(exc),
                        "context": json.dumps(ctx),
                    },
                )
                return self._execute_compensation(workflow, wf_id, r, ctx, failed_at=idx)

        r.hset(f"{self._PREFIX}{wf_id}", "status", self.COMPLETED)
        if workflow.on_complete:
            try:
                workflow.on_complete(ctx)
            except Exception as exc:
                logger.error("on_complete failed for wf %s: %s", wf_id, exc)
        return self.COMPLETED, ""

    def _execute_compensation(self, workflow, wf_id, r, ctx, failed_at) -> tuple:
        """Run comp[failed_at-1] down to comp[0].

        failed_at is the index of the step that failed; only steps 0..failed_at-1
        completed and need undoing.
        """
        for idx in range(failed_at - 1, -1, -1):
            if idx >= len(workflow.compensation):
                continue
            r.hset(
                f"{self._PREFIX}{wf_id}",
                mapping={"comp_step": str(idx), "status": self.WAITING_COMP},
            )
            try:
                workflow.compensation[idx](ctx)
                r.hset(f"{self._PREFIX}{wf_id}", "status", self.COMPENSATING)

            except _AsyncSuspend:
                return self.WAITING_COMP, ctx.get("_error", "")

            except Exception as exc:
                logger.error(
                    "Compensation step %d failed for wf %s: %s", idx, wf_id, exc
                )
                r.hset(f"{self._PREFIX}{wf_id}", "status", self.COMPENSATING)

        r.hset(f"{self._PREFIX}{wf_id}", "status", self.FAILED)
        if workflow.on_failed:
            try:
                workflow.on_failed(ctx)
            except Exception as exc:
                logger.error("on_failed callback failed for wf %s: %s", wf_id, exc)
        return self.FAILED, ctx.get("_error", "")


class _AsyncSuspend(BaseException):
    """Control-flow signal raised by suspend(). Inherits from BaseException
    so it is not caught by bare 'except Exception' blocks."""
