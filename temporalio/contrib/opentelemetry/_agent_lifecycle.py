"""Agent lifecycle events for workflows that pause on an external condition.

.. warning::
    This module is a prototype for
    open-telemetry/semantic-conventions-genai#445. Every event and attribute
    name below is a candidate convention in that proposal, not a stable one.

A workflow pauses by awaiting a condition, usually set by a signal or an
update. Temporal persists the workflow at the end of every workflow task, so a
pause is always also a durable point: the worker may stop, and a different
worker rebuilds the workflow from history and continues after the pause.

:py:func:`wait_for_input` wraps :py:func:`temporalio.workflow.wait_condition`
and records ``gen_ai.agent.paused`` and ``gen_ai.agent.checkpointed`` when the
pause begins, and ``gen_ai.agent.resumed`` when the condition is met. Records
go through the global OpenTelemetry logger provider, which should be a
:py:class:`ReplaySafeLoggerProvider` so that rebuilding a workflow from
history does not emit the pause a second time.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import timedelta

import opentelemetry._logs

import temporalio.workflow

# Candidate event names, pending open-telemetry/semantic-conventions-genai#445.
EVENT_AGENT_PAUSED = "gen_ai.agent.paused"
EVENT_AGENT_CHECKPOINTED = "gen_ai.agent.checkpointed"
EVENT_AGENT_RESUMED = "gen_ai.agent.resumed"

# Candidate attribute names, pending the same proposal.
ATTR_AGENT_NAME = "gen_ai.agent.name"
ATTR_EXECUTION_ID = "gen_ai.agent.execution.id"
ATTR_PAUSE_ID = "gen_ai.agent.pause.id"
ATTR_PAUSE_REASON = "gen_ai.agent.pause.reason"
ATTR_CHECKPOINT_ID = "gen_ai.agent.checkpoint.id"
ATTR_RESUMED_FROM_TYPE = "gen_ai.agent.resumed_from.type"
ATTR_RESUMED_FROM_ID = "gen_ai.agent.resumed_from.id"

PAUSE_REASON_HUMAN_INPUT = "human_input"
PAUSE_REASON_EXTERNAL_SYSTEM = "external_system"
RESUMED_FROM_TYPE_CHECKPOINT = "checkpoint"
RESUMED_FROM_TYPE_PAUSE = "pause"


def _emit(event_name: str, attributes: dict[str, str], body: str) -> None:
    opentelemetry._logs.get_logger(__name__).emit(
        event_name=event_name, attributes=attributes, body=body
    )


async def wait_for_input(
    condition: Callable[[], bool],
    *,
    reason: str = PAUSE_REASON_HUMAN_INPUT,
    timeout: timedelta | float | None = None,
) -> None:
    """Wait for ``condition`` and record the pause and the resume.

    .. warning::
        This function is experimental and may change in future versions.

    Args:
        condition: Same as for :py:func:`temporalio.workflow.wait_condition`.
        reason: Value of ``gen_ai.agent.pause.reason``.
        timeout: Same as for :py:func:`temporalio.workflow.wait_condition`.
    """
    if condition():
        # Nothing suspends, so there is no pause or durable point to record.
        return
    info = temporalio.workflow.info()
    execution_id = info.run_id
    # workflow.uuid4() is deterministic, so a worker rebuilding this workflow
    # from history derives the same pause id the first worker recorded.
    pause_id = str(temporalio.workflow.uuid4())
    # The workflow task that reaches this pause ends by persisting the
    # workflow, which is the durable point a later worker continues from. Its
    # WorkflowTaskStarted event id names that point within the run. Workflow
    # code can't observe that commit, so checkpointed is recorded before it.
    history_length = info.get_current_history_length()
    checkpoint_id = f"{info.run_id}:{history_length}"

    common = {ATTR_EXECUTION_ID: execution_id, ATTR_AGENT_NAME: info.workflow_type}
    _emit(
        EVENT_AGENT_PAUSED,
        {**common, ATTR_PAUSE_ID: pause_id, ATTR_PAUSE_REASON: reason},
        "Agent execution paused",
    )
    _emit(
        EVENT_AGENT_CHECKPOINTED,
        {**common, ATTR_CHECKPOINT_ID: checkpoint_id, ATTR_PAUSE_ID: pause_id},
        "Agent execution checkpointed",
    )

    await temporalio.workflow.wait_condition(condition, timeout=timeout)

    resumed = dict(common)
    # Another coroutine can satisfy the condition within the same activation,
    # and then nothing ended. Only a later workflow task, on this worker or on
    # one that rebuilt the workflow from history, is a new active segment that
    # continues from the persisted point.
    if temporalio.workflow.info().get_current_history_length() > history_length:
        resumed[ATTR_RESUMED_FROM_TYPE] = RESUMED_FROM_TYPE_CHECKPOINT
        resumed[ATTR_RESUMED_FROM_ID] = checkpoint_id
    _emit(EVENT_AGENT_RESUMED, resumed, "Agent execution resumed")
