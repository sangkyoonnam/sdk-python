# Agent lifecycle events on a Temporal workflow pause

Prototype for [open-telemetry/semantic-conventions-genai#445](https://github.com/open-telemetry/semantic-conventions-genai/pull/445). It is not a Temporal feature.

`temporalio/contrib/opentelemetry/_agent_lifecycle.py` adds `wait_for_input()`, which wraps `workflow.wait_condition()` and emits `gen_ai.agent.paused`, `gen_ai.agent.checkpointed` and `gen_ai.agent.resumed` as OpenTelemetry log records through `ReplaySafeLoggerProvider`. It emits nothing when the condition already holds. Workflow code can't observe when the task that paused is persisted, so `checkpointed` is emitted when the pause starts.

| Attribute | Value |
|---|---|
| `gen_ai.agent.execution.id` | workflow run ID |
| `gen_ai.agent.pause.id` | `workflow.uuid4()`, deterministic across replay |
| `gen_ai.agent.pause.reason` | passed by the workflow, since Temporal doesn't distinguish a human wait from any other condition |
| `gen_ai.agent.checkpoint.id` | `<run ID>:<WorkflowTaskStarted event ID>` of the task that reaches the pause |
| `gen_ai.agent.resumed_from.*` | `checkpoint` and the checkpoint ID when a later workflow task resumes the workflow |

`run.py` runs `workflow.py`, an approval workflow, two ways against a local dev server and writes `sample-output.json`:

- `same-worker`: one worker pauses and resumes the workflow from its cache.
- `worker-restart`: the first worker shuts down during the pause, and a second worker rebuilds the workflow from history and resumes it.

Based on temporalio 1.34.0. Use opentelemetry-api and opentelemetry-sdk 1.44 or earlier; with 1.45, `ReplaySafeLoggerProvider` fails (temporalio/sdk-python#2030).

```
pip install "temporalio[opentelemetry]==1.34.0" "opentelemetry-api==1.44.0" "opentelemetry-sdk==1.44.0"
# then put this branch's temporalio/contrib/opentelemetry/_agent_lifecycle.py into the installed package
python run.py
```
