"""Run the #445 lifecycle prototype through two pause/resume paths and dump telemetry.

same-worker: one worker pauses and resumes the workflow from its cache.
worker-restart: the first worker shuts down while the workflow is paused, and a
second worker rebuilds it from history and resumes it.
"""

import asyncio
import json
import sys
import uuid
from pathlib import Path

import opentelemetry._logs
import opentelemetry.trace
from opentelemetry.sdk._logs import LoggerProvider
from opentelemetry.sdk._logs.export import (
    InMemoryLogRecordExporter,
    SimpleLogRecordProcessor,
)
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from temporalio.api.enums.v1 import EventType
from temporalio.client import Client
from temporalio.contrib.opentelemetry import (
    OpenTelemetryPlugin,
    ReplaySafeLoggerProvider,
    create_tracer_provider,
)
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from workflow import ExpenseApproval, submit_expense

HERE = Path(__file__).parent
spans = InMemorySpanExporter()
logs = InMemoryLogRecordExporter()

tracer_provider = create_tracer_provider()
tracer_provider.add_span_processor(SimpleSpanProcessor(spans))
opentelemetry.trace.set_tracer_provider(tracer_provider)
log_provider = LoggerProvider()
log_provider.add_log_record_processor(SimpleLogRecordProcessor(logs))
opentelemetry._logs.set_logger_provider(ReplaySafeLoggerProvider(log_provider))
tracer = opentelemetry.trace.get_tracer("expense-app")


def worker(client: Client, task_queue: str) -> Worker:
    return Worker(
        client,
        task_queue=task_queue,
        workflows=[ExpenseApproval],
        activities=[submit_expense],
    )


async def wait_until_paused(handle) -> None:
    for _ in range(100):
        try:
            if await handle.query(ExpenseApproval.is_waiting):
                return
        except Exception:
            pass
        await asyncio.sleep(0.1)
    raise RuntimeError("workflow never paused")


async def scenario(client: Client, name: str, restart: bool) -> dict:
    spans.clear()
    logs.clear()
    task_queue = f"tq-{name}-{uuid.uuid4().hex[:6]}"
    workflow_id = f"expense-{name}"
    first = worker(client, task_queue)
    await first.__aenter__()
    with tracer.start_as_current_span("request expense"):
        handle = await client.start_workflow(
            ExpenseApproval.run, 250, id=workflow_id, task_queue=task_queue
        )
    await wait_until_paused(handle)
    if restart:
        await first.__aexit__(None, None, None)
        second = worker(client, task_queue)
        await second.__aenter__()
    with tracer.start_as_current_span("approve expense"):
        await handle.signal(ExpenseApproval.decide, True)
    result = await handle.result()
    await (second if restart else first).__aexit__(None, None, None)
    history = [e.event_type for e in (await handle.fetch_history()).events]
    return {
        "scenario": name,
        "workflow_id": workflow_id,
        "run_id": handle.result_run_id,
        "result": result,
        "history": [f"{i + 1}:{EventType.Name(t)}" for i, t in enumerate(history)],
        "spans": [
            {
                "name": s.name,
                "trace_id": f"{s.context.trace_id:032x}",
                "span_id": f"{s.context.span_id:016x}",
                "parent_span_id": f"{s.parent.span_id:016x}" if s.parent else None,
            }
            for s in spans.get_finished_spans()
        ],
        "logs": [
            {
                "event_name": r.log_record.event_name,
                "trace_id": f"{r.log_record.trace_id:032x}"
                if r.log_record.trace_id
                else None,
                "span_id": f"{r.log_record.span_id:016x}"
                if r.log_record.span_id
                else None,
                "attributes": dict(r.log_record.attributes or {}),
            }
            for r in logs.get_finished_logs()
        ],
    }


async def main() -> None:
    async with await WorkflowEnvironment.start_local() as env:
        client = await Client.connect(
            env.client.service_client.config.target_host,
            plugins=[OpenTelemetryPlugin()],
        )
        out = [
            await scenario(client, "same-worker", restart=False),
            await scenario(client, "worker-restart", restart=True),
        ]
    (HERE / "sample-output.json").write_text(json.dumps(out, indent=2))
    json.dump(out, sys.stdout, indent=2)


asyncio.run(main())
