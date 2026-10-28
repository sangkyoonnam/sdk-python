from datetime import timedelta

from temporalio import activity, workflow

with workflow.unsafe.imports_passed_through():
    from temporalio.contrib.opentelemetry._agent_lifecycle import wait_for_input


@activity.defn
async def submit_expense(amount: int) -> str:
    return f"submitted {amount}"


@workflow.defn
class ExpenseApproval:
    def __init__(self) -> None:
        self.approved: bool | None = None
        self.waiting = False

    @workflow.run
    async def run(self, amount: int) -> str:
        self.waiting = True
        await wait_for_input(lambda: self.approved is not None, reason="human_input")
        self.waiting = False
        if not self.approved:
            return "rejected"
        return await workflow.execute_activity(
            submit_expense, amount, start_to_close_timeout=timedelta(seconds=10)
        )

    @workflow.signal
    def decide(self, approved: bool) -> None:
        self.approved = approved

    @workflow.query
    def is_waiting(self) -> bool:
        return self.waiting
