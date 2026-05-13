from __future__ import annotations

from datetime import date

from ccflow import DateContext

from ccflow_etl import BackfillContext


def test_backfill_context_builds_business_day_contexts():
    context = BackfillContext(
        start_datetime="2024-01-05",
        end_datetime="2024-01-09",
        interval="1B",
        context=DateContext(date="2024-01-01"),
    )

    step_contexts = context.step_contexts()

    assert [step.date for step in step_contexts] == [date(2024, 1, 5), date(2024, 1, 8), date(2024, 1, 9)]
