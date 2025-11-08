"""A subscriber to parser.completed and nlp.batch_completed events, that
handles refreshing bill overview views in the database once new speeches have
been parsed, and once new summaries have been generated.
"""

import logging
from typing import Any

from pymongo.asynchronous.database import AsyncDatabase

from hansard.repositories.part_repository import AbstractPartRepository
from hansard.services import refresh_bill_overview
from utils.events.local import LocalPublisher, LocalSubscriber
from utils.logger import CustomLoggingAdapter

from .eventlist import BatchSummaryCompletedEvent, ParserCompletedEvent


class ViewUpdateSubscriber(LocalSubscriber):
    _supported_topics: list[str] = [
        "parser.completed",
        "nlp.batch_completed",
    ]

    def __init__(
        self,
        part_repo: AbstractPartRepository,
        publisher: LocalPublisher,
        database: AsyncDatabase[Any],
    ):
        self._logger = CustomLoggingAdapter(
            logging.getLogger(__name__), {"ctx": "Divisiveness"}
        )
        self.db = database
        self.part_repo = part_repo
        self.publisher = publisher
        super().__init__(publisher)

    @property
    def supported_topics(self) -> list[str]:
        return self._supported_topics

    async def handle(self, event: dict[str, Any]) -> None:
        if "parser.completed" in event.get("topic", ""):
            validated_event = ParserCompletedEvent.model_validate(event)
        elif "nlp.batch_completed" in event.get("topic", ""):
            validated_event = BatchSummaryCompletedEvent.model_validate(event)
        else:
            self._logger.info(
                f"skipping unknown event: {event.get('topic', '')}"
            )
            return
        if validated_event.topic in [
            "parser.completed",
            "nlp.batch_completed",
        ]:
            self._logger.info(f"received event: {validated_event.topic}")
            bill_ids: set[str] = set()
            curs = self.db["parts"].find(
                {
                    "speech_id": {
                        "$in": validated_event.speech_ids,
                    },
                    "part_seq": 0,
                }
            )
            async for doc in curs:
                bill_ids.update(doc.get("bill_ids") or [])
            self._logger.info(f"total bills to refresh: {len(bill_ids)}")
            for i, bill_id in enumerate(bill_ids):
                self._logger.info(
                    f"refreshing overview for bill: {bill_id} ({i+1}/{len(bill_ids)})"  # noqa
                )
                await refresh_bill_overview(bill_id, self.db)
