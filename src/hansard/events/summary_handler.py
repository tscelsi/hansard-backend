"""A subscriber to parser.completed events, that handles adding speeches to
the summary batch manager for LLM summarisation.
"""

import logging
from typing import Any

from hansard.entities.speech import HouseType
from hansard.nlp.services import SummaryBatchManager
from utils.events.local import LocalPublisher, LocalSubscriber
from utils.logger import CustomLoggingAdapter

from .eventlist import ParserCompletedEvent


class SummariserSubscriber(LocalSubscriber):
    _supported_topics: list[str] = ["parser.completed"]

    def __init__(
        self, publisher: LocalPublisher, batch_manager: SummaryBatchManager
    ):
        self._logger = CustomLoggingAdapter(
            logging.getLogger(__name__), {"ctx": "Summariser"}
        )
        self.batch_manager = batch_manager
        super().__init__(publisher)

    @property
    def supported_topics(self) -> list[str]:
        return self._supported_topics

    async def handle(self, event: dict[str, Any]) -> None:
        validated_event = ParserCompletedEvent.model_validate(event)
        if validated_event.topic == "parser.completed":
            if validated_event.house != HouseType.HOR:
                self._logger.info(
                    f"skipping event for house: {validated_event.house}, not HOR"  # noqa
                )
                return
            self._logger.info(
                f"received {len(validated_event.speech_ids)} speeches to summarise"  # noqa
            )
            self.batch_manager.add(validated_event.speech_ids)
