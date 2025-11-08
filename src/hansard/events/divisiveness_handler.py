"""A subscriber to parser.completed events, that handles refreshing the
divisiveness ratings of speakers once the new speeches have been parsed and
stored.
"""

import logging
from typing import Any

from hansard.nlp.services import calculate_house_divisiveness
from hansard.repositories.part_repository import AbstractPartRepository
from hansard.repositories.talker_repository import AbstractTalkerRepository
from utils.events.local import LocalPublisher, LocalSubscriber
from utils.logger import CustomLoggingAdapter

from .eventlist import ParserCompletedEvent


class DivisivenessSubscriber(LocalSubscriber):
    _supported_topics: list[str] = ["parser.completed"]

    def __init__(
        self,
        talker_repository: AbstractTalkerRepository,
        speech_part_repo: AbstractPartRepository,
        publisher: LocalPublisher,
    ):
        self._logger = CustomLoggingAdapter(
            logging.getLogger(__name__), {"ctx": "Divisiveness"}
        )
        self.talker_repository = talker_repository
        self.speech_part_repo = speech_part_repo
        self.publisher = publisher
        super().__init__(publisher)

    @property
    def supported_topics(self) -> list[str]:
        return self._supported_topics

    async def handle(self, event: dict[str, Any]) -> None:
        validated_event = ParserCompletedEvent.model_validate(event)
        if validated_event.topic == "parser.completed":
            self._logger.info(f"received event: {validated_event.topic}")
            # refresh divisiveness scores
            await calculate_house_divisiveness(
                validated_event.house,
                self.speech_part_repo,
                self.talker_repository,
            )
