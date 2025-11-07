import logging
from typing import Any

from openai import AsyncOpenAI
from pydantic import BaseModel

from hansard.entities.speech import HouseType
from hansard.nlp.services import (
    calculate_house_divisiveness,
    run_batch_speech_summarisation,
)
from hansard.repositories.part_repository import AbstractPartRepository
from hansard.repositories.speech_stats_repository import (
    AbstractSpeechStatsRepository,
)
from hansard.repositories.talker_repository import AbstractTalkerRepository
from utils.background_tasks import BackgroundTasks
from utils.events.local import LocalPublisher, LocalSubscriber
from utils.logger import CustomLoggingAdapter


class ExpectedEventForm(BaseModel):
    topic: str
    house: HouseType
    speech_ids: list[str]


class SummariserSubscriber(LocalSubscriber):
    def __init__(
        self,
        talker_repository: AbstractTalkerRepository,
        speech_part_repo: AbstractPartRepository,
        speech_stats_repo: AbstractSpeechStatsRepository,
        publisher: LocalPublisher,
        client: AsyncOpenAI,
        background_tasks: BackgroundTasks | None = None,
    ):
        self._logger = CustomLoggingAdapter(
            logging.getLogger(__name__), {"ctx": "Summariser"}
        )
        self.talker_repository = talker_repository
        self.speech_part_repo = speech_part_repo
        self.speech_stats_repo = speech_stats_repo
        self.publisher = publisher
        self.background_tasks = background_tasks
        self.client = client
        super().__init__(publisher)

    async def handle(self, event: dict[str, Any]) -> None:
        validated_event = ExpectedEventForm.model_validate(event)
        if validated_event.topic == "parser.completed":
            if validated_event.house != HouseType.HOR:
                self._logger.info(
                    f"skipping event for house: {validated_event.house}, not HOR"  # noqa
                )
                return
            self._logger.info(
                f"received {len(validated_event.speech_ids)} speeches to summarise"  # noqa
            )
            await run_batch_speech_summarisation(
                validated_event.speech_ids,
                self.speech_part_repo,
                self.speech_stats_repo,
                self.talker_repository,
                self.publisher,
                self.client,
                self.background_tasks,
            )


class DivisivenessSubscriber(LocalSubscriber):
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

    async def handle(self, event: dict[str, Any]) -> None:
        validated_event = ExpectedEventForm.model_validate(event)
        if validated_event.topic == "parser.completed":
            self._logger.info(f"received event: {validated_event.topic}")
            # refresh divisiveness scores
            await calculate_house_divisiveness(
                validated_event.house,
                self.speech_part_repo,
                self.talker_repository,
            )
