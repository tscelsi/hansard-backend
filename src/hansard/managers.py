from typing import Any

from pydantic_settings import BaseSettings
from pymongo import AsyncMongoClient

from hansard.events.divisiveness_handler import DivisivenessSubscriber
from hansard.events.summary_handler import SummariserSubscriber
from hansard.events.view_refresh_handler import ViewUpdateSubscriber
from hansard.nlp.services import SummaryBatchManager
from hansard.nlp.summary import CLIENT
from hansard.repositories.part_repository import MongoPartRepository
from hansard.repositories.speech_stats_repository import (
    MongoSpeechStatsRepository,
)
from hansard.repositories.talker_repository import MongoTalkerRepository
from utils.events.local import LocalPublisher, LocalSubscriber


class DbEnv(BaseSettings):
    MONGODB_DB: str = "mongodb://localhost:27017"
    MONGODB_DB_NAME: str = "tmp"


class DbManager:
    _instance: "DbManager | None" = None

    @staticmethod
    def get() -> "DbManager":
        if DbManager._instance is None:
            DbManager._instance = DbManager()
        return DbManager._instance

    def __init__(self):
        self.env = DbEnv()
        self.setup()

    def setup(self):
        self.client = AsyncMongoClient[Any](self.env.MONGODB_DB)
        self.db = self.client[self.env.MONGODB_DB_NAME]
        self.speech_part_repo = MongoPartRepository(
            self.client, self.env.MONGODB_DB_NAME
        )
        self.speech_stats_repo = MongoSpeechStatsRepository(
            self.client, self.env.MONGODB_DB_NAME
        )
        self.talker_repo = MongoTalkerRepository(
            self.client, self.env.MONGODB_DB_NAME
        )


class EventManager:
    _instance: "EventManager | None" = None
    _subscribers: list[LocalSubscriber] = []

    @staticmethod
    def get() -> "EventManager":
        if EventManager._instance is None:
            EventManager._instance = EventManager()
        return EventManager._instance

    def __init__(self):
        self.publisher = LocalPublisher()

    async def setup(self):
        db_manager = DbManager.get()
        # set up divisiveness subscriber
        # divisiveness_event_listener = DivisivenessSubscriber(
        #     talker_repository=db_manager.talker_repo,
        #     speech_part_repo=db_manager.speech_part_repo,
        #     publisher=self.publisher,
        # )
        # await divisiveness_event_listener.subscribe(
        #     divisiveness_event_listener.supported_topics
        # )
        # self._subscribers.append(divisiveness_event_listener)
        # set up summariser subscriber
        # batch_manager = SummaryBatchManager(
        #     db_manager.speech_part_repo,
        #     db_manager.speech_stats_repo,
        #     db_manager.talker_repo,
        #     self.publisher,
        #     CLIENT,
        # )
        # summariser_event_listener = SummariserSubscriber(
        #     self.publisher, batch_manager
        # )
        # await summariser_event_listener.subscribe(
        #     summariser_event_listener.supported_topics
        # )
        # self._subscribers.append(summariser_event_listener)

        view_refresh_event_listener = ViewUpdateSubscriber(
            db_manager.speech_part_repo, self.publisher, db_manager.db
        )
        await view_refresh_event_listener.subscribe(
            view_refresh_event_listener.supported_topics
        )
        self._subscribers.append(view_refresh_event_listener)
