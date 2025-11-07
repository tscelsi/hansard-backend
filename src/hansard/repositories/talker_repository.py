from abc import ABC, abstractmethod
from typing import Any, Hashable

from cachetools import LRUCache
from pymongo import AsyncMongoClient
from pymongo.operations import UpdateOne

from hansard.entities.talker import Talker, TalkerList
from utils.acache import cached


class AbstractTalkerRepository(ABC):
    @abstractmethod
    async def save_talkers(self, talkers: list[Talker]) -> None:
        pass

    @abstractmethod
    async def update_divisiveness(
        self, update_list: list[dict[Hashable, Any]]
    ) -> None:
        pass

    @abstractmethod
    async def list_talkers(self) -> TalkerList:
        pass


class MongoTalkerRepository(AbstractTalkerRepository):
    def __init__(
        self, mongo_client: AsyncMongoClient, db_name: str = "hansard"
    ):
        self.db = mongo_client[db_name]
        self.collection = self.db["talkers"]

    async def save_talkers(self, talkers: list[Talker]) -> None:
        for talker in talkers:
            if talker.id is None:
                continue
            await self.collection.update_one(
                {"id": talker.id},
                {"$set": talker.model_dump()},
                upsert=True,
            )

    async def update_divisiveness(
        self, update_list: list[dict[Hashable, Any]]
    ) -> None:
        await self.collection.bulk_write(
            [
                UpdateOne(
                    {"id": item["talker_id"]},
                    {
                        "$set": {
                            "divisiveness": item["divisiveness"],
                            "bill_divisiveness": item["bill_divisiveness"],
                        }
                    },
                )
                for item in update_list
            ]
        )

    @cached(cache=LRUCache(maxsize=1))
    async def list_talkers(self) -> TalkerList:
        talkers: list[Talker] = []
        async for doc in self.collection.find():
            talker = Talker.model_validate(doc)
            talkers.append(talker)
        return TalkerList(talkers)
