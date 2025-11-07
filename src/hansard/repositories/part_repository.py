from abc import ABC, abstractmethod
from datetime import datetime

from pymongo import AsyncMongoClient, UpdateOne

from hansard.entities.speech import Part, SpeechPart


class AbstractPartRepository(ABC):
    @abstractmethod
    async def upsert(self, speech_parts: list[Part]) -> None:
        pass

    @abstractmethod
    async def list_all_speech_parts(self) -> list[SpeechPart]:
        pass

    @abstractmethod
    async def list_speech_parts_by_speeches(
        self, speech_ids: list[str]
    ) -> list[SpeechPart]:
        pass


class MongoPartRepository(AbstractPartRepository):
    def __init__(
        self, mongo_client: AsyncMongoClient, db_name: str = "hansard"
    ):
        self.db = mongo_client[db_name]
        self.collection = self.db["parts"]

    async def upsert(self, speech_parts: list[Part]) -> None:
        requests = [
            UpdateOne(
                {"id": str(part.part_id)},
                {
                    "$set": {
                        "id": str(part.part_id),
                        "date": datetime.combine(
                            part.date, datetime.min.time()
                        ),
                        **(
                            {"speech_id": part.speech_id}
                            if isinstance(part, SpeechPart)
                            else {}
                        ),
                        **part.model_dump(
                            exclude={"id", "date", "speech_id", "part_id"}
                        ),
                    }
                },
                upsert=True,
            )
            for part in speech_parts
        ]
        result = await self.collection.bulk_write(requests)
        return result.upserted_ids

    async def list_all_speech_parts(self) -> list[SpeechPart]:
        parts: list[SpeechPart] = []
        curs = self.collection.find({"type": "speech"})
        async for doc in curs:
            parts.append(SpeechPart.model_validate(doc))
        return parts

    async def list_speech_parts_by_speeches(
        self, speech_ids: list[str]
    ) -> list[SpeechPart]:
        parts: list[SpeechPart] = []
        curs = self.collection.find(
            {
                "type": "speech",
                "speech_id": {"$in": [str(sid) for sid in speech_ids]},
            },
            sort=[("speech_id", 1), ("part_seq", 1)],
        )
        async for doc in curs:
            parts.append(SpeechPart.model_validate(doc))
        return parts
