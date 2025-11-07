import abc
from uuid import UUID

from pydantic import BaseModel, Field
from pymongo import AsyncMongoClient

from hansard.nlp.enums import SpeechTone


class SpeechStats(BaseModel):
    speech_id: str = Field(description="Unique identifier for the speech")
    summary: str | None = Field(
        default=None, description="LLM generated summary of the speech"
    )
    stance: float | None = Field(
        default=None, ge=-1.0, le=1.0, description="Stance value of the speech"
    )
    tone: list[SpeechTone] = Field(
        default=[], description="Detected tones in the speech"
    )


class AbstractSpeechStatsRepository(abc.ABC):
    @abc.abstractmethod
    async def upsert_speech_stats(self, stats: SpeechStats) -> None:
        pass

    @abc.abstractmethod
    async def get_speech_stats(self, speech_id: UUID) -> SpeechStats | None:
        pass

    @abc.abstractmethod
    async def list_all_speech_stats(self) -> list[SpeechStats]:
        pass

    @abc.abstractmethod
    async def list_speech_ids_with_summary(self) -> list[str]:
        pass


class MongoSpeechStatsRepository(AbstractSpeechStatsRepository):
    def __init__(
        self, mongo_client: AsyncMongoClient, db_name: str = "hansard"
    ):
        self.db = mongo_client[db_name]
        self.collection = self.db["speech_stats"]

    async def get_speech_stats(self, speech_id: UUID) -> SpeechStats | None:
        data = await self.collection.find_one({"id": str(speech_id)})
        if data:
            return SpeechStats.model_validate(data)
        return None

    async def list_all_speech_stats(self) -> list[SpeechStats]:
        cursor = self.collection.find({})
        results = []
        async for document in cursor:
            results.append(SpeechStats.model_validate(document))
        return results

    async def upsert_speech_stats(self, stats: SpeechStats) -> None:
        await self.collection.update_one(
            {"id": str(stats.id)},
            {
                "$set": stats.model_dump(
                    exclude={"id"}, exclude_none=True, mode="json"
                )
            },
            upsert=True,
        )

    async def list_speech_ids_with_summary(self) -> list[str]:
        cursor = self.collection.find({})
        speech_ids: list[str] = []
        async for document in cursor:
            speech_ids.append(document["speech_id"])
        return speech_ids
