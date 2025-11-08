from pydantic import BaseModel, Field

from hansard.entities.speech import HouseType


class BaseEvent(BaseModel):
    topic: str


class ParserCompletedEvent(BaseModel):
    """Fires on completion of parsing one or multiple
    sessions of parliamentary sitting(s).
    """

    topic: str = Field("parser.completed", frozen=True)
    house: HouseType
    speech_ids: list[str]


class BatchSummaryCompletedEvent(BaseModel):
    """Fires on completion of a batch of LLM summarisation jobs."""

    topic: str = Field("nlp.batch_completed", frozen=True)
    batch_id: str
    speech_ids: list[str]
