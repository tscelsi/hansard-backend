import datetime
from enum import Enum
from typing import Any, Literal, Sequence

from pydantic import BaseModel, Field, computed_field

from hansard.entities.talker import TalkerList


class SpeechPartType(str, Enum):
    INTERJECTION = "interjection"
    CONTINUATION = "continuation"
    SPEECH = "speech"


class ChamberType(str, Enum):
    HOR_MAIN = "main"
    HOR_FEDERATION = "federation"
    UNKNOWN = "unknown"


class HouseType(str, Enum):
    SENATE = "senate"
    HOR = "hor"


CHAMBER_MAP = {
    "chamber.xscript": ChamberType.HOR_MAIN,
    "fedchamb.xscript": ChamberType.HOR_FEDERATION,
}


class PartType(str, Enum):
    SPEECH = "speech"
    FIRST_READING = "first_reading"


class Part(BaseModel):
    def _get_id(self, type: Literal["part", "speech"]) -> str:
        """
        full id (which is unique per-part) could look like: 09102025_hor_main_3_1_none_0_0
        the speech id would then be: 09102025_hor_main_3_1_none_0 (missing the final segment)
        """

        def _none_if_none(val: Any):
            return "none" if val is None else val

        hashable: Sequence[str | int] = [
            self.date.isoformat(),
            self.house.value,
            self.chamber.value,
            self.debate_seq,
            _none_if_none(self.subdebate_1_seq),
            _none_if_none(self.subdebate_2_seq),
            getattr(self, "speech_seq", "none"),
        ]
        if type == "part":
            hashable.append(getattr(self, "part_seq", "none"))
        return "_".join(map(str, hashable))

    @computed_field
    @property
    def part_id(self) -> str:
        """The unique identifier for this part."""
        return self._get_id("part")

    date: datetime.date = Field(
        description="The date to which the part belongs."
    )
    bill_ids: list[str] | None = Field(
        description="""The identifier of any bills being discussed in the
                        speech. Only present if a bill is being discussed.
                        i.e. the `debate_category` must be 'BILLS'"""
    )
    house: HouseType = Field(
        description="The house in which the speech took place."
    )
    chamber: ChamberType = Field(
        description="The chamber in which the speech took place."
    )
    type: PartType = Field(
        description="""The type of part given. Can
                                 be a speech, or a first reading."""
    )
    debate_category: str = Field(
        description="""The category of debate during
                                 which the speech is given. For example
                                 'BILLS' or 'STATEMENTS BY MEMBERS'"""
    )
    debate_seq: int = Field(
        description="""The sequence number of the debate within the wider
        context of the proceedings"""
    )
    subdebate_1_title: str = Field(
        description="""The title of the debate during
                              which the speech is given. For example 'Repeal
                              Net Zero Bill 2025'"""
    )
    subdebate_1_info: str | None = Field(
        description="""Any extra information regarding the debate"""
    )
    subdebate_1_seq: int | None = Field(
        description="""The sequence number of the subdebate within the wider
        context of the debate"""
    )
    subdebate_2_title: str | None = Field(
        description="""The sub-category under which the speech is given. For
        example 'Second Reading'"""
    )
    subdebate_2_info: str | None = Field(
        description="""Any extra information regarding the subdebate"""
    )
    subdebate_2_seq: int | None = Field(
        description="""The sequence number of the subsubdebate within the wider
        context of the subdebate"""
    )


class SpeechPart(Part):
    @computed_field
    @property
    def speech_id(self) -> str:
        """The unique identifier for the speech to which this part belongs."""
        return self._get_id("speech")

    speech_seq: int = Field(
        description="""The sequence number of this within the wider
        context of the subdebate speeches."""
    )
    part_seq: int = Field(
        description="""The sequence number of this part within the wider
        context of the speech parts. This is used to order the parts when
        reconstructing the full speech."""
    )
    talker_id: str = Field(
        description="""The unique identifier of the talker
                           conducting this part of the speech"""
    )
    speech_content: str = Field(description="The speech part content")
    speech_part_type: SpeechPartType = Field(
        description="""The type of speech given. Can
                                 be a speech, an interjection, or a
                                 continuation after an interjection by another
                                 member."""
    )


class Speech(BaseModel):
    parts: list[SpeechPart] = Field(
        description="The parts that make up this speech."
    )

    @property
    def id(self):
        return self.parts[0].speech_id

    @property
    def title(self):
        return self.parts[0].subdebate_1_title

    @property
    def talker_id(self):
        return self.parts[0].talker_id

    @classmethod
    def from_parts(cls, parts: list[SpeechPart]) -> "Speech":
        if len(parts) == 0:
            raise ValueError("Speech cannot be empty. Need at least one part.")
        sorted_parts = sorted(parts, key=lambda part: part.part_seq)
        return cls(parts=sorted_parts)

    def to_string(self, talkers: TalkerList) -> str:
        """Convert the speech to a string transcript.

        Args:
            talker_lookup (dict[str, Talker]): A map of talker IDs to their
                information.
        """
        speech_str = f"Bill being discussed: {self.title}\n\n"
        for part in self.parts:
            talker = talkers.find(part.talker_id)
            if not talker:
                speech_str += f"UNKNOWN: {part.speech_content}\n\n"
            else:
                speech_str += f"{talker.name} ({talker.party}): {part.speech_content}\n\n"
        return speech_str
