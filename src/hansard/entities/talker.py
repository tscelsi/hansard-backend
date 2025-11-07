import logging

from bs4 import BeautifulSoup
from pydantic import BaseModel, Field, ValidationError

logger = logging.getLogger(__name__)


class Talker(BaseModel):
    """
    <talker>
        <page.no>15</page.no>
        <time.stamp/>
        <name role="metadata">Plibersek, Tanya Joan MP</name>
        <name.id>83M</name.id>
        <electorate>Sydney</electorate>
        <party>ALP</party>
        <in.gov/>
        <first.speech/>
    </talker>
    """

    id: str | None
    name: str
    electorate: str | None
    party: str | None
    divisiveness: float | None = Field(
        default=None,
        description="Divisiveness across all speeches in chamber.",
    )
    bill_divisiveness: float | None = Field(
        default=None,
        description="Divisiveness across bills speeches in chamber.",
    )

    def __hash__(self) -> int:
        return hash((self.id, self.name, self.electorate, self.party))

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Talker):
            return NotImplemented
        return (
            self.id == other.id
            and self.name == other.name
            and self.electorate == other.electorate
            and self.party == other.party
        )

    @classmethod
    def extract_talkers(cls, soup: BeautifulSoup) -> list["Talker"]:
        talkers: list[Talker] = []
        for talker_tag in soup.find_all("talker"):
            name_tag = talker_tag.find("name")
            name = name_tag.text.strip() if name_tag else None
            id_tag = talker_tag.find("name.id")
            id_ = id_tag.text.strip() if id_tag else None
            electorate_tag = talker_tag.find("electorate")
            electorate = (
                electorate_tag.text.strip() if electorate_tag else None
            )
            party_tag = talker_tag.find("party")
            party = party_tag.text.strip() if party_tag else None
            if not name or not id_ or not electorate or not party:
                logger.warning(f"Talker missing required fields: {talker_tag}")
                continue
            try:
                talker = cls(
                    id=id_,
                    name=name,
                    electorate=electorate,
                    party=party,
                )
            except ValidationError as e:
                logger.error(f"Validation error for talker {talker_tag}: {e}")
                continue
            talkers.append(talker)
        return list(set(talkers))


class TalkerList:
    def __init__(self, talkers: list[Talker]) -> None:
        self.talkers = talkers
        self._talker_map = {
            talker.id: talker for talker in talkers if talker.id
        }
        super().__init__()

    def __hash__(self) -> int:
        return hash(tuple(sorted(self._talker_map.keys())))

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, TalkerList):
            return NotImplemented
        return self._talker_map == other._talker_map

    def __iter__(self):
        return iter(self.talkers)

    def find(self, talker_id: str) -> Talker | None:
        return self._talker_map.get(talker_id)
