import logging
import re
from datetime import datetime
from pathlib import Path
from typing import Literal, cast

from bs4.element import Tag
from pydantic import ValidationError

from hansard.entities.speech import (
    CHAMBER_MAP,
    ChamberType,
    HouseType,
    Part,
    PartType,
    SpeechPart,
    SpeechPartType,
)
from hansard.entities.talker import Talker

logger = logging.getLogger(__name__)

PATTERN = re.compile(r".*?(?:\(\d{2}:\d{2}\))?:")


def is_interjection(tag: Tag) -> str | None:
    member_interjection = tag.find("a", {"type": "MemberInterjecting"})
    office_interjection = tag.find("span", {"class": "HPS-OfficeInterjecting"})
    general_interjection = tag.find(
        "span", {"class": "HPS-GeneralIInterjecting"}
    )
    if member_interjection:
        return "MEMBER"
    elif office_interjection:
        return "OFFICE"
    elif general_interjection:
        return "GENERAL"
    return None


def is_continuation(tag: Tag) -> bool:
    member_continuation = tag.find("a", {"type": "MemberContinuation"})
    if member_continuation:
        return True
    return False


def extract_talker(talker_tag: Tag) -> Talker:
    """
    <talker>
        <page.no>30</page.no>
        <time.stamp/>
        <name role="metadata">Wilson, Tim MP</name>
        <name.id>IMW</name.id>
        <electorate>Goldstein</electorate>
        <party>LP</party>
        <in.gov/>
        <first.speech/>
    </talker>
    """
    name_tag = talker_tag.find("name")
    name = name_tag.text.strip() if name_tag else None
    id_tag = talker_tag.find("name.id")
    id_ = id_tag.text.strip() if id_tag else None
    electorate_tag = talker_tag.find("electorate")
    electorate = electorate_tag.text.strip() if electorate_tag else None
    party_tag = talker_tag.find("party")
    party = party_tag.text.strip() if party_tag else None
    if not name:
        raise ValueError(f"Talker missing required fields: {talker_tag}")
    try:
        talker = Talker(
            id=id_,
            name=name,
            electorate=electorate,
            party=party,
        )
    except ValidationError as e:
        logger.error(f"Validation error for talker {talker_tag}: {e}")
        raise
    return talker


def extract_debate_category(debate_tag: Tag) -> str | None:
    debateinfo_tag = debate_tag.find("debateinfo")
    if not debateinfo_tag:
        return None
    title_tag = debateinfo_tag.find("title")
    if title_tag:
        return title_tag.text.strip()
    return None


def extract_debate_title(
    debate_tag: Tag, type: Literal["debate", "subdebate"]
) -> str | None:
    debateinfo_tag = debate_tag.find(
        "subdebateinfo" if type == "subdebate" else "debateinfo"
    )
    if not debateinfo_tag:
        return None
    title_tag = debateinfo_tag.find("title")
    if title_tag:
        return title_tag.text.strip()
    return None


def extract_debate_info(
    debate_tag: Tag, type: Literal["debate", "subdebate"]
) -> str | None:
    debatetext_tag = debate_tag.find(
        "subdebate.text" if type == "subdebate" else "debate.text"
    )
    if not debatetext_tag:
        return None
    text_ps = debatetext_tag.find_all("p", {"class": "HPS-Normal"})
    if text_ps:
        return "\n\n".join(p.get_text(strip=True) for p in text_ps)
    return None


def extract_bill_ids(debate_tag: Tag) -> list[str] | None:
    a_tags = debate_tag.find_all("a", {"type": "Bill"})
    if a_tags:
        return [a_tag["href"].strip() for a_tag in a_tags]
    return None


class Parser:
    def __init__(self, house: HouseType, filepath: str | Path):
        self.filepath = filepath
        self.house = house
        self.soup = self._load_soup()
        self.date = datetime.fromisoformat(
            self.soup.find("session.header").find("date").text
        )
        self.talkers: dict[str, Talker] = {}
        self.speech_id: str | None = None
        self.speech_ids: set[str] = set()
        self.bill_ids: list[str] | None = None
        self.chamber: ChamberType = ChamberType.UNKNOWN
        self.debate_category: str | None = None
        self.debate_seq = 0
        self.subdebate_1_title: str | None = None
        self.subdebate_1_info: str | None = None
        self.subdebate_1_seq: int | None = None
        self.subdebate_2_title: str | None = None
        self.subdebate_2_info: str | None = None
        self.subdebate_2_seq: int | None = None
        self.speech_seq: int = 0
        self.parts: list[Part] = []

    def _load_soup(self) -> Tag:
        from bs4 import BeautifulSoup

        with open(self.filepath, "r", encoding="utf-8") as f:
            content = f.read()
        soup = BeautifulSoup(content, "xml")
        return soup

    def parse_talkers(self) -> list[Talker]:
        talkers: list[Talker] = []
        for talker in self.soup.find_all("talker"):
            talkers.append(extract_talker(talker))
        return list(set(talkers))

    def parse(self) -> None:
        """Entry point for parsing the hansard XML file."""
        logger.debug(f"[{self.filepath}] starting parse")
        chamber_tag = self.soup.find("chamber.xscript")
        fed_chamber_tag = self.soup.find("fedchamb.xscript")
        chambers: list[Tag] = []
        if chamber_tag:
            chambers.append(cast(Tag, chamber_tag))
        if fed_chamber_tag:
            chambers.append(cast(Tag, fed_chamber_tag))
        if len(chambers) == 0:
            logger.warning("No chamber found in hansard")
        for chamber in chambers:
            self.chamber = CHAMBER_MAP.get(chamber.name, ChamberType.UNKNOWN)
            self.parse_speeches(chamber)
        logger.debug(f"[{self.filepath}] completed parse")

    def parse_speeches(self, chamber: Tag) -> list[Part]:
        debate_tags = chamber.find_all("debate")
        self.date = datetime.fromisoformat(
            self.soup.find("session.header").find("date").text
        )
        for debate_tag in debate_tags:
            self.debate_category = extract_debate_category(debate_tag)
            if self.debate_category is None:
                logger.warning(f"Debate missing title or info: {debate_tag}")
                continue
            subdebate_1_tags = debate_tag.find_all(
                "subdebate.1", recursive=False
            )
            self.subdebate_1_seq = 0
            for subdebate_1_tag in subdebate_1_tags:
                self.bill_ids = (
                    extract_bill_ids(subdebate_1_tag)
                    if self.debate_category == "BILLS"
                    else None
                )
                self.subdebate_1_title = extract_debate_title(
                    subdebate_1_tag, type="subdebate"
                )
                self.subdebate_1_info = extract_debate_info(
                    subdebate_1_tag, type="subdebate"
                )
                if self.subdebate_1_title is None:
                    logger.warning(
                        f"<subdebate.1> missing title or info: {subdebate_1_tag}"
                    )
                    continue
                for speech_tag in subdebate_1_tag.find_all(
                    "speech", recursive=False
                ):
                    speech_parts = self.parse_speech(
                        self.speech_seq, speech_tag
                    )
                    if not speech_parts:
                        logger.warning(
                            f"No speech parts found for speech {speech_id}"
                        )
                    self.parts.extend(speech_parts)
                    self.speech_ids.update(
                        [part.speech_id for part in speech_parts]
                    )
                    self.speech_seq += 1
                self.subdebate_2_seq = 0
                self.speech_seq = 0
                for subdebate_2_tag in subdebate_1_tag.find_all("subdebate.2"):
                    self.subdebate_2_title = extract_debate_title(
                        subdebate_2_tag, type="subdebate"
                    )
                    self.subdebate_2_info = extract_debate_info(
                        subdebate_2_tag, type="subdebate"
                    )
                    if self.subdebate_2_title is None:
                        logger.warning(
                            f"<subdebate.2> missing title or info: {subdebate_2_tag}"
                        )
                        continue
                    elif self.subdebate_2_title == "First Reading":
                        self.parts.append(
                            Part(
                                date=self.date,
                                house=self.house,
                                bill_ids=self.bill_ids,
                                chamber=self.chamber,
                                type=PartType.FIRST_READING,
                                debate_category=self.debate_category,
                                debate_seq=self.debate_seq,
                                subdebate_1_title=self.subdebate_1_title,
                                subdebate_1_info=self.subdebate_1_info,
                                subdebate_1_seq=self.subdebate_1_seq,
                                subdebate_2_title=self.subdebate_2_title,
                                subdebate_2_info=self.subdebate_2_info,
                                subdebate_2_seq=self.subdebate_2_seq,
                            )
                        )
                    for speech_tag in subdebate_2_tag.find_all(
                        "speech", recursive=False
                    ):
                        speech_parts = self.parse_speech(
                            self.speech_seq, speech_tag
                        )
                        if not speech_parts:
                            logger.warning(
                                f"No speech parts found for speech {self.speech_id}"
                            )
                        self.speech_ids.update(
                            [part.speech_id for part in speech_parts]
                        )
                        self.parts.extend(speech_parts)
                        self.speech_seq += 1
                    self.subdebate_2_title = None
                    self.subdebate_2_info = None
                    self.speech_seq = 0
                    self.subdebate_2_seq += 1
                self.bill_ids = None
                self.subdebate_1_title = None
                self.subdebate_1_info = None
                self.subdebate_1_seq += 1
            self.debate_category = None
            self.debate_seq += 1
        return self.parts

    def parse_speech(
        self, speech_seq: int, speech_tag: Tag
    ) -> list[SpeechPart]:
        part_seq = 0
        interjections = [
            extract_talker(x.find("talker"))
            for x in speech_tag.find_all("interjection")
        ]
        curr_interjection_ind = 0
        segments: list[SpeechPart] = []
        main_talker = extract_talker(
            speech_tag.find("talk.start").find("talker")
        )
        prev_speech_part: SpeechPart | None = None
        for p in speech_tag.find_all("p"):
            if is_continuation(p):
                if prev_speech_part is None:
                    raise ValueError(
                        "Continuation found before any speech part"
                    )
                elif prev_speech_part.talker_id == main_talker.id:
                    prev_speech_part.speech_content += "\n\n" + p.get_text(
                        strip=True
                    )
                else:
                    segments.append(prev_speech_part)
                    part_seq += 1
                    prev_speech_part = SpeechPart(
                        date=self.date,
                        house=self.house,
                        bill_ids=self.bill_ids,
                        chamber=self.chamber,
                        type=PartType.SPEECH,
                        debate_category=self.debate_category,
                        debate_seq=self.debate_seq,
                        subdebate_1_title=self.subdebate_1_title,
                        subdebate_1_info=self.subdebate_1_info,
                        subdebate_1_seq=self.subdebate_1_seq,
                        subdebate_2_title=self.subdebate_2_title,
                        subdebate_2_info=self.subdebate_2_info,
                        subdebate_2_seq=self.subdebate_2_seq,
                        talker_id=main_talker.id,
                        speech_seq=speech_seq,
                        part_seq=part_seq,
                        speech_part_type=SpeechPartType.CONTINUATION,
                        speech_content=re.sub(
                            PATTERN, "", p.get_text(strip=True), count=1
                        ).strip(),
                    )
            if (interjection_type := is_interjection(p)) is not None:
                if prev_speech_part is None:
                    raise ValueError(
                        "Interjection found before any speech part"
                    )
                if interjection_type == "GENERAL":
                    segments.append(prev_speech_part)
                    part_seq += 1
                    prev_speech_part = SpeechPart(
                        date=self.date,
                        house=self.house,
                        bill_ids=self.bill_ids,
                        chamber=self.chamber,
                        type=PartType.SPEECH,
                        talker_id="",
                        debate_category=self.debate_category,
                        debate_seq=self.debate_seq,
                        subdebate_1_title=self.subdebate_1_title,
                        subdebate_1_info=self.subdebate_1_info,
                        subdebate_1_seq=self.subdebate_1_seq,
                        subdebate_2_title=self.subdebate_2_title,
                        subdebate_2_info=self.subdebate_2_info,
                        subdebate_2_seq=self.subdebate_2_seq,
                        speech_seq=speech_seq,
                        part_seq=part_seq,
                        speech_part_type=SpeechPartType.INTERJECTION,
                        speech_content="",
                    )
                    continue
                try:
                    interjection_talker = interjections[curr_interjection_ind]
                except IndexError:
                    logger.warning(
                        f"Interjection talker index {curr_interjection_ind} out of range for speech {self.speech_id}"
                    )
                    logger.debug(f"text:{p.text}")
                    continue
                if prev_speech_part.talker_id == interjection_talker.id:
                    prev_speech_part.speech_content += "\n\n" + p.get_text(
                        strip=True
                    )
                    continue
                else:
                    segments.append(prev_speech_part)
                    part_seq += 1
                    prev_speech_part = SpeechPart(
                        date=self.date,
                        house=self.house,
                        bill_ids=self.bill_ids,
                        chamber=self.chamber,
                        type=PartType.SPEECH,
                        talker_id=interjection_talker.id,
                        debate_category=self.debate_category,
                        debate_seq=self.debate_seq,
                        subdebate_1_title=self.subdebate_1_title,
                        subdebate_1_info=self.subdebate_1_info,
                        subdebate_1_seq=self.subdebate_1_seq,
                        subdebate_2_title=self.subdebate_2_title,
                        subdebate_2_info=self.subdebate_2_info,
                        subdebate_2_seq=self.subdebate_2_seq,
                        speech_seq=speech_seq,
                        part_seq=part_seq,
                        speech_part_type=SpeechPartType.INTERJECTION,
                        speech_content=re.sub(
                            PATTERN, "", p.get_text(strip=True), count=1
                        ).strip(),
                    )
                curr_interjection_ind += 1
            else:
                if prev_speech_part is None:
                    # is the first speech part
                    assert part_seq == 0
                    prev_speech_part = SpeechPart(
                        date=self.date,
                        house=self.house,
                        bill_ids=self.bill_ids,
                        chamber=self.chamber,
                        type=PartType.SPEECH,
                        talker_id=main_talker.id,
                        debate_category=self.debate_category,
                        debate_seq=self.debate_seq,
                        subdebate_1_title=self.subdebate_1_title,
                        subdebate_1_info=self.subdebate_1_info,
                        subdebate_1_seq=self.subdebate_1_seq,
                        subdebate_2_title=self.subdebate_2_title,
                        subdebate_2_info=self.subdebate_2_info,
                        subdebate_2_seq=self.subdebate_2_seq,
                        speech_seq=speech_seq,
                        part_seq=part_seq,
                        speech_part_type=SpeechPartType.SPEECH,
                        speech_content=re.sub(
                            PATTERN, "", p.get_text(strip=True), count=1
                        ).strip(),
                    )
                elif prev_speech_part.talker_id == main_talker.id:
                    prev_speech_part.speech_content += "\n\n" + p.get_text(
                        strip=True
                    )
                else:
                    segments.append(prev_speech_part)
                    part_seq += 1
                    prev_speech_part = SpeechPart(
                        date=self.date,
                        house=self.house,
                        bill_ids=self.bill_ids,
                        chamber=self.chamber,
                        type=PartType.SPEECH,
                        talker_id=main_talker.id,
                        debate_category=self.debate_category,
                        debate_seq=self.debate_seq,
                        subdebate_1_title=self.subdebate_1_title,
                        subdebate_1_info=self.subdebate_1_info,
                        subdebate_1_seq=self.subdebate_1_seq,
                        subdebate_2_title=self.subdebate_2_title,
                        subdebate_2_info=self.subdebate_2_info,
                        subdebate_2_seq=self.subdebate_2_seq,
                        speech_seq=speech_seq,
                        part_seq=part_seq,
                        speech_part_type=SpeechPartType.SPEECH,
                        speech_content=re.sub(
                            PATTERN, "", p.get_text(strip=True), count=1
                        ).strip(),
                    )
        if prev_speech_part is not None:
            segments.append(prev_speech_part)
        return segments
