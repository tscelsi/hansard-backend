import logging
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Sequence, TypedDict

import httpx
from bs4 import BeautifulSoup, ResultSet, Tag
from pymongo.asynchronous.collection import AsyncCollection
from pymongo.asynchronous.database import AsyncDatabase

from hansard.downloader import Downloader, create_driver
from hansard.entities.speech import HouseType
from hansard.parser import Parser
from hansard.repositories.part_repository import AbstractPartRepository
from hansard.repositories.talker_repository import AbstractTalkerRepository
from hansard.views.bill_overview import (
    FinalBillOverviewResult,
    call_db,
    fill_missing_dates,
    get_party_speech_proportions,
)
from utils.events.local import LocalPublisher

logger = logging.getLogger(__name__)
HOUSE_URLS = {
    "hor": "https://www.aph.gov.au/Parliamentary_Business/Hansard/Hansreps_2011",  # noqa
    "senate": "https://www.aph.gov.au/Parliamentary_Business/Hansard/Hanssen261110",  # noqa
}


def find_unparsed_hrefs(anchors: ResultSet[Tag], last_parsed_date: date):
    dates = [
        (
            datetime.strptime(
                a.attrs.get("aria-label"),  # type: ignore
                "%d-%b-%Y",
            ).date(),
            str(a["href"]),
        )
        for a in anchors
        if a.attrs.get("aria-label") is not None
    ]
    sorted_dates = sorted(dates, key=lambda x: x[0])
    hrefs_to_parse: list[str] = []
    for _date, href in sorted_dates:
        if _date > last_parsed_date:
            hrefs_to_parse.append(href)
    return hrefs_to_parse


class LatestParsedType(TypedDict):
    value: datetime


async def check(
    speech_part_repo: AbstractPartRepository,
    talker_repo: AbstractTalkerRepository,
    latest_parsed_collection: AsyncCollection[LatestParsedType],
    publisher: LocalPublisher,
):
    logger.info(
        f"[{datetime.now(tz=timezone.utc)}] checking for new hansard sessions to parse"  # noqa
    )
    for house in [HouseType.HOR, HouseType.SENATE]:
        logger.debug(f"[{house.value}] checking for new sessions")
        new_sessions = await check_new_sessions_in_house(
            house, latest_parsed_collection
        )
        if new_sessions:
            await parse_many(
                house,
                new_sessions,
                speech_part_repo,
                talker_repo,
                publisher,
            )
            await latest_parsed_collection.update_one(
                {"house": house.value},
                {"$set": {"value": datetime.now(tz=timezone.utc)}},
                upsert=True,
            )
    logger.info("check complete")


async def check_new_sessions_in_house(
    house: HouseType,
    latest_parsed_collection: AsyncCollection[LatestParsedType],
):
    new_sessions_to_parse: list[str | Path] = []
    logger.debug(f"[{house.value}] checking for new sessions")
    async with httpx.AsyncClient(timeout=30) as httpx_client:
        res = await httpx_client.get(HOUSE_URLS[house.value])
    soup = BeautifulSoup(res.text, "html.parser")
    div = soup.find("div", {"class": "large-9"})
    if div is None or not isinstance(div, Tag):
        logger.warning(f"[{house.value}] could not find main content div")
        return new_sessions_to_parse  # empty
    anchors = div.css.select("table a")
    obj = await latest_parsed_collection.find_one({"house": house.value})
    if obj is None:
        logger.error(f"[{house.value}] no latest parsed date found")
        return new_sessions_to_parse  # empty
    last_parsed = obj["value"].date()
    hrefs = find_unparsed_hrefs(anchors, last_parsed)
    if not hrefs:
        logger.info(f"[{house.value}] no new sessions to parse")
        return new_sessions_to_parse  # empty
    downloader = Downloader(create_driver(headless=True))
    logger.debug(f"[{house.value}] found {len(hrefs)} new sessions to parse")
    for href in hrefs:
        try:
            filepath = downloader.download_hansard_xml(house, href)
            new_sessions_to_parse.append(filepath)
        except ValueError:
            continue
    return new_sessions_to_parse


async def parse_one(
    house: HouseType,
    filepath: Path | str,
    speech_part_repo: AbstractPartRepository,
    talker_repo: AbstractTalkerRepository,
    publisher: LocalPublisher | None = None,
):
    parser = Parser(house, filepath)
    talkers = parser.parse_talkers()
    parser.parse()
    await speech_part_repo.upsert(parser.parts)
    await talker_repo.save_talkers(talkers)
    if publisher:
        publisher.publish(
            {
                "topic": "parser.completed",
                "speech_ids": parser.speech_ids,
                "house": house.value,
            }
        )


async def parse_many(
    house: HouseType,
    new_session_files: Sequence[Path | str],
    speech_part_repo: AbstractPartRepository,
    talker_repo: AbstractTalkerRepository,
    publisher: LocalPublisher,
):
    speech_ids: set[str] = set()
    for filepath in new_session_files:
        parser = Parser(house, filepath)
        talkers = parser.parse_talkers()
        parser.parse()
        speech_ids.update(parser.speech_ids)
        await speech_part_repo.upsert(parser.parts)
        await talker_repo.save_talkers(talkers)
    publisher.publish(
        {
            "topic": "parser.completed",
            "speech_ids": list(speech_ids),
            "house": house.value,
        }
    )


async def refresh_bill_overview(
    bill_id: str, db: AsyncDatabase[Any]
) -> FinalBillOverviewResult | None:
    res = await call_db(bill_id, db)
    if res is None:
        logger.error(f"[bill:{bill_id}] error creating bill overview")
        return None
    party_speech_proportions = get_party_speech_proportions(
        res.get("partyCounts", [])
    )
    over_time = fill_missing_dates(res.get("overTime", []))
    await db["bill_overview"].update_one(
        {"bill_id": bill_id},
        {
            "$set": {
                "partySpeechProportions": party_speech_proportions,
                "speechesOverTime": over_time,
                "topSpeakers": res.get("topSpeakers", []),
                "speechList": res.get("speechList", []),
                "sentiment": res.get("sentiment", []),
                "updatedAt": datetime.now(tz=timezone.utc),
            }
        },
        upsert=True,
    )


async def main():
    logging_setup()
    await db_and_event_setup()
    house = HouseType.HOR
    # all hor sessions
    new_session_files = sorted(
        (DATA_DIR / "hansard" / house.value).glob("hansard-*.xml")
    )
    await parse_many(
        house,
        new_session_files,
        DbManager.get().speech_part_repo,
        DbManager.get().talker_repo,
        EventManager.get().publisher,
    )
    await asyncio.sleep(100)  # wait for event processing
