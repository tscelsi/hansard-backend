"""Generates the view data for a bill. On the website this is shown on the
/bills/details/{bill_id} page."""

import logging
from datetime import datetime, timedelta
from typing import Any, TypedDict

from pymongo.asynchronous.database import AsyncDatabase

logger = logging.getLogger(__name__)


class PartyCountResult(TypedDict):
    party: str
    count: int


PartyProportionResult = dict[str, float]


party_count_facet: list[dict[str, Any]] = [
    {"$match": {"part_seq": 0}},
    {
        "$lookup": {
            "from": "talkers",
            "localField": "talker_id",
            "foreignField": "id",
            "as": "talker_info",
        },
    },
    {"$unwind": "$talker_info"},
    {
        "$group": {
            "_id": "$talker_info.party",
            "count": {"$sum": 1},
        },
    },
    {"$project": {"_id": 0, "party": "$_id", "count": 1}},
    {"$sort": {"count": -1}},
]


class TopSpeakerResult(TypedDict):
    id: str
    name: str
    party: str
    count: int
    house: str


top_speaker_facet: list[dict[str, Any]] = [
    {"$match": {"part_seq": 0}},
    {
        "$group": {
            "_id": "$talker_id",
            "speech_count": {"$sum": 1},
            "house": {"$first": "$house"},
        },
    },
    {
        "$lookup": {
            "from": "talkers",
            "localField": "_id",
            "foreignField": "id",
            "as": "talker_info",
        },
    },
    {"$unwind": "$talker_info"},
    {
        "$project": {
            "_id": 0,
            "id": "$_id",
            "name": "$talker_info.name",
            "party": "$talker_info.party",
            "count": "$speech_count",
            "house": 1,
        },
    },
    {"$sort": {"count": -1, "name": 1}},
]


class SpeechesOverTimeResult(TypedDict):
    date: datetime
    hor: int
    senate: int


speeches_over_time: list[dict[str, Any]] = [
    {"$match": {"part_seq": 0}},
    {
        "$group": {
            "_id": {"date": "$date", "house": "$house"},
            "count": {"$sum": 1},
        },
    },
    {
        "$group": {
            "_id": "$_id.date",
            "counts": {"$push": {"k": "$_id.house", "v": "$count"}},
        },
    },
    {"$project": {"_id": 0, "date": "$_id", "counts": 1}},
    {"$addFields": {"countsObj": {"$arrayToObject": "$counts"}}},
    {
        "$project": {
            "date": 1,
            "hor": "$countsObj.hor",
            "senate": "$countsObj.senate",
        },
    },
]


class SpeechPartWithTalkerInfo(TypedDict):
    date: datetime
    part_id: str
    bill_ids: list[str]
    house: str
    chamber: str
    speech_id: str
    subdebate_1_title: str
    part_seq: int
    talker_id: str
    talker_name: str | None
    talker_party: str | None
    talker_electorate: str | None
    speech_content: str


class SpeechListResult(TypedDict):
    date: str
    parts: list[SpeechPartWithTalkerInfo]


speech_list: list[dict[str, Any]] = [
    {"$match": {"$or": [{"part_seq": 0}, {"type": "first_reading"}]}},
    {
        "$sort": {
            "date": 1,
            "debate_seq": 1,
            "subdebate_1_seq": 1,
            "subdebate_2_seq": 1,
            "speech_seq": 1,
            "part_seq": 1,
        },
    },
    {
        "$lookup": {
            "from": "talkers",
            "localField": "talker_id",
            "foreignField": "id",
            "as": "talker_info",
        },
    },
    {
        "$addFields": {
            "talker_name": {
                "$ifNull": [{"$arrayElemAt": ["$talker_info.name", 0]}, None],
            },
            "talker_party": {
                "$ifNull": [{"$arrayElemAt": ["$talker_info.party", 0]}, None],
            },
            "talker_electorate": {
                "$ifNull": [
                    {"$arrayElemAt": ["$talker_info.electorate", 0]},
                    None,
                ],
            },
        },
    },
    {"$project": {"talker_info": 0}},
    {"$group": {"_id": "$date", "parts": {"$push": "$$ROOT"}}},
    {"$sort": {"_id": -1}},
]


class SentimentResult(TypedDict):
    talker_id: str
    speech_id: str
    name: str
    party: str
    electorate: str
    house: str
    stance: float
    tone: float


sentiment: list[dict[str, Any]] = [
    {"$match": {"part_seq": 0}},
    {
        "$sort": {
            "date": 1,
            "debate_seq": 1,
            "subdebate_1_seq": 1,
            "subdebate_2_seq": 1,
            "speech_seq": 1,
            "part_seq": 1,
        },
    },
    {
        "$lookup": {
            "from": "talkers",
            "localField": "talker_id",
            "foreignField": "id",
            "as": "talker_info",
        },
    },
    {"$unwind": {"path": "$talker_info"}},
    {
        "$lookup": {
            "from": "speech_stats",
            "localField": "speech_id",
            "foreignField": "speech_id",
            "as": "speech_stats",
        },
    },
    {"$unwind": {"path": "$speech_stats"}},
    {
        "$project": {
            "_id": 0,
            "talker_id": 1,
            "speech_id": 1,
            "name": "$talker_info.name",
            "party": "$talker_info.party",
            "electorate": "$talker_info.electorate",
            "house": 1,
            "stance": "$speech_stats.stance",
            "tone": "$speech_stats.tone",
        },
    },
]


class BillOverviewResult(TypedDict):
    partyCounts: list[PartyCountResult]
    topSpeakers: list[TopSpeakerResult]
    overTime: list[SpeechesOverTimeResult]
    speechList: list[SpeechListResult]
    sentiment: list[SentimentResult]


async def call_db(bill_id: str, db: AsyncDatabase[Any]):
    async with await db["parts"].aggregate(
        [
            {
                "$match": {
                    "bill_ids": bill_id,
                }
            },
            {
                "$facet": {
                    "partyCounts": party_count_facet,
                    "topSpeakers": top_speaker_facet,
                    "overTime": speeches_over_time,
                    "speechList": speech_list,
                    "sentiment": sentiment,
                }
            },
        ]
    ) as cursor:
        async for doc in cursor:
            return doc


def get_party_speech_proportions(
    party_counts: list[PartyCountResult],
) -> PartyProportionResult:
    counts = party_counts or []
    total = sum(r["count"] for r in counts)
    proportions: PartyProportionResult = {}
    for r in counts:
        proportion = round((r["count"] / total) * 100, 2) if total > 0 else 0.0
        proportions[r["party"]] = proportion
    return proportions


def fill_missing_dates(
    speeches_over_time: list[SpeechesOverTimeResult],
):
    existing_dates = set(s["date"] for s in speeches_over_time)
    # set hour, min, sec to 0 for comparison
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    acc = today - timedelta(days=18)
    while acc <= today:
        if acc not in existing_dates:
            speeches_over_time.append(
                SpeechesOverTimeResult(**{"date": acc, "hor": 0, "senate": 0})
            )
        acc += timedelta(days=1)
    speeches_over_time.sort(key=lambda x: x["date"])
    return speeches_over_time


class FinalBillOverviewResult(TypedDict):
    partySpeechProportions: PartyProportionResult
    speechesOverTime: list[SpeechesOverTimeResult]
    topSpeakers: list[TopSpeakerResult]
    speechList: list[SpeechListResult]
    sentiment: list[SentimentResult]


async def generate_bill_overview(
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
    return {
        "partySpeechProportions": party_speech_proportions,
        "speechesOverTime": over_time,
        "topSpeakers": res.get("topSpeakers", []),
        "speechList": res.get("speechList", []),
        "sentiment": res.get("sentiment", []),
    }
