import asyncio
import logging
from typing import TypedDict

from openai import AsyncOpenAI
from pymongo import AsyncMongoClient
from pymongo.asynchronous.collection import AsyncCollection

from hansard.entities.speech import HouseType
from hansard.nlp.services import (
    SummaryBatchManager,
    calculate_house_divisiveness,
)
from hansard.nlp.summary import openai_save_batch_results
from hansard.repositories.part_repository import MongoPartRepository
from hansard.repositories.speech_stats_repository import (
    MongoSpeechStatsRepository,
)
from hansard.repositories.talker_repository import MongoTalkerRepository
from hansard.services import LatestParsedType, check
from utils.background_tasks import BackgroundTasks
from utils.events.local import LocalPublisher

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
modules = ["pymongo", "urllib3", "httpx", "selenium", "httpcore", "openai"]
for module in modules:
    logging.getLogger(module).setLevel("ERROR")


async def _parse_main_loop(
    speech_part_repo: MongoPartRepository,
    talker_repo: MongoTalkerRepository,
    latest_parsed_collection: AsyncCollection[LatestParsedType],
    publisher: LocalPublisher,
):
    while True:
        try:
            await check(
                speech_part_repo,
                talker_repo,
                latest_parsed_collection,
                publisher,
            )
            await calculate_house_divisiveness(
                HouseType.HOR, speech_part_repo, talker_repo
            )
            await calculate_house_divisiveness(
                HouseType.SENATE, speech_part_repo, talker_repo
            )
        except Exception as e:
            logger.error(f"Error during check(): {e}")
        logger.info("Check loop sleeping for 24 hours...")
        await asyncio.sleep(60 * 60 * 24)


async def parse_main(
    db_conn_str: str = "mongodb://localhost:27017/", db_name: str = "hansard"
):
    publisher = LocalPublisher()
    mongo_client = AsyncMongoClient[LatestParsedType](db_conn_str)
    latest_parsed_collection = mongo_client[db_name]["latest_parsed"]
    speech_part_repo = MongoPartRepository(mongo_client, db_name)
    talker_repo = MongoTalkerRepository(mongo_client, db_name)
    await _parse_main_loop(
        speech_part_repo,
        talker_repo,
        latest_parsed_collection,
        publisher,
    )


async def summarise_main(
    db_conn_str: str = "mongodb://localhost:27017/", db_name: str = "hansard"
):
    class ClientType(TypedDict):
        speech_id: str

    publisher = LocalPublisher()
    mongo_client = AsyncMongoClient[ClientType](db_conn_str)
    speech_part_repo = MongoPartRepository(mongo_client, db_name)
    speech_stats_repo = MongoSpeechStatsRepository(mongo_client, db_name)
    talker_repo = MongoTalkerRepository(mongo_client, db_name)
    client = AsyncOpenAI()
    background_tasks = BackgroundTasks.get()
    batch_manager = SummaryBatchManager(
        speech_part_repo,
        speech_stats_repo,
        talker_repo,
        publisher,
        client,
    )
    background_tasks.add(batch_manager.start)
    collection = mongo_client[db_name]["parts"]
    cursor = collection.find({"house": "hor", "type": "speech", "part_seq": 0})
    speech_ids: set[str] = set()
    async for doc in cursor:
        speech_ids.add(doc["speech_id"])
    batch_manager.add(list(speech_ids))
    while not batch_manager.empty:
        await asyncio.sleep(1)
    logger.info("All speeches summarised.")


async def save_batch(
    db_conn_str: str = "mongodb://localhost:27017/", db_name: str = "hansard"
):
    mongo_client = AsyncMongoClient(db_conn_str)
    speech_stats_repo = MongoSpeechStatsRepository(mongo_client, db_name)
    client = AsyncOpenAI()
    await openai_save_batch_results(
        "batch_690e72b21c24819089af6894a7ef5c3e", speech_stats_repo, client
    )


if __name__ == "__main__":
    asyncio.run(summarise_main(db_name="tmp"))
