import asyncio
import logging

from openai import AsyncOpenAI
from pymongo import AsyncMongoClient
from pymongo.asynchronous.collection import AsyncCollection

from hansard.entities.speech import HouseType
from hansard.nlp_subscribers import (
    DivisivenessSubscriber,
    SummariserSubscriber,
)
from hansard.parser import Parser
from hansard.repositories.part_repository import MongoPartRepository
from hansard.repositories.speech_stats_repository import (
    MongoSpeechStatsRepository,
)
from hansard.repositories.talker_repository import MongoTalkerRepository
from hansard.services import LatestParsedType, check
from paths import DATA_DIR
from utils.background_tasks import BackgroundTasks
from utils.events.local import LocalPublisher

logger = logging.getLogger(__name__)


def parse_one():
    PATH = DATA_DIR / "hansard" / "senate" / "hansard-2025-07-30.xml"
    parser = Parser(HouseType.SENATE, PATH)
    parser.parse()


async def main_loop(
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
        except Exception as e:
            logger.error(f"Error during check(): {e}")
        logger.info("Check loop sleeping for 24 hours...")
        await asyncio.sleep(60 * 60 * 24)  # Check every day


async def main():
    logging.basicConfig(level=logging.DEBUG)
    publisher = LocalPublisher()
    mongo_client = AsyncMongoClient[LatestParsedType](
        "mongodb://localhost:32768/?directConnection=true"
    )
    latest_parsed_collection = mongo_client["hansard"]["latest_parsed"]
    speech_part_repo = MongoPartRepository(mongo_client, "hansard")
    speech_stats_repo = MongoSpeechStatsRepository(mongo_client, "hansard")
    talker_repo = MongoTalkerRepository(mongo_client, "hansard")
    client = AsyncOpenAI()
    background_tasks = BackgroundTasks()
    summariser_event_listener = SummariserSubscriber(
        talker_repository=talker_repo,
        speech_part_repo=speech_part_repo,
        speech_stats_repo=speech_stats_repo,
        publisher=publisher,
        client=client,
        background_tasks=background_tasks,
    )
    divisiveness_event_listener = DivisivenessSubscriber(
        talker_repository=talker_repo,
        speech_part_repo=speech_part_repo,
        publisher=publisher,
    )
    await summariser_event_listener.subscribe(["parser.completed"])
    await divisiveness_event_listener.subscribe(["parser.completed"])
    await main_loop(
        speech_part_repo,
        talker_repo,
        latest_parsed_collection,
        publisher,
    )


if __name__ == "__main__":
    asyncio.run(main())
