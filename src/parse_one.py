import asyncio
import logging

from hansard.entities.speech import HouseType
from hansard.managers import DbManager, EventManager
from hansard.services import parse_one
from paths import DATA_DIR


def logging_setup():
    logging.basicConfig(level=logging.DEBUG)
    modules = ["pymongo", "urllib3", "httpx", "selenium"]
    for module in modules:
        logging.getLogger(module).setLevel("ERROR")


async def db_and_event_setup():
    DbManager.get()
    event_manager = EventManager.get()
    await event_manager.setup()


async def main():
    logging_setup()
    await db_and_event_setup()
    await parse_one(
        HouseType.HOR,
        DATA_DIR / "hansard" / "hor" / "hansard-2025-11-03.xml",
        DbManager.get().speech_part_repo,
        DbManager.get().talker_repo,
        publisher=EventManager.get().publisher,
    )
    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
