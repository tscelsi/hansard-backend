import asyncio
import logging

from hansard.entities.speech import HouseType
from hansard.managers import DbManager, EventManager
from hansard.services import parse_many
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
    house = HouseType.HOR
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
    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
