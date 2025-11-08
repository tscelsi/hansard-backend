import asyncio
import logging

import pandas as pd
from openai import AsyncOpenAI

from hansard.entities.speech import HouseType, Speech, SpeechPart
from hansard.nlp.summary import (
    openai_create_batch_file,
    openai_poll_batch_results,
    openai_upload_batch_file,
)
from hansard.repositories.part_repository import AbstractPartRepository
from hansard.repositories.speech_stats_repository import (
    AbstractSpeechStatsRepository,
)
from hansard.repositories.talker_repository import AbstractTalkerRepository
from utils.background_tasks import BackgroundTasks
from utils.events.local import LocalPublisher
from utils.logger import CustomLoggingAdapter

from .divisiveness import (
    calculate_divisiveness as calculate_divisiveness_domain,
)

logger = logging.getLogger(__name__)


async def calculate_house_divisiveness(
    house: HouseType,
    part_repo: AbstractPartRepository,
    talker_repo: AbstractTalkerRepository,
):
    result = await part_repo.list_all_speech_parts()
    df = pd.DataFrame([part.model_dump() for part in result])
    df = df[df.type == "speech"]
    house_mask = df.house == house
    empty_bill_id_mask = df["bill_ids"].isna() | df["bill_ids"] == ""
    house_df = calculate_divisiveness_domain(df[house_mask])
    house_bill_df = calculate_divisiveness_domain(
        df[house_mask & ~empty_bill_id_mask]
    )
    house_bill_df = house_bill_df.loc[:, ["talker_id", "divisiveness"]]
    house_bill_df.rename(
        columns={"divisiveness": "bill_divisiveness"}, inplace=True
    )
    house_df = house_df.loc[:, ["talker_id", "divisiveness"]]
    house_df = house_df.merge(house_bill_df, on="talker_id", how="left")
    house_df = (
        house_df.groupby("talker_id")  # type: ignore
        .aggregate(
            {
                "divisiveness": "mean",
                "bill_divisiveness": "mean",
            }
        )
        .reset_index()
    )
    house_updates = house_df.to_dict(orient="records")  # type: ignore
    await talker_repo.update_divisiveness(house_updates)


async def run_batch_speech_summarisation(
    speech_ids: list[str],
    speech_part_repo: AbstractPartRepository,
    speech_stats_repo: AbstractSpeechStatsRepository,
    talker_repo: AbstractTalkerRepository,
    publisher: LocalPublisher,
    client: AsyncOpenAI,
    background_tasks: BackgroundTasks | None = None,
):
    """Attempt to run a batch of speech summarisation using OpenAI batch API.

        This should be run when new speeches are added to the database.

    Args:
        speech_ids (list[str]): list of speech IDs to summarise
        speech_part_repo (AbstractPartRepository): Repository for speech parts
        speech_stats_repo (AbstractSpeechStatsRepository): Repository for
            speech stats
        talker_repo (AbstractTalkerRepository): Repository for talkers
        publisher (LocalPublisher): Event publisher
        background_tasks (BackgroundTasks): Background task manager
        client (AsyncOpenAI): OpenAI client
    """
    talkerlist = await talker_repo.list_talkers()
    speech_parts = await speech_part_repo.list_speech_parts_by_speeches(
        speech_ids
    )
    already_summarised_ids = (
        await speech_stats_repo.list_speech_ids_with_summary()
    )
    speeches: list[Speech] = []
    # because speech parts are returned sorted by speech_id and speech_seq
    # we can group them by speech_id in one pass
    current_speech_id: str | None = None
    current_speech_parts: list[SpeechPart] = []
    for part in speech_parts:
        if part.speech_id != current_speech_id:
            if current_speech_id is not None:
                speeches.append(Speech(parts=current_speech_parts))
            current_speech_id = part.speech_id
            current_speech_parts = [part]
        else:
            current_speech_parts.append(part)
    if current_speech_id is not None:
        speeches.append(Speech(parts=current_speech_parts))
    speech_entries = [
        (speech.id, speech.to_string(talkerlist))
        for speech in speeches
        if speech.id not in already_summarised_ids
    ]
    openai_create_batch_file(speeches, talkerlist)
    batch_res = await openai_upload_batch_file(client)
    if background_tasks:
        background_tasks.add(
            openai_poll_batch_results,
            batch_res.id,
            speech_stats_repo,
            publisher,
            client,
        )


class SummaryBatchManager:
    speech_id_buffer: list[str] = []

    def __init__(
        self,
        speech_part_repo: AbstractPartRepository,
        speech_stats_repo: AbstractSpeechStatsRepository,
        talker_repo: AbstractTalkerRepository,
        publisher: LocalPublisher,
        client: AsyncOpenAI,
    ):
        self._logger = CustomLoggingAdapter(
            logging.getLogger(__name__), {"ctx": "BatchSummaryManager"}
        )
        self.speech_part_repo = speech_part_repo
        self.speech_stats_repo = speech_stats_repo
        self.talker_repo = talker_repo
        self.publisher = publisher
        self.client = client

    @property
    def empty(self) -> bool:
        return len(self.speech_id_buffer) == 0

    def add(self, speech_ids: list[str]) -> None:
        self.speech_id_buffer.extend(speech_ids)

    async def get_running_batch_id(self) -> str | None:
        res = await self.client.batches.list()
        while True:
            for batch in res.data:
                if batch.status in (
                    "in_progress",
                    "validating",
                    "finalizing",
                    "cancelling",
                ):
                    return batch.id
            if res.has_more:
                res = await self.client.batches.list(after=res.data[-1].id)
            else:
                break
        return None

    async def get_next_batch(self):
        if not self.speech_id_buffer:
            return list[str]()
        already_summarised_ids = (
            await self.speech_stats_repo.list_speech_ids_with_summary()
        )
        next_speech_ids: list[str] = []
        last_index = 0
        for speech_id in self.speech_id_buffer:
            if speech_id not in already_summarised_ids:
                next_speech_ids.append(speech_id)
            last_index += 1
            if len(next_speech_ids) >= 1000:
                break
        self.speech_id_buffer = self.speech_id_buffer[last_index:]
        return next_speech_ids

    async def start(self):
        self._logger.info("starting batch summary manager")
        while True:
            next_speech_ids = await self.get_next_batch()
            if not next_speech_ids:
                self._logger.debug("no speech IDs to summarise, sleeping")
                await asyncio.sleep(10)
                continue
            try:
                self._logger.info(
                    f"running batch speech summarisation for {len(next_speech_ids)} speeches"  # noqa
                )
                await self.run_next_batch_speech_summarisation(next_speech_ids)
            except Exception as e:
                self._logger.error(
                    f"error running batch speech summarisation: {e}"
                )

    async def run_next_batch_speech_summarisation(
        self, speech_ids: list[str]
    ) -> None:
        """Attempt to run a batch of speech summarisation using OpenAI batch
        API.

            This should be run when new speeches are added to the database.

        Args:
            speech_ids (list[str]): list of speech IDs to summarise
        """
        talkerlist = await self.talker_repo.list_talkers()
        speech_parts = (
            await self.speech_part_repo.list_speech_parts_by_speeches(
                speech_ids
            )
        )
        already_summarised_ids = (
            await self.speech_stats_repo.list_speech_ids_with_summary()
        )
        speeches: list[Speech] = []
        # because speech parts are returned sorted by speech_id and speech_seq
        # we can group them by speech_id in one pass
        current_speech_id: str | None = None
        current_speech_parts: list[SpeechPart] = []
        for part in speech_parts:
            if part.speech_id != current_speech_id:
                if current_speech_id is not None:
                    speeches.append(Speech(parts=current_speech_parts))
                current_speech_id = part.speech_id
                current_speech_parts = [part]
            else:
                current_speech_parts.append(part)
        if current_speech_id is not None:
            speeches.append(Speech(parts=current_speech_parts))
        speech_entries = [
            (speech.id, speech.to_string(talkerlist))
            for speech in speeches
            if speech.id not in already_summarised_ids
        ]
        openai_create_batch_file(speech_entries)
        batch_res = await openai_upload_batch_file(self.client)
        await openai_poll_batch_results(
            batch_res.id,
            self.speech_stats_repo,
            self.publisher,
            self.client,
        )
