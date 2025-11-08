import asyncio
import json
import logging
from typing import Any

import tiktoken
from openai import AsyncOpenAI
from openai.types.responses import ParsedResponse, ResponseTextConfigParam
from pydantic import BaseModel, Field

from hansard.entities.speech import Speech
from hansard.entities.talker import TalkerList
from hansard.nlp.enums import SpeechTone
from hansard.repositories.speech_stats_repository import (
    AbstractSpeechStatsRepository,
    SpeechStats,
)
from utils.events.local import LocalPublisher

logger = logging.getLogger(__name__)


class SpeechSummaryResult(BaseModel):
    summary: str = Field(description="A concise summary of the speech.")
    stance: float = Field(
        description="A numerical value representing the stance of the speech towards the bill being discussed. Range from -1 (strongly against) to 1 (strongly for)."
    )
    tone: list[SpeechTone]

    # additionalProperties to false
    class Config:
        extra = "forbid"


OPENAI_MODEL = "gpt-5-mini-2025-08-07"
ENCODER = tiktoken.encoding_for_model(OPENAI_MODEL)
MIN_TOKENS_FOR_SUMMARISATION = 150
MAX_TOKENS_BEFORE_TRUNCATION = 5000
BATCH_FILE_PATH = "openai_batch_file.jsonl"
BATCH_FINALISED_STATES = {"completed", "failed", "expired", "cancelled"}
BATCH_MAX_POLL_TIME_SECONDS = 86400  # 24 hours
CLIENT = AsyncOpenAI()

OPENAI_SYSTEM_PROMPT = """
You are an expert parliamentary analyst. The user will provide you with an entire, or partial transcript of a parliamentary speech that discusses a specific bill.
It could be a new bill, an amendment to an existing bill, a repeal of an existing bill, or a motion related to a bill.

The title signifies which bill is being discussed.

Your job will be to analyse the speech and provide some distilled information about it.
Pay particular attention to:
    - the main arguments made and any key points.
    - the tone of the speech, whether it is aggressive, supportive, critical etc.
    - the stance of the speech towards the bill being discussed. Provide reasoning for your assessment of their stance.

Be careful, when deciding on stance. Don't confuse support for repealing a bill or amending a bill with support for the original bill it is repealing or amending.
For example, if the speech title is 'Repeal Net Zero 2025 Bill' then you should identify stance toward the repeal bill, not the original bill.
Similarly, if the speech is discussing an amendment to an existing bill, identify stance toward the amendment, not the original bill.
"""


def openai_gen_user_prompt(speech_str: str):
    prompt = f"""Here is a parliamentary speech transcript:{speech_str}. Respond with three things:
    1. A concise summary of the speech.
    2. A numerical value representing the stance of the speech towards the bill being discussed. Range from -1 (strongly against) to 1 (strongly for).
    3. A list of tones that best describe the speech, taken from this list: {", ".join([tone.value for tone in SpeechTone])}.
    """
    return prompt


def openai_create_responses_batch_input(
    speech_id: str, speech_content: str
) -> dict[str, Any] | None:
    toks = ENCODER.encode(speech_content)
    toks_len = len(toks)
    if toks_len < MIN_TOKENS_FOR_SUMMARISATION:
        logger.error(
            f"[speech:{speech_id}] too short for summarisation ({toks_len} tokens), skipping."  # noqa
        )
        return None
    elif toks_len > MAX_TOKENS_BEFORE_TRUNCATION:
        toks = toks[:MAX_TOKENS_BEFORE_TRUNCATION]
        speech_content = ENCODER.decode(toks)
        logger.error(
            f"[speech:{speech_id}] too long for summarisation ({toks_len} tokens), truncating to {MAX_TOKENS_BEFORE_TRUNCATION} tokens."  # noqa
        )
    return {
        "custom_id": str(speech_id),
        "method": "POST",
        "url": "/v1/responses",
        "body": {
            "model": OPENAI_MODEL,
            "text": ResponseTextConfigParam(
                format={
                    "type": "json_schema",
                    "name": SpeechSummaryResult.__name__,
                    "schema": SpeechSummaryResult.model_json_schema(),
                }
            ),
            "input": openai_gen_user_prompt(speech_content),
            "reasoning": {"effort": "medium"},
        },
    }


def openai_create_batch_file(speeches: list[Speech], talkerlist: TalkerList):
    entries: list[dict[str, Any]] = []
    for speech in speeches:
        input = openai_create_responses_batch_input(
            speech.id, speech.to_string(talkerlist)
        )
        if input:
            entries.append(input)
    # save as .jsonl file
    with open(BATCH_FILE_PATH, "w") as f:
        for i, entry in enumerate(entries):
            f.write(json.dumps(entry))
            if i != len(entries) - 1:
                f.write("\n")


async def openai_upload_batch_file(client: AsyncOpenAI):
    file_res = await client.files.create(
        file=open(BATCH_FILE_PATH, "rb"), purpose="batch"
    )
    batch_res = await client.batches.create(
        input_file_id=file_res.id,
        endpoint="/v1/responses",
        completion_window="24h",
    )
    return batch_res


async def openai_save_batch_results(
    batch_id: str,
    speech_stats_repo: AbstractSpeechStatsRepository,
    client: AsyncOpenAI,
):
    batch_results = await client.batches.retrieve(batch_id=batch_id)
    assert batch_results.output_file_id is not None
    output_results = await client.files.content(
        file_id=batch_results.output_file_id
    )
    speech_ids = set[str]()
    for res in output_results.text.split("\n"):
        if res == "":
            continue
        data = json.loads(res)
        speech_id = str(data["custom_id"])
        parsed_response = ParsedResponse(**data["response"]["body"])
        # load pydantic from string
        try:
            summary_result = SpeechSummaryResult.model_validate_json(
                parsed_response.output_text
            )
        except Exception as e:
            logger.error(f"[speech:{speech_id}] error parsing summary: {e}")
            continue
        speech_stats = SpeechStats(
            speech_id=speech_id,
            summary=summary_result.summary,
            stance=summary_result.stance,
            tone=summary_result.tone,
        )
        await speech_stats_repo.upsert_speech_stats(speech_stats)
        speech_ids.add(speech_id)
        logger.info(f"[speech:{speech_id}] upserted speech stats")
    return speech_ids


async def openai_poll_batch_results(
    batch_id: str,
    speech_stats_repo: AbstractSpeechStatsRepository,
    publisher: LocalPublisher,
    client: AsyncOpenAI,
    poll_interval_seconds: int = 600,  # 10 minutes
):
    poll_start_time = asyncio.get_event_loop().time()
    while True:
        logger.info(f"[batch:{batch_id}] polling for results...")
        batch_res = await client.batches.retrieve(batch_id=batch_id)
        if batch_res.status == "completed":
            logger.info(f"[batch:{batch_id}] completed.")
            speech_ids = await openai_save_batch_results(
                batch_id, speech_stats_repo, client
            )
            publisher.publish(
                {
                    "topic": "nlp.batch_completed",
                    "batch_id": batch_id,
                    "speech_ids": list(speech_ids),
                }
            )
            break
        elif batch_res.status in BATCH_FINALISED_STATES or (
            asyncio.get_event_loop().time() - poll_start_time
            > BATCH_MAX_POLL_TIME_SECONDS
        ):
            logger.error(f"[batch:{batch_id}] failed or expired.")
            publisher.publish(
                {"topic": "nlp.batch_failed", "batch_id": batch_id}
            )
            break
        else:
            logger.info(
                f"[batch:{batch_id}] status: {batch_res.status}, polling again in {poll_interval_seconds} seconds."  # noqa
            )
            publisher.publish(
                {"topic": "nlp.batch_polling", "batch_id": batch_id}
            )
            await asyncio.sleep(poll_interval_seconds)
