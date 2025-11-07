from typing import cast

import pandas as pd
from textblob import TextBlob

from hansard.entities.speech import SpeechPartType


def count_interjections(df: pd.DataFrame):
    df["speech_interjection_count"] = len(
        df[df.speech_part_type == SpeechPartType.INTERJECTION]
    )
    return df


def get_interjection_score(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate an interjection score for each speech.

    The interjection score is normalised between 0 and 1 and represents the
    frequency of interjections within the speech. The higher the score, the
    more interjections.
    """
    df = df.groupby("speech_id", group_keys=False).apply(count_interjections)
    df["max_interjection_count"] = df["speech_interjection_count"].max()
    df["min_interjection_count"] = df["speech_interjection_count"].min()
    df["interjection_score"] = (
        df["speech_interjection_count"] - df["min_interjection_count"]
    ) / (df["max_interjection_count"] - df["min_interjection_count"])
    return df


def get_speech_length(speech_parts: pd.DataFrame) -> pd.DataFrame:
    """Calculate the length of each speech in words, without interjections by
    others."""
    speech_parts["speech_length"] = speech_parts.loc[
        :, "speech_part_length"
    ].sum()
    return speech_parts


def get_speech_divisiveness(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate a divisiveness score for a speech.

    Interjection score = I (0 to 1)
    Sentiment = S (-1 to 1)

    Divisiveness for speech x = l_norm(I - (sum(S) / n))
    """
    l_norm = df["length_norm"].max()
    i = df["interjection_score"].max()
    s = df["sentiment"].mean()
    df["divisiveness"] = l_norm * (i - s)
    return df


def get_word_count_and_sentiment(text: str) -> tuple[int, float | None]:
    blob = TextBlob(text)
    word_count = len(blob.words)
    sentiment = cast(float, blob.sentiment.polarity)
    return word_count, sentiment


def calculate_divisiveness(df: pd.DataFrame) -> pd.DataFrame:
    df = get_interjection_score(df)
    # we can now remove interjections from the dataframe
    df = df[
        df.speech_part_type.isin(
            [SpeechPartType.SPEECH, SpeechPartType.CONTINUATION]
        )
    ]
    df.loc[:, ["speech_part_length", "sentiment"]] = (
        df["speech_content"].apply(get_word_count_and_sentiment).tolist()
    )
    df = df.groupby("speech_id", group_keys=False).apply(get_speech_length)
    # remove small speech parts so their sentiment calculation isn't used.
    df = df[df.speech_part_length > 100]
    df["max_speech_length"] = df["speech_length"].max()
    df["min_speech_length"] = df["speech_length"].min()
    df["length_norm"] = (df["speech_length"] - df["min_speech_length"]) / (
        df["max_speech_length"] - df["min_speech_length"]
    )
    df = df.groupby("speech_id").apply(get_speech_divisiveness)
    df = df.reset_index(drop=True)
    return df
