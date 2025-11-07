import pandas as pd

from hansard.entities.speech import SpeechPartType
from hansard.nlp.divisiveness import get_interjection_score


def test_interjection_score():
    df = pd.DataFrame(
        {
            "speech_id": ["s1", "s1", "s1", "s1", "s2", "s2", "s3"],
            "speech_part_type": [
                SpeechPartType.SPEECH,
                SpeechPartType.INTERJECTION,
                SpeechPartType.CONTINUATION,
                SpeechPartType.INTERJECTION,
                SpeechPartType.SPEECH,
                SpeechPartType.INTERJECTION,
                SpeechPartType.SPEECH,
            ],
        }
    )
    result_df = get_interjection_score(df)
    assert result_df["interjection_score"].tolist() == [
        1.0,
        1.0,
        1.0,
        1.0,
        0.5,
        0.5,
        0.0,
    ]


def test_interjection_score_when_none():
    df = pd.DataFrame(
        {
            "speech_id": ["s1", "s2", "s3"],
            "speech_part_type": [
                SpeechPartType.SPEECH,
                SpeechPartType.SPEECH,
                SpeechPartType.SPEECH,
            ],
        }
    )
    result_df = get_interjection_score(df)
    assert result_df["interjection_score"].tolist() == [0.0, 0.0, 0.0]


def test_interjection_score_when_wrong_type():
    df = pd.DataFrame(
        {
            "speech_id": ["s1", "s2", "s3"],
            # the following should be Enum
            "speech_part_type": [
                "INTERJECTION",
                "INTERJECTION",
                "INTERJECTION",
            ],
        }
    )
    result_df = get_interjection_score(df)
    assert result_df["interjection_score"].tolist() == [0.0, 0.0, 0.0]
