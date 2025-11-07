from hansard.entities.speech import HouseType
from hansard.parser import Parser
from paths import TEST_DIR


def test_parse_speech_content(parser: Parser):
    speech_tag = parser.soup.find("speech")
    speech_parts = parser.parse_speech(0, speech_tag)
    assert len(speech_parts) > 0
    assert speech_parts[0].speech_content.endswith(
        "as a parliament, we honour them."
    )


def test_parse_speech_content_with_interjection(parser: Parser):
    speech_tag = parser.soup.find_all("debate")[2].find_all("speech")[1]
    assert speech_tag is not None
    assert speech_tag.find("a", {"type": "MemberInterjecting"}) is not None
    speech_parts = parser.parse_speech(0, speech_tag)
    assert len(speech_parts) > 0
    assert speech_parts[0].speech_content.endswith(
        "hey have been assessed as needing."
    )


def test_parse_speech_content_with_interjection_and_continuation(
    parser: Parser,
):
    speech_tag = parser.soup.find_all("debate")[2].find_all("speech")[2]
    assert speech_tag is not None
    assert speech_tag.find("a", {"type": "MemberInterjecting"}) is not None
    speech_parts = parser.parse_speech(0, speech_tag)
    assert len(speech_parts) == 3
    assert speech_parts[0].speech_content.endswith("They had to suffer—")
    assert speech_parts[0].part_seq == 0
    assert speech_parts[2].speech_content.startswith(
        "yes—to bring forward 20,000"
    )
    assert speech_parts[2].speech_content.endswith(
        "older people in this country."
    )


def test_parse_speech_content_no_interjection(parser: Parser):
    speech_tag = parser.soup.find("speech")
    assert speech_tag is not None
    assert speech_tag.find("a", {"type": "MemberInterjecting"}) is None
    speech = parser.parse_speech(0, speech_tag)
    assert len(speech) == 1
    assert speech[0].speech_content.endswith(
        "as a parliament, we honour them."
    )


def test_parse_speech_content_single_interjection(parser: Parser):
    speech_tag = parser.soup.find_all("debate")[2].find_all("speech")[1]
    assert speech_tag is not None
    assert speech_tag.find("a", {"type": "MemberInterjecting"}) is not None
    speech = parser.parse_speech(0, speech_tag)
    assert len(speech) == 2
    assert speech[0].type.name == "SPEECH"
    assert speech[0].speech_part_type.name == "SPEECH"
    assert speech[1].speech_part_type.name == "INTERJECTION"
    assert speech[1].speech_content.endswith(
        "required to speak to the amendments."
    )


def test_parse_speech_content_single_interjection_and_continuation(
    parser: Parser,
):
    speech_tag = parser.soup.find_all("debate")[2].find_all("speech")[2]
    assert speech_tag is not None
    assert speech_tag.find("a", {"type": "MemberInterjecting"}) is not None
    speech = parser.parse_speech(0, speech_tag)
    assert len(speech) == 3
    assert speech[0].speech_part_type.name == "SPEECH"
    assert speech[1].speech_part_type.name == "INTERJECTION"
    assert speech[2].speech_part_type.name == "CONTINUATION"


def test_parse_bill_id():
    parser = Parser(
        HouseType.HOR,
        TEST_DIR / "hansard" / "_files" / "test_two_consecutive_bills.xml",
    )
    speech_parts = parser.parse_speeches(parser.soup)
    assert len(speech_parts) == 6
    assert speech_parts[0].bill_ids == ["r7346"]
    assert speech_parts[3].bill_ids == ["r7347"]
