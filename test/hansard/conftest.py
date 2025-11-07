from uuid import uuid4

import pytest
from bs4 import BeautifulSoup

from hansard.entities.speech import HouseType
from hansard.parser import Parser
from paths import TEST_DIR


@pytest.fixture
def sample_soup():
    with open(
        TEST_DIR / "hansard" / "_files" / "test_hansard.xml",
        "r",
        encoding="utf-8",
    ) as f:
        xml_content = f.read()
    return BeautifulSoup(xml_content, "xml")


@pytest.fixture
def parser():
    p = Parser(
        HouseType.HOR, TEST_DIR / "hansard" / "_files" / "test_hansard.xml"
    )
    p.speech_id = uuid4()
    p.debate_category = "BILLS"
    p.subdebate_1_title = "Repeal Net Zero Bill 2025"
    return p
