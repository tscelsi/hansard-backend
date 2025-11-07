from bs4 import BeautifulSoup

from hansard.entities.talker import Talker


def test_extract_talkers(sample_soup: BeautifulSoup):
    talkers = Talker.extract_talkers(sample_soup)
    assert len(talkers) == 37
