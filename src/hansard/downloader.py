import logging

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.firefox.webdriver import WebDriver
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from hansard.entities.speech import HouseType
from paths import DATA_DIR

logger = logging.getLogger(__name__)
HANSARD_DIR = DATA_DIR / "hansard"


def create_driver(headless: bool = False) -> WebDriver:
    options = Options()
    options.set_preference(
        "general.useragent.override",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:143.0) Gecko/20100101 Firefox/143.0",  # noqa
    )
    if headless:
        options.add_argument("--headless")
    driver = webdriver.Firefox(options=options)
    return driver


class Downloader:
    """Taken loosely from:

    Sherratt, Tim. (2019, November 17).
    GLAM-Workbench/australian-commonwealth-hansard (Version v0.1.0).
    Zenodo. http://doi.org/10.5281/zenodo.3544706
    """

    def __init__(self, driver: WebDriver) -> None:
        self.driver = driver

    def download_hansard_xml(self, house: HouseType, url: str):
        logger.debug(f"[{house.value}, {url}] downloading")
        self.driver.get(url)
        elem = WebDriverWait(self.driver, 15).until(
            EC.element_to_be_clickable(
                (By.XPATH, "//a[.//span[contains(text(), 'View XML')]]")
            )
        )
        try:
            elem.click()
        except Exception as e:
            logger.error(f"[{house.value}, {url}] error clicking link: {e}")
            raise ValueError("Could not click View XML link") from e
        # switch to new tab
        WebDriverWait(self.driver, 10).until(EC.number_of_windows_to_be(2))
        self.driver.switch_to.window(self.driver.window_handles[-1])
        WebDriverWait(self.driver, 10).until(
            EC.presence_of_element_located((By.TAG_NAME, "hansard"))
        )
        xml_content = self.driver.page_source
        logger.debug(f"[{house.value}, {url}] downloaded")
        soup = BeautifulSoup(xml_content, "xml")
        date = soup.find("session.header").find("date").text
        # check if file already exists
        filepath = HANSARD_DIR / house.value / f"hansard-{date}.xml"
        if not filepath.exists():
            with open(filepath, "w", encoding="utf-8") as f:
                f.write(xml_content)
            logger.debug(f"[{house.value}, {url}] saved hansard-{date}.xml")
        else:
            logger.debug(
                f"[{house.value}, {url}] hansard-{date}.xml already exists, skipping save"
            )
        self.driver.close()
        self.driver.switch_to.window(self.driver.window_handles[0])
        return filepath


def download_one(url: str):
    driver = create_driver()
    downloader = Downloader(driver)
    downloader.download_hansard_xml(HouseType.HOR, url)
    driver.quit()


if __name__ == "__main__":
    # download_one(
    #     "https://www.aph.gov.au/Parliamentary_Business/Hansard/Hansard_Display?bid=chamber/hansardr/28772/&sid=0000"
    # )
    pass
