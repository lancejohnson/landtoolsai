# Built-in
from pathlib import Path
import unittest

# Local imports
import scrape_landwatch

# Third party lib
from bs4 import BeautifulSoup


event = {
    "starting_url": "https://www.landwatch.com/Oklahoma_land_for_sale/Osage_County/Land"
}


class TestLocationPrep(unittest.TestCase):
    def test_get_location_zipcode(self):
        with open(Path("tests/zipcode.html")) as zipcode_html:
            first_page_soup = BeautifulSoup(zipcode_html, "html.parser")
            location = scrape_landwatch.get_location(first_page_soup)
            print(location)
            self.assertTrue(location is not None)

    def test_get_location_county(self):
        with open(Path("tests/county.html")) as county_html:
            first_page_soup = BeautifulSoup(county_html, "html.parser")
            location = scrape_landwatch.get_location(first_page_soup)
            print(location)
            self.assertTrue(location is not None)

    def test_get_location_city(self):
        with open(Path("tests/city.html")) as city_html:
            first_page_soup = BeautifulSoup(city_html, "html.parser")
            location = scrape_landwatch.get_location(first_page_soup)
            print(location)
            self.assertTrue(location is not None)

    def test_get_location_state(self):
        with open(Path("tests/state.html")) as state_html:
            first_page_soup = BeautifulSoup(state_html, "html.parser")
            location = scrape_landwatch.get_location(first_page_soup)
            print(location)
            self.assertTrue(location is not None)

    def test_get_num_of_results(self):
        with open(Path("tests/zipcode.html")) as zipcode_html:
            first_page_soup = BeautifulSoup(zipcode_html, "html.parser")
            num_of_results = scrape_landwatch.get_num_of_results(first_page_soup)
            print(f"num_of_results = {num_of_results}")
            self.assertTrue(isinstance(num_of_results, int))


if __name__ == "__main__":
    unittest.main()
