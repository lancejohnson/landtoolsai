# System libs
import asyncio
import csv
from datetime import datetime
import math
import os
import re
from urllib.parse import quote

# 3rd party
import aiohttp
from bs4 import BeautifulSoup
import httpx
import requests
from tenacity import retry, stop_after_attempt

# Local


MAX_RETRIES_COUNT = 10
SCRAPER_API_KEY = os.environ.get("SCRAPER_API_KEY", "")
SCRAPERAPI_URL = "http://api.scraperapi.com"


def get_location(first_page_soup):
    """
    Parse location whatever the input.  Three expected cases are
    1) County, State 2) Zipcode 3) City, State
    """

    location_title = first_page_soup.find("h1").text
    location_clean = location_title.replace(" Land for sale :", "")
    location_formatted = location_clean.replace(", ", "-").replace(" ", "_")

    breadcrumb_links = first_page_soup.find("h2")
    try:
        breadcrumb_text = breadcrumb_links.text
    except AttributeError as e:
        print(e)

    # The double \n\n throws off the list
    breadcrumb_split = breadcrumb_text.replace("\n\n", "\n").split("\n")
    zipcode_present = breadcrumb_split[-3].isnumeric()

    if zipcode_present:
        zipcode = breadcrumb_split[-3]
        location_formatted = "-".join((location_formatted, zipcode))

    return location_formatted


def get_num_of_results(first_page_soup):
    resultscount_list_soup = first_page_soup.find("span", {"class": "resultscount"})
    if resultscount_list_soup:
        resultscount_list = resultscount_list_soup.text.split("\xa0")
        # Quest: why does this kind of soup yield characters like
        # '\xa0' when I use soup.text?
        return int(resultscount_list[5].replace(",", ""))
    else:
        return 1


def gen_paginated_urls(first_page_soup, num_of_results, CON_LIMIT):
    paginated_urls = []
    if num_of_results > 15:
        num_of_pages = math.ceil(num_of_results / 15)
        pagination_base_url = first_page_soup.find("link", {"rel": "next"})["href"][:-1]
        for i in range(2, num_of_pages + 1):
            paginated_urls.append(f"{pagination_base_url}{i}")
        paginated_urls = [
            paginated_urls[i : i + CON_LIMIT]
            for i in range(0, len(paginated_urls), CON_LIMIT)
        ]
    return paginated_urls


@retry(stop=stop_after_attempt(MAX_RETRIES_COUNT))
async def fetch(url):
    SCRAPER_API_KEY = os.environ.get("SCRAPER_API_KEY", "")
    SCRAPERAPI_URL = "http://api.scraperapi.com"
    encoded_url = quote(url)
    # Construct URL manually instead of using params because
    # aiohttp seems to have a bug quoting the URL.
    final_url = f"{SCRAPERAPI_URL}/?api_key={SCRAPER_API_KEY}&url={encoded_url}"
    async with httpx.AsyncClient() as client:
        r = await client.get(final_url)
        return r.text


async def get_serps_response(paginated_urls):
    async with aiohttp.ClientSession():
        soups = []
        for serp_url_block in paginated_urls:
            responses = await asyncio.gather(*[fetch(url) for url in serp_url_block])
            soups.extend([BeautifulSoup(resp, "html.parser") for resp in responses])
        return soups


def listing_parser(listing_soup, county):
    """This takes the soup for an individual property listing and transforms
    it into the following schema
    """
    example_dict = {  # noqa: F841
        "pid": 25009439,
        "listing_url": "https://www.landwatch.com/Coconino-County-Arizona-Land-for-sale/pid/25009439",  # noqa: E501
        "city": "Flagstaff",
        "state": "AZ",
        "price": 2800000,
        "acres": 160.00,
        "description": "JUST REDUCED $310,000! Absolutely beautiful 160 acre parcel completely surrounded by the Coconino National Forest within 2 miles of Flagstaff city limits. ... ",  # noqa: E501
        "office_name": "First United Realty, Inc.",
        "office_url": "https://www.landwatch.com/default.aspx?ct=r&type=146,157956",  # noqa: E501
        "office_status": "Signature Partner",
        "date_first_seen": "Oct 26, 2019",
        "price_per_acre": 17500.00,  # this field is calculated
    }

    listing_dict = {}
    base_url = "https://www.landwatch.com"
    listing_dict["listing_url"] = (
        base_url + listing_soup.find("div", {"class": "propName"}).find("a")["href"]
    )
    listing_dict["pid"] = int(listing_dict["listing_url"].split("/")[-1])
    try:
        acre_soup = listing_soup.find(text=re.compile(r"Acre"))
        if acre_soup:
            acres = float(acre_soup.split("Acre")[0])
            listing_dict["acres"] = acres
        else:
            listing_dict["acres"] = 1
        price_soup = listing_soup.find("div", {"class": "propName"})
        if price_soup:
            price = int(price_soup.text.split("$")[-1].strip().replace(",", ""))
            listing_dict["price"] = price
        else:
            listing_dict["price"] = 1
        listing_dict["price_per_acre"] = listing_dict["price"] / listing_dict["acres"]

        title_soup = listing_soup.find("div", {"class": "propName"})
        if title_soup:
            title_string = title_soup.text.split("$")[0].strip()
            city = re.findall(r",?[a-zA-Z][a-zA-Z0-9]*,", title_string)
            listing_dict["city"] = (
                city[0].replace(",", "") if len(city) == 2 else "CityNotPresent"
            )
        else:
            listing_dict["city"] = "NotPresent"
        description = listing_soup.find("div", {"class": "description"})
        listing_dict["description"] = (
            description.text.strip() if description else "DescNotPresent"
        )

        listing_dict["county"] = county["county"]

        office_name = listing_soup.find("a", {"class": "officename"})
        listing_dict["office_name"] = (
            office_name.text if office_name else "OfficeNameNotPresent"
        )  # noqa: E501

        office_rel_url_bs = listing_soup.find("a", {"class": "officename"})
        if office_rel_url_bs:
            office_url = base_url + office_rel_url_bs["href"]
            listing_dict["office_url"] = office_url
        else:
            listing_dict["office_url"] = "OfficeURLNotPresent"

        office_status = listing_soup.find("div", {"class": "propertyAgent"})
        listing_dict["office_status"] = (
            office_status.text.strip().split("\n")[1].strip()
            if office_status
            else "OfficeStatusBlank"
        )

        listing_dict["date_first_seen"] = datetime.now().date()
    except Exception:
        listing_dict["acres"] = "Error"
    return listing_dict


def write_to_csv(dict):
    with open("template.csv", "a") as f:
        w = csv.DictWriter(f, dict.keys())
        w.writerow(dict)


def scrape_landwatch(event, context):
    # Expect event to be something like:
    # {
    #     "starting_url": "https://www.landwatch.com/Wyoming_land_for_sale/Albany_County/Land"
    # }

    counter = 0
    CON_LIMIT = 10

    county = {"landwatchurl": event["landwatch"]}
    resp = requests.get(
        SCRAPERAPI_URL, {"api_key": SCRAPER_API_KEY, "url": county["landwatchurl"]}
    )
    first_page_soup = BeautifulSoup(resp.content, "html.parser")
    county["location"] = get_location(first_page_soup)

    # Expect county to be something like:
    # county = {
    #     "landwatchurl": "https://www.landwatch.com/Oklahoma_land_for_sale/Osage_County/Land",
    #     "location": "Osage_County-OK",
    # }

    num_of_results = get_num_of_results(first_page_soup)

    print(f"{county['location']} Start - {num_of_results} listings")
    paginated_url_blocks = gen_paginated_urls(
        first_page_soup, num_of_results, CON_LIMIT
    )
    soups = [first_page_soup]
    soups.extend(asyncio.run(get_serps_response(paginated_url_blocks)))
    for soup in soups:
        listings_soup_list = soup.select("div.result")
        for listing_soup in listings_soup_list:
            listing_dict = listing_parser(listing_soup, county)
            write_to_csv(listing_dict)
            # write_listing(listing_dict)
            counter += 1

        print(f"{county['location']} complete\nTotal listings: {counter}")
