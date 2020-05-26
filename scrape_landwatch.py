# System libs
import asyncio
import csv
from datetime import datetime, date
import io
import logging
import math
import os
import re

# 3rd party
from asyncio_pool import AioPool
from bs4 import BeautifulSoup
import httpx
from tenacity import retry, stop_after_attempt

# Local


logger = logging.getLogger()
logger.setLevel(logging.INFO)
MAX_RETRIES_COUNT = 10


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


def gen_paginated_urls(first_page_soup, num_of_results):
    paginated_urls = []
    if num_of_results > 15:
        num_of_pages = math.ceil(num_of_results / 15)
        pagination_base_url = first_page_soup.find("link", {"rel": "next"})["href"][:-1]
        for i in range(2, num_of_pages + 1):
            paginated_urls.append(f"{pagination_base_url}{i}")
    return paginated_urls


async def fetch_urls(*, urls, con_limit, tag_check, dict_check, proxies):
    """
    :param proxies: Options are luminati, crawlera, scraperapi
    """

    @retry(stop=stop_after_attempt(MAX_RETRIES_COUNT))
    async def fetch_url(url, retries=MAX_RETRIES_COUNT):
        if proxies == "scraperapi":
            SCRAPER_API_KEY = os.environ.get("SCRAPER_API_KEY", "")
            SCRAPERAPI_URL = "https://api.scraperapi.com"
            params = {
                "api_key": SCRAPER_API_KEY,
                "url": url,
            }
            logging.info(f"Start page fetch for {url}")
            async with httpx.AsyncClient() as client:
                resp = await client.get(SCRAPERAPI_URL, timeout=60, params=params)
        elif proxies == "crawlera":
            CRAWLERA_API_KEY = os.environ.get("crawleraAPIKey", "")

            proxy = {
                "http": f"http://{CRAWLERA_API_KEY}:@proxy.crawlera.com:8010/",
                "https": f"http://{CRAWLERA_API_KEY}:@proxy.crawlera.com:8010/",
            }

            headers = {"X-Crawlera-Profile": "desktop"}
            logging.info(f"Start page fetch for {url}")

            async with httpx.AsyncClient(
                headers=headers, proxies=proxy, verify=False
            ) as client:
                resp = await client.get(url, timeout=60)
        elif proxies == "luminati":
            LUMINATI_CUSTOMER_ID = os.environ.get("LUMINATI_CUSTOMER_ID", "")
            LUMINATI_DEFAULT_ZONE = os.environ.get("LUMINATI_DEFAULT_ZONE", "")
            LUMINATI_PASSWORD = os.environ.get("LUMINATI_PASSWORD", "")

            proxy = {
                "http": f"http://lum-customer-{LUMINATI_CUSTOMER_ID}-zone-{LUMINATI_DEFAULT_ZONE}-country-us:{LUMINATI_PASSWORD}@zproxy.lum-superproxy.io:22225",  # noqa:E501
                "https": f"http://lum-customer-{LUMINATI_CUSTOMER_ID}-zone-{LUMINATI_DEFAULT_ZONE}-country-us:{LUMINATI_PASSWORD}@zproxy.lum-superproxy.io:22225",  # noqa:E501
            }

            headers = {
                "Origin": "https://www.bing.com",
                "Referer": "https://www.bing.com",
                "Accept": "test/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",  # noqa:E501
                "Accept-Language": "en-US,en;q=0.9",
                "Accept-Encoding": "gzip, deflate, br",
            }
            logging.info(f"Start page fetch for #{urls.index(url)} {url}")
            async with httpx.AsyncClient(
                headers=headers, proxies=proxy, verify=False
            ) as client:
                resp = await client.get(url, timeout=60)
                logging.info(f"Response received for #{urls.index(url)} {url}")
        test_soup = BeautifulSoup(resp.text, "html.parser")
        if test_soup.find(tag_check, dict_check):
            return resp.text
        else:
            print(
                f"Soup test failed for #{urls.index(url)} {url}. {retries} retries remaining"
            )
            if retries > 0:
                retries -= 1
                raise ValueError(f"Soup test failed.  {retries} retries remaining")

    pool = AioPool(size=con_limit)
    page_htmls = await pool.map(fetch_url, urls)
    return page_htmls


def convert_resps_to_soups(htmls):
    soups = []
    for html in htmls:
        soups.extend([BeautifulSoup(html, "html.parser")])

    return soups


def listing_parser(listing_soup, location):
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
        # import pdb; pdb.set_trace()
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

        listing_dict["location"] = location["location"]

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
    except Exception as e:
        logging.ERROR(f"Error is {e}")
        listing_dict["acres"] = "Error"
    return listing_dict


def write_to_csv_in_buffer(dicts):
    fieldnames = list(dicts[0].keys())
    output_buffer = io.StringIO()
    writer = csv.DictWriter(output_buffer, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(dicts)

    return output_buffer


def upload_csv_to_s3(*, in_mem_csv, location, BUCKET):
    # Used this StackOverflow answer
    # https://stackoverflow.com/questions/45699905/csv-file-upload-from-buffer-to-s3

    today_str = str(date.today())
    s3_csv_key = f"{today_str}-{location['location']}.csv"
    csv_as_bytes = io.BytesIO(in_mem_csv.getvalue().encode())

    # AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID", "")
    # AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "")

    s3 = boto3.client(
        "s3"  # ,
        # aws_access_key_id=AWS_ACCESS_KEY_ID,
        # aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )
    s3.put_object(
        Bucket=BUCKET,
        Key=s3_csv_key,
        Body=csv_as_bytes,
        ContentType="text/csv",
        ACL="public-read",
    )

    # Example finished AWS S3 URL
    # https://giftsondemand.s3.amazonaws.com/2020-01-07/10_r.png
    aws_url = f"https://{BUCKET}.s3.amazonaws.com/{s3_csv_key}"

    return aws_url


def scrape_landwatch(event, context):
    # Expect event to be something like:
    # {
    #     "landwatch_url": "https://www.landwatch.com/Oklahoma_land_for_sale/Osage_County/Land"
    # }

    counter = 0
    CON_LIMIT = 10

    location = {"landwatchurl": event["starting_url"]}
    resp_texts = asyncio.run(
        fetch_urls(
            urls=[location["landwatchurl"]],
            con_limit=CON_LIMIT,
            tag_check="div",
            dict_check={"class": "resultstitle"},
            proxies="luminati",
        )
    )
    selected_resp = resp_texts[0]
    first_page_soup = BeautifulSoup(selected_resp, "html.parser")
    location["location"] = get_location(first_page_soup)

    # Expect location to be something like:
    # location = {
    #     "landwatchurl": "https://www.landwatch.com/Oklahoma_land_for_sale/Osage_County/Land",
    #     "location": "Osage_County-OK",
    # }

    num_of_results = get_num_of_results(first_page_soup)

    print(f"{location['location']} Start - {num_of_results} listings")
    paginated_urls = gen_paginated_urls(first_page_soup, num_of_results)

    page_htmls = asyncio.run(
        fetch_urls(
            urls=paginated_urls,
            con_limit=CON_LIMIT,
            tag_check="div",
            dict_check={"class": "resultstitle"},
            proxies="luminati",
        )
    )
    soups = [first_page_soup]
    soups.extend(convert_resps_to_soups(page_htmls))

    listings = []
    for soup in soups:
        listings_soup_list = soup.select("div.result")
        for listing_soup in listings_soup_list:
            listing_dict = listing_parser(listing_soup, location)
            listings.append(listing_dict)
            counter += 1

        print(
            f"{location['location']} Part {soups.index(soup)} complete\nTotal listings: {counter}"
        )

    csv_in_buffer = write_to_csv_in_buffer(listings)
    csv_url = upload_csv_to_s3(
        in_mem_csv=csv_in_buffer, location=location, BUCKET=event["bucket"]
    )

    return csv_url


if __name__ == "__main__":
    event = {
        "starting_url": "https://www.landwatch.com/Oklahoma_land_for_sale/Osage_County/Land",
        "bucket": "landtoolsai",
    }
    print(scrape_landwatch(event, None))
