# Built-in libs

# Local libs
import scrape_landwatch

# 3rd party libs
import requests


def live_test(event, context):
    counter = 0
    CON_LIMIT = 25

    county = {"landwatchurl": event["landwatch"]}
    resp = requests.get(
        SCRAPERAPI_URL, {"api_key": SCRAPER_API_KEY, "url": county["landwatchurl"]}
    )
    first_page_soup = scrape_landwatch.BeautifulSoup(resp.content, "html.parser")
    county["location"] = scrape_landwatch.get_location(first_page_soup)

    # Expect county to be something like:
    # county = {
    #     "landwatchurl": "https://www.landwatch.com/Oklahoma_land_for_sale/Osage_County/Land",
    #     "location": "Osage_County-OK",
    # }

    num_of_results = scrape_landwatch.get_num_of_results(first_page_soup)

    print(f"{county['location']} Start - {num_of_results} listings")
    paginated_url_blocks = scrape_landwatch.gen_paginated_urls(
        first_page_soup, num_of_results, CON_LIMIT
    )
    soups = [first_page_soup]
    soups.extend(scrape_landwatch.asyncio.run(scrape_landwatch.get_serps_response(paginated_url_blocks)))
    for soup in soups:
        listings_soup_list = soup.select("div.result")
        for listing_soup in listings_soup_list:
            listing_dict = scrape_landwatch.listing_parser(listing_soup, county)
            scrape_landwatch.write_to_csv(listing_dict)
            counter += 1

        print(f"{county['location']} complete\nTotal listings: {counter}")


if __name__ == "__main__":
    live_test(
        {
            "starting_url": "https://www.landwatch.com/Wyoming_land_for_sale/Albany_County/Land"
        },
        None,
    )
