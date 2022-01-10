import json

import requests

from utils import (
    make_soup,
    save_data,
    remove_days_from_string,
    to_date_from_month_and_day,
    save_to_db,
    get_chrome_driver,
    wait_for_element
)

THIS_WEEK_URL = "https://www.forexfactory.com/calendar"
NEXT_WEEK_URL = "https://www.forexfactory.com/calendar?week=next"
DAILY_FX_URL = "https://www.dailyfx.com/economic-calendar#next-seven-days"


def get_daily_fx_data():
    print(f"Visting: {DAILY_FX_URL}")
    soup = make_soup(requests.get(DAILY_FX_URL).text)

    data = list()
    scripts = soup.find_all("script")
    for script in scripts:
        if "DfxEconomicCalendarPageBuilder.create(" in str(script):
            s1 = str(script).split(
                "DfxEconomicCalendarPageBuilder.create(")[1]
            s1 = s1.replace("dataProvider,", "")
            s1 = s1.split("hashes:")[0].rsplit("],")[0]
            s1 = f"{s1}]"

            data = json.loads(s1)

    all_data = list()
    for record in data:
        if record["importance"] != "high":
            continue

        date_txt = record["date"].split("T")
        date = date_txt[0]
        _time = date_txt[1]
        if record["allDayEvent"]:
            _time = "All Day"
        all_data.append({
            "title": record["title"],
            "currency": record["currency"],
            "date": date,
            "time": _time,
            "impact": record["importance"],
            "actual": record["actual"],
            "forecast": record["forecast"],
            "previous": record["previous"]
        })

    return all_data


def main():

    driver = get_chrome_driver()

    all_data = list()
    for url in [THIS_WEEK_URL, NEXT_WEEK_URL]:
        print(f"Visiting: {url}")

        driver.get(url)
        wait_for_element(
            driver, '//div[contains(@class, "calendarexports")]//a[contains(text(), "JSON")]')

        soup = make_soup(driver.page_source)
        main_table = soup.find("table", {"class": "calendar__table"})
        rows = main_table.find_all("tr", {"class": "calendar_row"})

        date_txt = ""
        time_txt = ""
        for row in rows:
            date = row.find("td", {"class": "date"}).get_text().strip()
            if date:
                date_txt = remove_days_from_string(date)

            _time = row.find("td", {"class": "time"}).get_text().strip()
            if _time:
                time_txt = _time
            currency = row.find("td", {"class": "currency"}).get_text().strip()

            impact = ""
            impact_classes = row.find("td", {"class": "impact"})["class"]
            impact_classes = " ".join(impact_classes)
            if "high" in impact_classes:
                impact = "High"
            elif "low" in impact_classes:
                impact = "Low"
            elif "medium" in impact_classes:
                impact = "Medium"
            elif "holiday" in impact_classes:
                impact = "Holiday"

            title = row.find("td", {"class": "event"}).get_text().strip()
            actual = row.find("td", {"class": "actual"}).get_text().strip()
            forecast = row.find("td", {"class": "forecast"}).get_text().strip()
            previous = row.find("td", {"class": "previous"}).get_text().strip()

            if impact == "High":
                all_data.append({
                    "title": title,
                    "currency": currency,
                    "date": to_date_from_month_and_day(date_txt),
                    "time": time_txt,
                    "impact": impact.lower(),
                    "actual": actual,
                    "forecast": forecast,
                    "previous": previous
                })

    # Get Daily FX data
    daily_fx_data = get_daily_fx_data()
    if daily_fx_data:
        all_data.extend(daily_fx_data)

    # Print pretty json
    json_formatted_str = json.dumps(all_data, indent=2)
    print(json_formatted_str)

    print("Sacing data to csv...")
    for record in all_data:
        save_data(record)

    print("Saving data to DB...")
    save_to_db(all_data)

    print("Execution Completed")


if __name__ == "__main__":
    main()

