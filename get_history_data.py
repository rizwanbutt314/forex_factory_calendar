import time
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from utils import (
    make_soup,
    save_data,
    remove_days_from_string,
    to_date_from_month_and_day,
    save_to_db,
    generate_years_months_dates,
    str_to_datetime,
    get_today_date
)

THIS_WEEK_URL = "https://www.forexfactory.com/calendar"


def scrape_calendar_page(driver, start_date):
    _start_date = str_to_datetime(start_date)

    soup = make_soup(driver.page_source)

    rows = soup.find_all("tr", {"class": "calendar_row"})
    print("Total: ", len(rows))

    date_txt = ""
    all_data = list()
    for row in rows:
        impact = ""

        # Title
        title = row.find("td", {"class": "event"}).find(
            "span").get_text().strip()

        # Date
        try:
            date = row.find("td", {"class": "date"}).find(
                "span", {"class": "date"}).get_text()
            date_txt = date.strip()
            _day = date_txt.split(" ")[-1]
            date_txt = str_to_datetime(f"{_start_date.year}-{_start_date.month}-{_day}", format="%Y-%m-%d").date()
        except:
            date = ""

        # Time
        _time = row.find("td", {"class": "time"}).get_text().strip()

        # Currency
        currency = row.find("td", {"class": "currency"}).get_text().strip()

        # Impact
        impact_classes = row.find("td", {"class": "impact"})["class"]
        impact_classes = " ".join(impact_classes)
        if "high" in impact_classes:
            impact = "High"
        elif "low" in impact_classes:
            impact = "Low"
        elif "medium" in impact_classes:
            impact = "Medium"

        # Actual value
        try:
            actual = row.find("td", {"class": "actual"}).get_text().strip()
        except:
            actual = ""

        # Forecast value
        try:
            forecast = row.find("td", {"class": "forecast"}).get_text().strip()
        except:
            forecast = ""
        
        # Previous value
        try:
            previous = row.find("td", {"class": "previous"}).find(
                "span").get_text().strip()
        except:
            previous = ""

        all_data.append({
            "title": title,
            "currency": currency,
            "time": _time,
            "date": date_txt,
            "impact": impact,
            "actual": actual,
            "forecast": forecast,
            "previous": previous
        })

    for record in all_data:
        save_data(record)


def main():
    today_date = get_today_date()
    current_month_number = today_date.month
    
    driver = uc.Chrome()
    driver.get('https://www.forexfactory.com/calendar')
    time.sleep(5)

    all_dates = generate_years_months_dates()
    all_dates = all_dates[:24+current_month_number]
    for date_range in all_dates:
        start, end = date_range
        print(start, end)

        # Click to open Calendar filter
        calendar_btn = driver.find_element(By.XPATH, '//li[contains(@class, "calendar__options")]//a')
        calendar_btn.click()
        time.sleep(3)

        # Enter Start date
        start_date_field = driver.find_element(By.XPATH, '//input[@data-container="Calendar_mainCal_begindate"]')
        start_date_field.clear()
        start_date_field.send_keys(start)

        # Enter End date
        end_date_field = driver.find_element(By.XPATH, '//input[@data-container="Calendar_mainCal_enddate"]')
        end_date_field.clear()
        end_date_field.send_keys(end)

        # Click on Apply Filter button
        apply_filters_btn = driver.find_element(By.XPATH, '//input[@name="flexSettings"]')
        apply_filters_btn.click()
        time.sleep(5)

        scrape_calendar_page(driver, start)

        # break


if __name__ == "__main__":
    main()
