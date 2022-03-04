import numpy as np
import pandas as pd
from sqlalchemy import create_engine

from utils_v2 import (
    str_to_datetime,
    datetime_to_str,
    get_next_date,
    DB_HOST,
    DB_PASSWORD,
    DB_USERNAME,
    DB_DATABASE,
    DB_TABLE
)

cnx = create_engine(
    f'mysql+pymysql://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}/{DB_DATABASE}')

news_df = pd.read_sql(f'SELECT * FROM {DB_TABLE}', cnx)
news_df = news_df.loc[news_df['impact'] == "high"]
news_df = news_df[["title", "currency", "_date"]]

CURRENCY_SEARCHES = {
    "USD": [
        "Non-Farm Employment",
        "FOMC",
        "Fed Chair",
        "CPI"
    ],
    "EUR": [
        "Monetary Policy Statement",
        "ECB Press Conference"
    ],
    "GBP": [
        "Official Bank Rate",
        "GDP"
    ],
    "JPY": [
        "BOJ Outlook Report"
    ],
    "AUD": [
        "RBA Rate Statement",
        "Employment Change",
        "Unemployment Rate"
    ],
    "NZD": [
        "Official Cash Rate",
        "GDP",
        "Employment Change"
    ],
    "CAD": [
        "Employment Change",
        "Unemployment Rate",
        "BOC Rate Statement",
        "CPI"
    ],
    "CHF": [
        "SNB Monetary Policy Assessment"
    ]
}


def check_news_present(data, searches, symbol):
    results = list()
    for d in data:
        results.extend(list(
            map(lambda search: f"({d[1]} - {d[0]})" if search in d[0] and symbol == d[1] else '', searches)))

    filtered_results = [r for r in results if r]
    return len(filtered_results) > 0, ", ".join(filtered_results)


def check_news(symbol, date_time):
    # Split symbol to get both symbols
    first_symbol, second_symbol = symbol.split('_')

    # Generate next 24,48,72 dates
    date = date_time
    _date = str_to_datetime(date, format="%Y-%m-%d")
    news_24_date = datetime_to_str(get_next_date(_date, days=2))
    news_48_date = datetime_to_str(get_next_date(_date, days=3))
    news_72_date = datetime_to_str(get_next_date(_date, days=4))

    first_symbol_searches = CURRENCY_SEARCHES.get(first_symbol, [])
    second_symbol_searches = CURRENCY_SEARCHES.get(second_symbol, [])

    # Filter news dataframe against both symbols and 24/48/72 dates
    filtered_news_df = news_df.loc[(news_df['currency'].isin([first_symbol, second_symbol])) & (
        news_df['_date'].isin([news_24_date, news_48_date, news_72_date]))]

    # Check for news_24
    data_24 = np.array(filtered_news_df.loc[filtered_news_df['_date'] == news_24_date][[
                       "title", "currency"]].values.tolist())

    # V2 to check for news_24
    status_24_a, news_24_a = check_news_present(
        data_24, first_symbol_searches, first_symbol)
    status_24_b, news_24_b = check_news_present(
        data_24, second_symbol_searches, second_symbol)
    is_news_24 = any([status_24_a, status_24_b])
    news_24_events = ", ".join([n for n in [news_24_a, news_24_b] if n])

    # Check for news_48
    data_48 = np.array(filtered_news_df.loc[filtered_news_df['_date'] == news_48_date][[
                       "title", "currency"]].values.tolist())

    # V2 to check for news_48
    status_48_a, news_48_a = check_news_present(
        data_48, first_symbol_searches, first_symbol)
    status_48_b, news_48_b = check_news_present(
        data_48, second_symbol_searches, second_symbol)
    is_news_48 = any([status_48_a, status_48_b])
    news_48_events = ", ".join([n for n in [news_48_a, news_48_b] if n])

    # Check for news_72
    data_72 = np.array(filtered_news_df.loc[filtered_news_df['_date'] == news_72_date][[
                       "title", "currency"]].values.tolist())

    # V2 to check for news_72
    status_72_a, news_72_a = check_news_present(
        data_72, first_symbol_searches, first_symbol)
    status_72_b, news_72_b = check_news_present(
        data_72, second_symbol_searches, second_symbol)
    is_news_72 = any([status_72_a, status_72_b])
    news_72_events = ", ".join([n for n in [news_72_a, news_72_b] if n])

    return news_24_events, news_48_events, news_72_events


if __name__ == "__main__":
    symbol = "GBP_AUD"
    date_time = "2022-02-14"
    news_24_events, news_48_events, news_72_events = check_news(
        symbol, date_time)

    print("1", news_24_events)
    print("2", news_48_events)
    print("3", news_72_events)
