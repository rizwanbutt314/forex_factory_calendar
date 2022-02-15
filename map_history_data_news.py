from webbrowser import get
import numpy as np
import pandas as pd

from utils import (
    str_to_datetime,
    datetime_to_str,
    get_next_date,
    get_db_connection,
    update_to_db
)


news_df = pd.read_csv("output.csv")
news_df = news_df.loc[news_df['Impact'] == "High"]
news_df = news_df[["Title", "Currency", "Date"]]


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


def check_news_present_v0(titles, searches):
    results = list()
    for title in titles:
        results.extend(list(map(lambda search: search in title, searches)))

    return any(results)


def check_news(row):
    # Split symbol to get both symbols
    first_symbol, second_symbol = row["symbol"].split('_')

    # Generate next 24,48,72 dates
    date = row["date_time"]
    _date = str_to_datetime(date, format="%Y-%m-%d")
    news_24_date = datetime_to_str(get_next_date(_date, days=2))
    news_48_date = datetime_to_str(get_next_date(_date, days=3))
    news_72_date = datetime_to_str(get_next_date(_date, days=4))

    # Get searches of both symbols
    searches = CURRENCY_SEARCHES.get(
        first_symbol, []) + CURRENCY_SEARCHES.get(second_symbol, [])

    first_symbol_searches = CURRENCY_SEARCHES.get(first_symbol, [])
    second_symbol_searches = CURRENCY_SEARCHES.get(second_symbol, [])

    # Filter news dataframe against both symbols and 24/48/72 dates
    filtered_news_df = news_df.loc[(news_df['Currency'].isin([first_symbol, second_symbol])) & (
        news_df['Date'].isin([news_24_date, news_48_date, news_72_date]))]

    # Check for news_24
    data_24 = np.array(filtered_news_df.loc[filtered_news_df['Date'] == news_24_date][[
                       "Title", "Currency"]].values.tolist())
    # is_news_24, news_24_events = check_news_present(data_24, searches)

    # V2 to check for news_24
    status_24_a, news_24_a = check_news_present(data_24, first_symbol_searches, first_symbol)
    status_24_b, news_24_b = check_news_present(data_24, second_symbol_searches, second_symbol)
    is_news_24 = any([status_24_a, status_24_b])
    news_24_events = ", ".join([n for n in [news_24_a, news_24_b] if n])


    # Check for news_48
    data_48 = np.array(filtered_news_df.loc[filtered_news_df['Date'] == news_48_date][[
                       "Title", "Currency"]].values.tolist())
    # is_news_48, news_48_events = check_news_present(data_48, searches)

    # V2 to check for news_48
    status_48_a, news_48_a = check_news_present(data_48, first_symbol_searches, first_symbol)
    status_48_b, news_48_b = check_news_present(data_48, second_symbol_searches, second_symbol)
    is_news_48 = any([status_48_a, status_48_b])
    news_48_events = ", ".join([n for n in [news_48_a, news_48_b] if n])


    # Check for news_72
    data_72 = np.array(filtered_news_df.loc[filtered_news_df['Date'] == news_72_date][[
                       "Title", "Currency"]].values.tolist())
    # is_news_72, news_72_events = check_news_present(data_72, searches)

    # V2 to check for news_72
    status_72_a, news_72_a = check_news_present(data_72, first_symbol_searches, first_symbol)
    status_72_b, news_72_b = check_news_present(data_72, second_symbol_searches, second_symbol)
    is_news_72 = any([status_72_a, status_72_b])
    news_72_events = ", ".join([n for n in [news_72_a, news_72_b] if n])

    # row['news_24'] = is_news_24
    # row['news_48'] = is_news_48
    # row['news_72'] = is_news_72
    row['news_24'] = news_24_events
    row['news_48'] = news_48_events
    row['news_72'] = news_72_events

    return row


def iterate_db_results(cursor, chunk_size=10):
    'iterator using fetchmany and consumes less memory'
    while True:
        results = cursor.fetchmany(chunk_size)
        if not results:
            break

        yield results
        # for result in results:
        #     yield result

def main():
    
    table = "forex_algo_signal_day_csv"
    conn, cur = get_db_connection()
    cur.execute('select * from forex_algo_signal_day_csv')

    for results in iterate_db_results(cur):
        updated_data = list(map(check_news, results))
        update_to_db(updated_data, table=table)

    conn.close()


if __name__ == "__main__":
    main()
