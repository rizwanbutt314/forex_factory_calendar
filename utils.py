import os
import csv
import datetime

import pandas as pd
import mysql.connector
from bs4 import BeautifulSoup

os.environ['WDM_LOG_LEVEL'] = '0'
basepath = os.path.dirname(os.path.abspath(__file__))
filename = "output.csv"

# MYSQL Settings
DB_HOST = "localhost"
DB_USERNAME = "root"
DB_PASSWORD = "mysql123"
DB_DATABASE = "frx"
DB_TABLE = "frx_data"


def make_soup(page_source):
    soup = BeautifulSoup(page_source, "html5lib")
    return soup


def save_data(data):
    filepath = os.path.join(basepath, filename)
    if not os.path.exists(filepath):
        headings = ["Title", "Currency", "Date", "Time",
                    "Impact", "Actual", "Forecast", "Previous"]
        save_to_csv(filepath, headings)

    save_to_csv(filepath, [
        data["title"],
        data["currency"],
        data["date"],
        data["time"],
        data["impact"],
        data["actual"],
        data["forecast"],
        data["previous"],
    ])


def save_to_csv(output_file, data):
    new_data = []
    with open(output_file, 'a') as f:
        writer = csv.writer(f)
        try:
            writer.writerow(data)
        except:
            try:
                for x in data:
                    try:
                        x.encode('utf-8')
                    except:
                        try:
                            x.decode('utf-8')
                        except:
                            x = ''
                    new_data.append(x)
            except:
                pass
            new_data = [x.encode('ascii', 'replace') for x in new_data]
            writer.writerow(new_data)
            f.close()


def remove_days_from_string(text):
    days = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    for d in days:
        text = text.replace(d, "")
    return text


def to_date_from_month_and_day(input_txt):
    today = datetime.date.today()
    current_month = today.month
    current_year = today.year

    input_txt = input_txt.split(" ")
    datetime_object = datetime.datetime.strptime(input_txt[0], "%b")

    month_number = datetime_object.month
    year_number = current_year
    if month_number < current_month:
        year_number = current_year + 1

    date_time_obj = datetime.datetime.strptime(
        f"{year_number}-{month_number}-{input_txt[1]}", '%Y-%m-%d')
    formatted_date = date_time_obj.strftime('%Y-%m-%d')

    return formatted_date


def save_to_db(data):
    mydb = mysql.connector.connect(
        host=DB_HOST,
        user=DB_USERNAME,
        password=DB_PASSWORD,
        database=DB_DATABASE,
        auth_plugin='mysql_native_password'
    )

    mycursor = mydb.cursor(buffered=True)

    # Check for duplicates
    filtered_data = list()
    for record in data:
        sql = f"""SELECT * FROM {DB_TABLE} WHERE title="{record['title']}" AND currency="{record['currency']}" AND impact="{record['impact']}" AND _date="{record['date']}" """
        mycursor.execute(sql)
        myresult = mycursor.fetchone()
        if not myresult:
            filtered_data.append(record)
        else:
            # Update the existing data
            sql = f"""UPDATE {DB_TABLE} SET _time="{record['time']}", latest_release_link="{record['latest_release_link']}", actual="{record['actual']}", forecast="{record['forecast']}", previous="{record['previous']}" WHERE title="{record['title']}" AND currency="{record['currency']}" AND impact="{record['impact']}" AND _date="{record['date']}" """
            mycursor.execute(sql)
            mydb.commit()

    # INSERT data
    sql = f"INSERT INTO {DB_TABLE} (title, currency, _date, _time, impact, actual, forecast, previous, latest_release_link) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"

    data_to_db = [(d["title"], d["currency"], d["date"], d["time"],
                   d["impact"], d["actual"], d["forecast"], d["previous"], d["latest_release_link"]) for d in filtered_data]

    mycursor.executemany(sql, data_to_db)
    mydb.commit()


def generate_years_months_dates():
    all_dates = list()
    months = 12
    for year in ["2020", "2021", "2022"]:
        monthStart = pd.date_range(
            year, periods=months, freq='MS').strftime("%B %d, %Y")
        monthEnd = pd.date_range(year, periods=months,
                                 freq='M').strftime("%B %d, %Y")

        all_dates.extend([(s, e) for s, e in zip(monthStart, monthEnd)])

    return all_dates


def str_to_datetime(date_str, format="%B %d, %Y"):
    return datetime.datetime.strptime(date_str, format)


def datetime_to_str(date, format="%Y-%m-%d"):
    return date.strftime(format)


def get_next_date(date, days=1):
    return date + datetime.timedelta(days=days)


def get_today_date():
    return datetime.datetime.today()


# For data mapping
def get_db_connection():
    conn = mysql.connector.connect(
        host=DB_HOST,
        user=DB_USERNAME,
        password=DB_PASSWORD,
        database=DB_DATABASE,
        auth_plugin='mysql_native_password'
    )

    cur = conn.cursor(buffered=True, dictionary=True)
    return conn, cur


def update_to_db(data, table=""):
    conn, cur = get_db_connection()

    for record in data:
        # Update the existing data
        sql = f"""
                UPDATE {table} 
                SET news_24="{record['news_24']}", news_48="{record['news_48']}", news_72="{record['news_72']}" 
                WHERE symbol="{record['symbol']}" AND date_time="{record['date_time']}" """
        cur.execute(sql)

    conn.commit()
    conn.close()


def format_datetime_zonebased(_date, _time):
    date_time_formatted = ""
    try:
        _date = to_date_from_month_and_day(_date)
        date_time_formatted = datetime.datetime.strptime(f"{to_date_from_month_and_day(_date)} {_time}", '%Y-%m-%d %I:%M%p' if (
            'am' in _time.lower() or 'pm' in _time.lower()) else '%Y-%m-%d %H:%M')
        london_date_time = date_time_formatted + datetime.timedelta(hours=5)
        _date = datetime_to_str(london_date_time)
        _time = datetime_to_str(london_date_time, format='%I:%M%p')
    except ValueError:
        pass

    return _date, _time