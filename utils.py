import os
import csv
import datetime

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

    mycursor = mydb.cursor()

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
            sql = f"""UPDATE {DB_TABLE} SET _time="{record['time']}", actual="{record['actual']}", forecast="{record['forecast']}", previous="{record['previous']}" WHERE title="{record['title']}" AND currency="{record['currency']}" AND impact="{record['impact']}" AND _date="{record['date']}" """
            mycursor.execute(sql)
            mydb.commit()

    # INSERT data
    sql = f"INSERT INTO {DB_TABLE} (title, currency, _date, _time, impact, actual, forecast, previous) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"

    data_to_db = [(d["title"], d["currency"], d["date"], d["time"],
                   d["impact"], d["actual"], d["forecast"], d["previous"]) for d in filtered_data]

    mycursor.executemany(sql, data_to_db)
    mydb.commit()
