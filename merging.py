#merges shit
#could also be used for calculating correlations with data
import re
import sqlite3
import os
from urllib.parse import urljoin
import json
import time

#Open a Databaase cursor and connector path
def database_setup(db_name):
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path +'/'+db_name)
    cur = conn.cursor()
    return cur, conn

def join_runs_weather(cur,conn):
    cur.execute("SELECT Baseball.Date, Baseball.Total_Runs, Day_Weather.temperature, Day_Weather.windspeed, Day_Weather.precipitation_mm FROM Baseball JOIN Day_Weather ON Baseball.Date = Day_Weather.date")
    count = 0
    for date in cur:
        count -= -1
        print(date)
    print(count)
    pass

def main():
    cur, conn = database_setup("baseball_weather.db")
    join_runs_weather(cur,conn)
    pass

if __name__ == "__main__":
    main()