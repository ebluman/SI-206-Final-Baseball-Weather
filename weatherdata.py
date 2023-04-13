import requests
import json
import re
import sqlite3
import os
import time

from urllib.parse import urljoin
# the maximum number of rows able to be returned is 16 
# since that is the max number of homes games the mets have in one season
#apiread(int, int, string (ex:"april2021"), cur, conn)
def apiread(month, year, monthyear, cur, conn):
    #cur.execute("DROP TABLE Day_Weather")
    cur.execute("CREATE TABLE IF NOT EXISTS Day_Weather (date DATE PRIMARY KEY, weather_code INT, temperature NUMERICAL, precipitation_mm NUMERICAL, windspeed NUMERICAL, winddirection_id INT)")
    data={}
    data5 = ["weathercode","temperature_2m_mean","precipitation_sum","windspeed_10m_max","winddirection_10m_dominant"]
    begin_date = str(year) + "-" + "%02d" % month + "-01"
    end_date = str(year) + "-" + '%02d' % (month+1) + "-01"
    url = f"https://archive-api.open-meteo.com/v1/archive?latitude=40.75&longitude=-73.84&start_date={begin_date}&end_date={end_date}&daily=weathercode,temperature_2m_mean,precipitation_sum,windspeed_10m_max,winddirection_10m_dominant&timezone=America%2FNew_York&temperature_unit=fahrenheit"
    info = requests.get(url)
    json_data = json.loads(info.text)

    dateNum = 0
    daily = json_data["daily"]
    for date in daily["time"]:
        if date in monthyear:
            row = []
            for datpoint in data5:
                point = daily[datpoint][dateNum]
                row.append(point)
            data[date] = row
        dateNum -= -1
    count = 0
    for i in data.keys():
        if count == 25:
            pass
        dir = ((data[i][4]+22.5) // 45) % 8
        #print(dir)
        cur.execute("INSERT OR IGNORE INTO Day_Weather (date, weather_code, temperature, precipitation_mm, windspeed, winddirection_id) VALUES (?,?,?,?,?,?)",
                    (i, data[i][0],data[i][1],data[i][2],data[i][3],dir))
        count -= -1
    conn.commit()
    pass

def wmodata(cur,conn):
    wmo = [(0,"fair"),(1,"mainly clear"),(2,"partly cloudy"),(3,"overcast"),(51,"light drizzle"),(53,"moderate drizzle"),(55,"heavy drizzle"),(61,"slight rain"),(63,"moderate rain"),(65,"heavy rain")]   
    cur.execute("CREATE TABLE IF NOT EXISTS Weather_Codes (weather_code INT PRIMARY KEY, weather_type TEXT)") 
    for tup in wmo:
        cur.execute("INSERT OR IGNORE INTO Weather_Codes (weather_code, weather_type) VALUES (?,?)",
                    (tup[0],tup[1]))
    conn.commit()
    pass

def direction(cur,conn):
    #cur.execute("DROP TABLE Weather_Directions")
    directions = [(0,"North"),(1,"Northeast"),(2,"East"),(3,"Southeast"),(4,"South"),(5,"Southwest"),(6,"West"),(7,"Northwest")]   
    cur.execute("CREATE TABLE IF NOT EXISTS Weather_Directions (winddirection_id INT PRIMARY KEY, winddirection TEXT)") 
    for tup in directions:
        cur.execute("INSERT OR IGNORE INTO Weather_Directions (winddirection_id, winddirection) VALUES (?,?)",
                    (tup[0],tup[1]))
    conn.commit()
    pass


def main():
    #2021
    april2021 = ["2021-04-08","2021-04-10","2021-04-13","2021-04-14","2021-04-23",
                 "2021-04-24","2021-04-25","2021-04-27","2021-04-28"] #9 
    may2021 = ["2021-05-07","2021-05-08","2021-05-09","2021-05-11","2021-05-12",
               "2021-05-24","2021-05-25","2021-05-27","2021-05-29"] #9 
    june2021 = ["2021-06-11","2021-06-12","2021-06-13","2021-06-14","2021-06-15",
                "2021-06-16","2021-06-17","2021-06-21","2021-06-22","2021-06-23",
                "2021-06-25","2021-06-26","2021-06-27"] #13
    july2021 = ["2021-07-05","2021-07-07","2021-07-09","2021-07-10","2021-07-11",
                "2021-07-23","2021-07-24","2021-07-25","2021-07-26","2021-07-27",
                "2021-07-28","2021-07-29","2021-07-30","2021-07-31"] #14
    august2021 = ["2021-08-01","2021-08-11","2021-08-12","2021-08-13","2021-08-14",
                  "2021-08-15","2021-08-24","2021-08-25","2021-08-26","2021-08-27",
                  "2021-08-28","2021-08-29","2021-08-31"] #13
    september2021 = ["2021-09-02","2021-09-10","2021-09-11","2021-09-12","2021-09-13",
                     "2021-09-14","2021-09-15","2021-09-17","2021-09-18","2021-09-19",
                     "2021-09-28","2021-09-29","2021-09-30"] #13
    october2021 = [] #0
    #2022
    april2022 = ["2022-04-15","2022-04-16","2022-04-17","2022-04-19","2022-04-20",
                 "2022-04-21","2022-04-29","2022-04-30"] #8
    may2022 = ["2022-05-01","2022-05-02","2022-05-03","2022-05-04","2022-05-13",
               "2022-05-14","2022-05-15","2022-05-17","2022-05-18","2022-05-19",
               "2022-05-27","2022-05-28","2022-05-29","2022-05-30","2022-05-31"] #15 
    june2022 = ["2022-06-01","2022-06-14","2022-06-15","2022-06-16","2022-06-17",
                "2022-06-18","2022-06-19","2022-06-20","2022-06-28","2022-06-29"] #10 
    july2022 = ["2022-07-01","2022-07-02","2022-07-03","2022-07-07","2022-07-08",
                "2022-07-09","2022-07-10","2022-07-22","2022-07-23","2022-07-24",
                "2022-07-26","2022-07-27"] #12 
    august2022 = ["2022-08-04","2022-08-05","2022-08-06","2022-08-07","2022-08-08",
                  "2022-08-09","2022-08-10","2022-08-12","2022-08-13","2022-08-14",
                  "2022-08-25","2022-08-26","2022-08-27","2022-08-28","2022-08-30","2022-08-31"] #16
    september2022 = ["2022-09-01","2022-09-02","2022-09-03","2022-09-04","2022-09-12",
                     "2022-09-13","2022-09-14","2022-09-15","2022-09-16","2022-09-17",
                     "2022-09-18","2022-09-27","2022-09-28"] #13
    october2022 = ["2022-10-04","2022-10-05","2022-10-07","2022-10-08","2022-10-09"] #5
    
    date_list = {2021:[april2021, may2021, june2021, july2021, august2021, september2021],
                 2022:[april2022, may2022, june2022, july2022, august2022, september2022, october2022]}
    #print(date_list)
    #data has rain delays where games were canceled
    
    #creates WMO table and direction table
    #-------------------------------------------------INPUT-------------------------------------------------------#
    #Can change name of database here
    cur, conn = open_database('Weather2.db')
    wmodata(cur,conn)
    direction(cur,conn)
    #puts single month from single year into database
    #month = 10 #did not get yet
    #year = 2022
    #monthyear = date_list[year][month-4]
    #apiread(month,year,monthyear,cur,conn)
    #-----------------------------------------------END INPUT-----------------------------------------------------#
    
    #puts all data into weather database Day_Weather in loop (still less than 25 each time)
    #cur.execute("DROP TABLE Day_Weather")
    for year in date_list:
        for month in range(4,len(date_list[year])+4):
            print("Inputing month " + str(month) + " of year " + str(year) + " weather data...")
            #monthyear = year[]
            apiread(month,year,date_list[year][month-4],cur,conn)
            time.sleep(5)
    print("Finished Database!")
    pass

def open_database(db_name):
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path+'/'+db_name)
    cur = conn.cursor()
    return cur, conn



if __name__ == "__main__":
    main()