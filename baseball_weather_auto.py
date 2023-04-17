# BeautifulSoup and API: Weather and Baseball
# Names: Emma Bluman & Joshua Richman
# Date: 4/16

#############################################################################################################################
# To create a new database, either get rid of count.txt or set the value in count.txt to 0
#############################################################################################################################

import requests
from bs4 import BeautifulSoup
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

# :::::::::::::::::::::::::: COUNTER FUNCTIONS :::::::::::::::::::::::::::::::::::#

# Counter functions so we don't need to type in new stuff and can do one at a time
def countmake():
    # Set the path to the file where the count will be stored
    count_file_path = "count.txt"

    # Check if the count file exists
    if os.path.exists(count_file_path):
        # If it exists, read the count from the file
        with open(count_file_path, "r") as f:
           count = int(f.read().strip())
    else:
    # If it doesn't exist, initialize the count to 0
        count = 0
    return count

def countIncrement():
    count = countmake()
    count_file_path = "count.txt"
    # Increment the count by 1
    count += 1

    # Write the updated count back to the file
    with open(count_file_path, "w") as f:
        f.write(str(count))
        
# :::::::::::::::::::::::::: COUNTER FUNCTIONS END ::::::::::::::::::::::::::::::::#


# :::::::::::::::::::::::::: Extract Baseball Data ::::::::::::::::::::::::::::::::#
def soup_database(year, month_abrev, month_id, cur, conn):
    #Create Table
    cur.execute('CREATE TABLE IF NOT EXISTS Baseball (Date DATE PRIMARY KEY, Win TEXT, Runs_Scored INT, Runs_Allowed INT, Total_Runs INT)')
    
    #Write the url, get the request, and make BeautifulSoup
    url = 'https://www.baseball-reference.com/teams/NYM/' + str(year) + '-schedule-scores.shtml' 
    # print(url)
    response = requests.get(url)
    content = response.content
    soup = BeautifulSoup(content, 'html.parser')

    #Find tags that lead to the table
    tag = soup.find('div', id = 'content')
    area = tag.find('div', id = 'all_team_schedule')
    table = area.find('table', id = 'team_schedule')
    rows = table.find_all('tr')
    month_list = []   #create list for month that will be imported in dictionary
    for row in rows:
        cells = row.find_all('td')  #extract table data
        if len(cells) > 1:
            if cells[3].text != "@":   #denotes home games if not @ a stadium
                if month_abrev in cells[0].text:
                    full_date = cells[0].text #get date
                    day_date = re.search('\d{1,2}', full_date) 
                    day = day_date.group(0)
                    if len(day) == 1:
                        day = "0" + str(day)
                    date = str(year) + "-" + str(month_id) + '-' + str(day) #put date in right format
                    month_list.append(date)
                    
                    win = cells[5].text.strip('-')[0] #Simply want win or loss do not need the wo denotation
                    runs_scored = int(cells[6].text)
                    runs_allowed = int(cells[7].text)
                    total_runs = runs_scored + runs_allowed
                    
                    #Write row of table for each date
                    cur.execute("INSERT OR IGNORE INTO Baseball (Date, Win, Runs_Scored, Runs_Allowed, Total_Runs) VALUES (?,?,?,?,?)", (date, win, runs_scored, runs_allowed, total_runs))
    conn.commit()
    
# :::::::::::::::::::::::::: Extract Baseball Data END::::::::::::::::::::::::::::::::#


# :::::::::::::::::::::::::::::::::: WEATHER API STUFF :::::::::::::::::::::::::::::::::::::::::::::::: #

#apiread(int, int, string (ex:"april2021"), cur, conn)
def weatherApi(month, year, monthyear, cur, conn):
    #cur.execute("DROP TABLE Weather")
    
    #Creates Weather table if it does not exist
    cur.execute("CREATE TABLE IF NOT EXISTS Weather (date DATE PRIMARY KEY, weather_code INT, temperature NUMERICAL, precipitation_mm NUMERICAL, windspeed NUMERICAL, winddirection_id INT)")
    
    #dictionary used to store collected data
    data={}
    
    #titles of table columns
    data5 = ["weathercode","temperature_2m_mean","precipitation_sum","windspeed_10m_max","winddirection_10m_dominant"]
    
    #creating beginning and end date strings to be used in url
    begin_date = str(year) + "-" + "%02d" % month + "-01"
    end_date = str(year) + "-" + '%02d' % (month+1) + "-01"
    url = f"https://archive-api.open-meteo.com/v1/archive?latitude=40.75&longitude=-73.84&start_date={begin_date}&end_date={end_date}&daily=weathercode,temperature_2m_mean,precipitation_sum,windspeed_10m_max,winddirection_10m_dominant&timezone=America%2FNew_York&temperature_unit=fahrenheit"
    
    #gets data from api/url
    info = requests.get(url)
    json_data = json.loads(info.text)


    #daily gets all column data being collected from the api
    daily = json_data["daily"]
    
    # for every day in the month, if the date is in the passed monthyear list that contains that months home game dates, it will
    # create a list of all of the data being collected for that date, and put that list into the data dictionary with the date as the key.
    #dateNum is used to iterate through the data in the json at a given date
    dateNum = 0
    for date in daily["time"]:
        if date in monthyear:
            row = []
            for datpoint in data5:
                point = daily[datpoint][dateNum]
                row.append(point)
            data[date] = row
        dateNum -= -1
        
    # for every key (date) in the data dictionary, make sure there are not more than 25 dates being added,
    # then change the weather direction from a 0-360 value to an id from 0-7 that aligns to N,NE, etc.
    # then insert all of the data for that date into the Weather table
    count = 0
    for i in data.keys():
        if count == 25:
            pass
        dir = ((data[i][4]+22.5) // 45) % 8
        #print(dir)
        cur.execute("INSERT OR IGNORE INTO Weather (date, weather_code, temperature, precipitation_mm, windspeed, winddirection_id) VALUES (?,?,?,?,?,?)",
                    (i, data[i][0],data[i][1],data[i][2],data[i][3],dir))
        count -= -1
    conn.commit()
    pass

# Creates the Weather_Codes table
# the information is from the website, but there was no easy way to get the words to describe the numbers without hardcoding
def wmodata(cur,conn):
    wmo = [(0,"fair"),(1,"mainly clear"),(2,"partly cloudy"),(3,"overcast"),(51,"light drizzle"),(53,"moderate drizzle"),(55,"heavy drizzle"),(61,"slight rain"),(63,"moderate rain"),(65,"heavy rain")]   
    cur.execute("CREATE TABLE IF NOT EXISTS Weather_Codes (weather_code INT PRIMARY KEY, weather_type TEXT)") 
    for tup in wmo:
        cur.execute("INSERT OR IGNORE INTO Weather_Codes (weather_code, weather_type) VALUES (?,?)",
                    (tup[0],tup[1]))
    conn.commit()
    pass

# Creates the Weather_Directions table
# there was no converted direction for wind from the website, so I implemented my own classifications for easier use of the data
def direction(cur,conn):
    #cur.execute("DROP TABLE Weather_Directions")
    directions = [(0,"North"),(1,"Northeast"),(2,"East"),(3,"Southeast"),(4,"South"),(5,"Southwest"),(6,"West"),(7,"Northwest")]   
    cur.execute("CREATE TABLE IF NOT EXISTS Weather_Directions (winddirection_id INT PRIMARY KEY, winddirection TEXT)") 
    for tup in directions:
        cur.execute("INSERT OR IGNORE INTO Weather_Directions (winddirection_id, winddirection) VALUES (?,?)",
                    (tup[0],tup[1]))
    conn.commit()
    pass

#::::::::::::::::::::::::::::: WEATHER API END :::::::::::::::::::::::::::::::::::::::#


# This function retrieves all dates in the month and year inputed that are in the Baseball table.
# shit will not work if baseball data for range is not in database first
def get_dates(month, year, cur):
        dates = []
        begin_date = str(year) + "-" + "%02d" % month + "-01"
        end_date = str(year) + "-" + '%02d' % (month+1) + "-01"
        
        cur.execute(f"SELECT Date FROM Baseball WHERE NOT Date > '{begin_date}' OR DATE < '{end_date}'")
        for date in cur:
            dates.append(date[0])
        return dates

def main():
    #creates WMO table and direction table
    curr, conn = database_setup('baseball_weather2.db')
    
    #drops all tables if needed
    #curr.execute("DROP TABLE Weather_Codes")
    #curr.execute("DROP TABLE Weather_Directions")
    #curr.execute("DROP TABLE Baseball")
    #curr.execute("DROP TABLE Weather")
    
    #creates Weather_Codes table
    wmodata(curr,conn)
    #creates Weather_directions table
    direction(curr,conn)

    #Creates table for all baseball data with less than 25 going in at a time
    #Creates table for weather data calling the same dates from baseball data
    #No games for october 2021
    
    #By using the count that is grabbed from the 'count.txt' file, this program will iterate through
    #the confusion matrix by adding a month to the Baseball table, then adding a month to the Weather table
    pointer = countmake()
    #print(pointer)
    #Not an actual confusion matrix, it just looks weird
    confusionMatrix = [('2021', "Apr", "04"),(4,2021),
                       ('2021', "May", "05"),(5,2021),
                       ('2021', "Jun", "06"),(6,2021),
                       ('2021', "Jul", "07"),(7,2021),
                       ('2021', "Aug", "08"),(8,2021),
                       ('2021', "Sep", "09"),(9,2021),
                       ('2022', "Apr", "04"),(4,2022),
                       ('2022', "May", "05"),(5,2022),
                       ('2022', "Jun", "06"),(6,2022),
                       ('2022', "Jul", "07"),(7,2022),
                       ('2022', "Aug", "08"),(8,2022),
                       ('2022', "Sep", "09"),(9,2022),
                       ('2022', "Oct", "10"),(10,2022)]
    
    #if the count/pointer is above the length of the confusion matrix, it won't break the code
    inputs = confusionMatrix[pointer % len(confusionMatrix)]
    #print(inputs)
    
    tab = ""
    month = ""
    year = ""
    #if the pointer is even, it adds to the Baseball table, otherwise it adds to the Weather Table
    #tab keeps track of which table was just added to.
    if pointer % 2 == 0:
        tab = "Baseball"
        month = inputs[2]
        year = inputs[0]
        soup_database(inputs[0], inputs[1], inputs[2], curr, conn)
    else:
        tab = "Weather"
        month = "%02d" % inputs[0]
        year = str(inputs[1])
        weatherApi(inputs[0], inputs[1], get_dates(inputs[0], inputs[1], curr),curr,conn)
    countIncrement()
    mhyr = year + "-" + month
    print("This program has now run " + str(countmake()) + " time(s). It just added or tried to add data from " + mhyr + " to the " + tab + " table.")
    
if __name__ == "__main__":
    main()


#Months must be denoted as Apr, May, Jun, Jul, Aug, Sep, Oct