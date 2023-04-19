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

def json_file(filename, finaldic):
    full_path = os.path.join(os.path.dirname(__file__), filename)
    #f = open(full_path,'w')
    #file_data = f.read()
    test = {1: "one", 2: "two"}
    jstr = json.dumps(finaldic, indent=4)
    f = open(full_path, 'w')
    f.write(jstr)
    f.close()
    #json_data = json.loads(file_data)
    #return json_data
    pass

# Creates a dictionary that is organized by monthyear (ex: Apr2021) that contains inner keys of dates 
# that contain all of the information about the weather and baseball game during that day.
# INPUT: cur = ... (pointer to database)
# OUTPUT: months: dictionary described above 
# ex: {"Apr2021": {'2021-04-08': {"W/L": 'W', "Mets Runs": 3, ...}, '2021-04-10': {...}, ...}, "May2021": {...}, ...}
def dates_by_month_full_comb(cur):
    monthlis={"Apr":4,"May":5,"Jun":6,"Jul":7,"Aug":8,"Sep":9,"Oct":10}
    months={"Apr2021":{}, "May2021":{}, "Jun2021":{}, "Jul2021":{}, "Aug2021":{}, "Sep2021":{}, "Apr2022":{}, "May2022":{}, "Jun2022":{}, "Jul2022":{}, "Aug2022":{}, "Sep2022":{}, "Oct2022":{}}
    for monthyear in months:
        month = monthlis[re.findall("^...",monthyear)[0]]
        year = re.findall("20..",monthyear)[0]
        begin_date = year + "-" + "%02d" % month + "-01"
        end_date = year + "-" + '%02d' % (month+1) + "-01"
        cur.execute(f"SELECT Baseball.Date, Baseball.Win, Baseball.Runs_Scored, Baseball.Runs_Allowed, Baseball.Total_Runs, Weather_Codes.weather_type, Weather.temperature, Weather.precipitation_mm, Weather.windspeed, Weather_Directions.winddirection FROM Weather JOIN Baseball ON Baseball.Date = Weather.date AND Baseball.Date >= '{begin_date}' AND Baseball.Date < '{end_date}' LEFT JOIN Weather_codes ON Weather.weather_code = Weather_codes.weather_code LEFT JOIN Weather_Directions ON Weather.winddirection_id = Weather_Directions.winddirection_id")
        for day in cur:
            row = {"W/L":day[1], "Mets Runs": day[2], "Opponents Runs": day[3], "Total Runs": day[4], "Weather Code": day[5], "Temperature": day[6], "Precipitation(mm)": day[7], "Max Wind Speed": day[8], "Wind Direction": day[9]}
            months[monthyear][day[0]] = row
    return months

# Creates a dictionary containing the average baseball game stats of 
# Mets winning percentage, Mets runs, Opponents runs, and total games in a month
# INPUT: months: dictionary created in dates_by_month_full_comb
# OUTPUT: avg_bball_month: dictionary described above 
# ex: {"Apr2021": {"Winning Percentage": 0.6, "Mets Runs per Game": 3.0, ...}, "May2021": {...}, ...}
def avg_month_game(months):
    avg_bball_month = {}
    for month in months:
        wins = 0
        games = 0
        met_runs = 0
        op_runs = 0
        for game in months[month]:
            if months[month][game]["W/L"] == 'W':
                wins -= -1
            met_runs += months[month][game]["Mets Runs"]
            op_runs += months[month][game]["Opponents Runs"]
            games -= -1
        inHold = {}
        inHold["Winning Percentage"] = round(wins/games,3)
        inHold["Average Mets Runs per Game"] = round(met_runs/games,3)
        inHold["Average Opponents Runs per Game"] = round(op_runs/games,3)
        inHold["Total Games"] = games
        avg_bball_month[month] = inHold      
    return avg_bball_month

# Creates a dictionary containing the average weather stats of 
# Temperature (Fahrenheit), Precipitation (mm), Max Wind Speed, and total games in a month
# INPUT: months: dictionary created in dates_by_month_full_comb
# OUTPUT: avg_weather_month: dictionary described above 
# ex: {"Apr2021": {"Average Temperature": 53.26, "Average Precipitation": 3.5, ...}, "May2021": {...}, ...}
def avg_month_weather(months):
    avg_weather_month = {}
    for month in months:
        temps = 0
        precip = 0
        speed = 0
        games = 0
        for game in months[month]:
            temps += months[month][game]["Temperature"]
            precip += months[month][game]["Precipitation(mm)"]
            speed += months[month][game]["Max Wind Speed"]
            games -= -1
        inHold = {}
        inHold["Average Temperature"] = round(temps/games,3)
        inHold["Average Precipitation"] = round(precip/games,3)
        inHold["Average Max Wind Speed"] = round(speed/games,3)
        inHold["Total Games"] = games
        avg_weather_month[month] = inHold      
    return avg_weather_month

# Creates a dictionary containing the Mets winning percentage by wind direction
# INPUT: months: dictionary created in dates_by_month_full_comb
# OUTPUT: average_win: dictionary described above 
# ex: {"Southeast": {"Winning Percentage": 0.65, "Total Games": 20}, "South": {...}, ...}
def winloss_by_winddirection(months):
    avgHold = {}
    average_win ={}
    for month in months:
        for game in months[month]:
            if months[month][game]["Wind Direction"] in avgHold:
                avgHold[months[month][game]["Wind Direction"]].append((months[month][game]['W/L'],game))
            else:
                avgHold[months[month][game]["Wind Direction"]] = [(months[month][game]['W/L'],game)]
    #print(avgHold)
    for key in avgHold:
        games = 0
        wins = 0
        for game in avgHold[key]:
            if game[0] == 'W':
                wins -= -1
            games -= -1
        inHold = {}
        inHold["Winning Percentage"] = round(wins/games,3)
        inHold["Total Games"] = games
        average_win[key] = inHold
    return average_win
    pass

# Creates a dictionary containing the Mets winning percentage by weather code
# INPUT: months: dictionary created in dates_by_month_full_comb
# OUTPUT: average_win: dictionary described above 
# ex: {"partly cloudy": {"Winning Percentage": 0.375, "Total Games": 16}, "moderate drizzle": {...}, ...}
def winloss_by_weathercode(months):
    avgHold = {}
    average_win ={}
    for month in months:
        for game in months[month]:
            if months[month][game]["Weather Code"] in avgHold:
                avgHold[months[month][game]["Weather Code"]].append((months[month][game]['W/L'],game))
            else:
                avgHold[months[month][game]["Weather Code"]] = [(months[month][game]['W/L'],game)]
    #print(avgHold)
    for key in avgHold:
        games = 0
        wins = 0
        for game in avgHold[key]:
            if game[0] == 'W':
                wins -= -1
            games -= -1
        inHold = {}
        inHold["Winning Percentage"] = round(wins/games,3)
        inHold["Total Games"] = games
        average_win[key] = inHold
    return average_win
    pass

# Creates a dictionary containing the Mets winning percentage by monthyear
# INPUT: months: dictionary created in dates_by_month_full_comb
# OUTPUT: average_win: dictionary described above 
# ex: {"Apr2021": {"Winning Percentage": 0.375, "Total Games": 16}, "May2021": {...}, ...}
def winloss_by_month(months):
    average_win ={}
    for month in months:
        wins = 0
        games = 0
        for game in months[month]:
            if months[month][game]["W/L"] == 'W':
                wins -= -1
            games -= -1
        inHold = {}
        inHold["Winning Percentage"] = round(wins/games,3)
        inHold["Total Games"] = games
        average_win[month] = inHold
    return average_win
    pass


#def avg_windspeed_by_direction(wind_data):
#    avgHold = {}
#    average_speed ={}
#    for key in wind_data:
#        if wind_data[key][3] in avgHold:
#            avgHold[wind_data[key][3]].append(wind_data[key][2])
#        else:
#            avgHold[wind_data[key][3]] = [wind_data[key][2]]
#    print(avgHold)
#    for key in avgHold:
#        average_speed[key] = sum(avgHold[key]) / len(avgHold[key])
#    print(average_speed)
#    return average_speed

# Creates a dictionary containing the Mets winning percentage by weather code
# INPUTS: months: dictionary created in dates_by_month_full_comb
#         team: a string containing either "Mets","Opponents", or "Total"
# OUTPUT: average_win: dictionary described above 
# ex: given team="Mets", 
# {"partly cloudy": {"Average Mets Runs": 4.125, "Total Games": 16}, "moderate drizzle": {...}, ...}
def avg_runs_by_weathercode(months,team):
    avgHold = {}
    average_win ={}
    for month in months:
        for game in months[month]:
            if months[month][game]["Weather Code"] in avgHold:
                avgHold[months[month][game]["Weather Code"]].append((months[month][game][team + ' Runs'],game))
            else:
                avgHold[months[month][game]["Weather Code"]] = [(months[month][game][team + ' Runs'],game)]
    #print(avgHold)
    for key in avgHold:
        games = 0
        runs = 0
        for score in avgHold[key]:
            runs += score[0]
            games -= -1
        inHold = {}
        inHold["Average "+team+" Runs"] = round(runs/games,3)
        inHold["Total Games"] = games
        average_win[key] = inHold
    return average_win

# Creates a dictionary containing the Mets winning percentage by wind direction
# INPUTS: months: dictionary created in dates_by_month_full_comb
#         team: a string containing either "Mets","Opponents", or "Total"
# OUTPUT: average_win: dictionary described above 
# ex: given team="Mets", 
# {"South": {"Average Mets Runs": 4.125, "Total Games": 16}, "Southeast": {...}, ...}
def avg_runs_by_wind_direction(months,team):
    avgHold = {}
    average_win ={}
    for month in months:
        for game in months[month]:
            if months[month][game]["Wind Direction"] in avgHold:
                avgHold[months[month][game]["Wind Direction"]].append((months[month][game][team + ' Runs'],game))
            else:
                avgHold[months[month][game]["Wind Direction"]] = [(months[month][game][team + ' Runs'],game)]
    #print(avgHold)
    for key in avgHold:
        games = 0
        runs = 0
        for score in avgHold[key]:
            runs += score[0]
            games -= -1
        inHold = {}
        inHold["Average "+team+" Runs"] = round(runs/games,3)
        inHold["Total Games"] = games
        average_win[key] = inHold
    return average_win

################################ END CALCULATIONS ###############################################

#def avg_runs_by_winddirection(wind_data):
#    avgHold = {}
#    average_runs ={}
#    for key in wind_data:
#        if wind_data[key][3] in avgHold:
#            avgHold[wind_data[key][3]].append(wind_data[key][0])
#        else:
#            avgHold[wind_data[key][3]] = [wind_data[key][0]]
#    print(avgHold)
#    for key in avgHold:
#        average_runs[key] = sum(avgHold[key]) / len(avgHold[key])
#    print(average_runs)
#    return average_runs
#    pass

#plot
def score_by_maxwindspeed(months):
    games = {}
    for month in months:
        for day in months[month]:
            hold = {}
            hold[day] = [months[month][day]["Mets Runs"],months[month][day]["Opponents Runs"],months[month][day]["Max Wind Speed"]]
    pass

def main():
    cur, conn = database_setup("baseball_weather2.db")
    conn.commit()
    #wind_data = join_runs_weather(cur,conn)
    #weather_data = join_runs_weather2(cur,conn)
    #print(wind_data)
    #avgrun = avg_runs_by_winddirection(wind_data)
    #avgspeed = avg_windspeed_by_direction(wind_data)
    months = dates_by_month_full_comb(cur)
    winloss = winloss_by_winddirection(months)
    wlwthrcode = winloss_by_weathercode(months)
    wlmonth = winloss_by_month(months)
    avg_mgame = avg_month_game(months)
    avg_mweather = avg_month_weather(months)
    #Must use "Mets","Opponents", or "Total"
    team = "Mets"
    avgwcs = avg_runs_by_weathercode(months, team)
    avgwds = avg_runs_by_wind_direction(months, team)
    #for month in avg_win_score:
    #    print(avg_win_score[month]["winning percentage"])
    finaldic = {"W/L per Wind Direction" : winloss,
                "W/L per Weather Code": wlwthrcode,
                "W/L per month": wlmonth, 
                "Average Monthly Game Stats":avg_mgame,
                "Average Monthly Weather Stats": avg_mweather,
                "Average "+ team + " Runs by Weather Code": avgwcs,
                "Average "+ team + " Runs by Wind Direction": avgwds}
    #create json file
    json_file("data.json", finaldic)
    pass

if __name__ == "__main__":
    main()