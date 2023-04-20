import seaborn as sns
import pandas as pd
import json
import matplotlib.pyplot as plt

# Load the JSON data into a Python object
# Takes in filename (str) and returns data a loaded json
def open_json(filename):
    with open(filename, 'r') as f:
        data = json.load(f)
    return data


# Takes in data (read through open_json), the name of json key (str), title of graph (str)
# x-axis title (str), y-axis (str), outfile name (str)
def barplot(data, key, y_axis, title, x_title, y_title, outfile): 

    df = pd.DataFrame.from_dict(data[key], orient='index') #open a pandas df from data

    fig, ax = plt.subplots(figsize=(14, 8)) #Create axis size large enough to make plots readable

    # Create the bar plot using Seaborn
    sns.barplot(x=df.index, y=y_axis, data=df, color='lightblue', ax=ax)

    # Set the title and axis labels
    plt.title(title)
    plt.xlabel(x_title)
    plt.ylabel(y_title)

    fig.subplots_adjust(bottom=0.2) #Adjust the spacing

    # Save the plot
    plt.savefig(outfile, dpi=300)


def scatterplot(data_dict, x_var, y_var, title, x_title, y_title,  outfile):

    df = pd.DataFrame(data_dict)
    # print(df)

    # Create scatterplot using Seaborn
    sns.regplot(data=df, x=x_var, y= y_var,  line_kws={'color': 'red'})
   
    #Add labels
    plt.title(title)
    plt.xlabel(x_title)
    plt.ylabel(y_title)
   
    #Save the plot
    plt.savefig(outfile, dpi=300) 

def main():
    #Open data
    data = open_json('data.json')

    #Create Barplots
    barplot(data, 'W/L per Wind Direction', 'Winning Percentage', 'Win Percentage by Wind Direction', 'Wind Direction', 'Win Percentage', 'wind_dir_wins.jpg')
    plt.clf()
    barplot(data, "W/L per Weather Code", 'Winning Percentage', "Win Percentage by Weather Condition", "Weather Condition", "Win Percentage", "weather_condition_win.jpg")

    # Extract data from nested dictionary for the scatterplots so that it is in a readable format for the scatterplot function
    data_dict = {}
    runs_list = []
    for month in data["Average Monthly Game Stats"]:
        runs_avg = data["Average Monthly Game Stats"][month]['Average Mets Runs per Game']
        runs_list.append(runs_avg)
    # print(runs_list)
    data_dict['Mets Runs'] = runs_list

    temp_list = []
    for month in data["Average Monthly Weather Stats"]:
        temp_avg = data["Average Monthly Weather Stats"][month]["Average Temperature"]
        temp_list.append(temp_avg)
    # print(temp_list)
    data_dict['Avg Temp'] = temp_list

    windmph_list = []
    for month in data["Average Monthly Weather Stats"]:
        windmph_avg = data["Average Monthly Weather Stats"][month]["Average Max Wind Speed"]
        windmph_list.append(windmph_avg)
    data_dict["High Wind Avg"] = windmph_list

    # print(data_dict)

    #Create Scatterplots
    plt.clf()
    scatterplot(data_dict, 'Mets Runs', 'Avg Temp', "Mets Runs Scored by Temperature", "Mets Runs Scored", "Temperature", "runs_temp.jpg")
    plt.clf()
    scatterplot(data_dict, 'Mets Runs', 'High Wind Avg', "Mets Runs Scored by Wind", "Mets Runs Scored", "High Winds (mph)", "runs_winds.jpg")



if __name__ == "__main__":
    main()








    