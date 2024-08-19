import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
import numpy as np
from datetime import datetime

url = 'https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'
table_attributes = ["Country", "GDP_USD_millions"]
db_name = 'World_Economies.db'
table_name = 'Countries_by_GDP'
csv_path = './Countries_by_GDP.csv'
# Code for ETL operations on Country-GDP data

# Importing the required libraries

def extract(url, table_attributes):
    """ This function extracts the required
    information from the website and saves it to a dataframe. The
    function returns the dataframe for further processing. """

    # get webpage
    html_page = requests.get(url).text

    # parse html
    data = BeautifulSoup(html_page, 'html.parser')

    # create an empty data frame with pre-set attributes
    df = pd.DataFrame(columns=table_attributes)

    # get table and specific rows from parsed html
    table = data.find_all('tbody')
    rows = table[2].find_all('tr')

    # iterate through the rows to get specific data we need
    for row in rows:
        # find columns
        col = row.find_all('td')

        # Check if columns isn't empty, if the first columns has a hyperlink, and the value is not —
        if len(col) != 0:
            if col[0].find('a') is not None and "—" not in col[2]:

                # load data into dictionaries
                data_dict = {"Country": col[0].a.contents[0],
                             "GDP_USD_millions": col[2].contents[0]}

                # load data into dataframe
                df1 = pd.DataFrame(data_dict, index=[0])
                df = pd.concat([df, df1], ignore_index=True)


    return df

def transform(df):
    """ This function converts the GDP information from Currency
    format to float value, transforms the information of GDP from
    USD (Millions) to USD (Billions) rounding to 2 decimal places.
    The function returns the transformed dataframe."""

    GDP_list = df["GDP_USD_millions"].tolist()
    GDP_list = [float("".join(x.split(','))) for x in GDP_list]
    GDP_list = [np.round(x/1000, 2) for x in GDP_list]

    df["GDP_USD_millions"] = GDP_list
    df = df.rename(columns= {"GDP_USD_millions": "GDP_USD_billions"})

    return df

def load_to_csv(df, csv_path):
    """ This function saves the final dataframe as a `CSV` file
    in the provided path. Function returns nothing."""
    df.to_csv(csv_path)

def load_to_db(df, sql_connection, table_name):
    """ This function saves the final dataframe as a database table
    with the provided name. Function returns nothing."""
    df.to_sql(table_name, sql_connection, if_exists="replace", index=False)

def run_query(query_statement, sql_connection):
    """ This function runs the stated query on the database table and
    prints the output on the terminal. Function returns nothing. """
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)

def log_progress(message):
    """ This function logs the mentioned message at a given stage of the code execution to a log file. Function returns nothing"""
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestamp_format = now.strftime(timestamp_format)
    with open("./etl_project_log.txt", "a") as file:
        file.write(timestamp_format + ": " + message + "\n")

''' Here, you define the required entities and call the relevant 
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''
# Extraction of data
log_progress("Preliminaries complete. Initiating ETL process.:")
extracted_data = extract(url, table_attributes)
log_progress("Data extraction complete. Initiating Transformation process.")

# Transformation of data
extracted_data = transform(extracted_data)
log_progress("Data transformation complete. Initiating loading process.")

# Loading data into a csv file
load_to_csv(extracted_data, csv_path)
log_progress("Data saved to CSV file.")

# initializing sql connection
sql_connection = sqlite3.connect(db_name)
log_progress("SQL Connection initiated.")

# Load data to database
load_to_db(extracted_data, sql_connection, table_name)
log_progress("Data loaded to Database as table. Running the query.")

# Check to see if data was loaded
sql_query = f"SELECT * from {table_name} WHERE GDP_USD_billions >= 100"
run_query(sql_query, sql_connection)

log_progress("Process Complete.")

sql_connection.close()