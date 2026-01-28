# Importing the required libraries
import pandas as pd 
import numpy as np 
from bs4 import BeautifulSoup
from datetime import datetime
import xml.etree.ElementTree as ET
import requests
import sqlite3

def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''
    
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(log_file, 'a') as f:
        f.write(timestamp + ':' + message + '\n')

def extract(url, table_attribs):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''

    df = pd.DataFrame(columns=table_attribs)
    headers = {'User-Agent': 'Mozilla/5.0'}
    page = requests.get(url, headers=headers).text
    data = BeautifulSoup(page, 'html.parser')
    tables = data.find_all('tbody')
    rows = tables[0].find_all("tr")

    for row in rows:
        col = row.find_all("td")
        if len(col) >=3:
            data_dict = { 
                'Name': col[1].get_text(strip=True),
                'MC_USD_Billion': col[2].get_text(strip=True)
            }
            df1 = pd.DataFrame(data_dict, index=[0])
            df = pd.concat([df, df1], ignore_index=True)
            
    return df

def transform(df):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''
    
    rates = pd.read_csv(csv_path)
    exchange_rate = rates.set_index('Currency')['Rate'].to_dict()
    df['MC_USD_Billion'] = df['MC_USD_Billion'].str.replace(',', '').astype(float)

    for currency, rate in exchange_rate.items():
        df[f'MC_{currency}_Billion'] = [round(x * rate, 2) for x in df['MC_USD_Billion']]

    return df

def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
    df.to_csv(output_path)

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''
    
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''

    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)

''' The relevant functions in the correct order to complete the project.'''

url = 'https://web.archive.org/web/20230908091635%20/https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs = ['Name', 'MC_USD_Billion']
csv_path = 'https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv'
output_path = './Largest_banks_data.csv'
db_name = 'Banks.db'
table_name = 'Largest_banks'
log_file = 'code_log.txt'

log_progress("Start ETL")

df = extract(url, table_attribs)

log_progress("Data extraction complete. Initialing transform process")

df = transform(df)

log_progress("Transforming complete. Initialing loading file")

load_to_csv(df, output_path)

log_progress("Loading complete. Initialing loading to a db")

sql_connection = sqlite3.connect(db_name)

log_progress('SQL Connection initiated.')

load_to_db(df, sql_connection, table_name)

log_progress("Database complete. Initialing the queries")

query_statement = f"SELECT * FROM  {table_name}"
run_query(query_statement, sql_connection)

query_statement = f"SELECT AVG(MC_GBP_Billion) FROM {table_name}" 
run_query(query_statement, sql_connection)

query_statement = f"SELECT Name from {table_name} LIMIT 5"
run_query(query_statement, sql_connection)

log_progress("Process complete")

sql_connection.close()
