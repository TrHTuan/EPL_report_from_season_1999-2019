import pandas as pd
import numpy as np
import pymysql

#Format pandas display option from float to integer
pd.options.display.float_format = '{:,.0f}'.format
pd.set_option('display.max_columns',None)

#Create a list to store dataframe (e.g: csv file)
#This is done by creating a list that would append the dataframe one by one

#Create a common file path
file_path = 'Common file path of the raw data goes here\'
#Create an empty list
list = []
#Create a function to append data frame to the list:
def create_variable(list):
    name = 0
    for i in range(1999,2020):
        name = file_path + str(i) + '_season.csv'
        # print(name)
        import_csv = pd.read_csv(r"%s"%name)
        list.append(import_csv)
# Run the function
create_variable(list)
# We now have a list of dataframe
# print(list[0])

# Create a clean up function:
def clean_up(df):
    #Create a 'Year' column based on 'Date'
    df['Year'] = df['Date'].str[-4:]
    #Split the 'FT' column into 'Team 1 score' and 'Team 2 score'
    df[['Team 1 goals','Team 2 goals']] = df['FT'].str.split('-',1,expand = True)
    #Drop the 'Round','FT' and 'Date' column
    df = df.drop(['Round','Date','FT'], axis =1, inplace = True)

# Create a function to calculate team score after each match:
def score(df):
    df['Team 1 score'] = ''
    df['Team 2 score'] = ''
    for i in range(len(df)):
        if df['Team 1 goals'][i] > df['Team 2 goals'][i]:
            df['Team 1 score'][i] = 3
            df['Team 2 score'][i] = 0
        elif df['Team 1 goals'][i] < df['Team 2 goals'][i]:
            df['Team 1 score'][i] = 0
            df['Team 2 score'][i] = 3
        elif df['Team 1 goals'][i] == df['Team 2 goals'][i]:
            df['Team 1 score'][i] = 1
            df['Team 2 score'][i] = 1

# Reorder column in dataframe:
col = ['Year','Team 1','Team 1 goals','Team 1 score','Team 2','Team 2 goals','Team 2 score']

#Create a loop to run the functions:
for i in range(len(list)):
    clean_up(list[i])
    score(list[i])
    list[i] = list[i][col]
# print()
# print(list[0])

#Connect to mysql server:
conn = pymysql.connect(database = 'epl_1', user = 'MySQL server name goes here', password = 'MySQL server password goes here')
csr = conn.cursor()

#Create multiple table in mysql server using python:
def sql_table():
    for j in range(1999,2020):
        drop_table = 'Drop table if exists season_' + str(j) +';'
        create_table = 'Create table season_' + str(j) + ' (' + \
        "year varchar(4) not null," + \
        "Team_1 varchar(100) not null," + \
        "Team_1_goals varchar(2) not null," + \
        "Team_1_score varchar(1) not null," + \
        "Team_2 varchar(100)," + \
        "Team_2_goals varchar(2) not null," + \
        "Team_2_score varchar(1) not null);"
        csr.execute(drop_table)
        csr.execute(create_table)
        conn.commit()
#Run the function to create table
sql_table()

#Create a function to transfer data to MySQL:
def insert_data(list):
    a = 1999
    while a <= 2019:
        for b in list:
            insert = 'Insert into season_' + str(a) +' values'
            for c in range(b.shape[0]):
                insert += '("'
                for d in range(b.shape[1]):
                    insert += str(b.iloc[c][d]) + '","'
                insert = insert[:-2] + "),"
            insert = insert[:-1] + ';'
            csr.execute(insert)
            conn.commit()
            a += 1
insert_data(list)

# With this function built, the only few lines of code you'd need in MySQL server include creating a database to store information
# and use that database name during the transferring process
