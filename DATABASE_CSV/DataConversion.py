import MySQLdb
import pandas as pd
import numpy as np
import os,sys
import glob
import datetime
import re
import csv

def connectdb():
    print('Connecting to database...')
    # Connect to the mysqlDatabase
    db = MySQLdb.connect(host="localhost", user="root", passwd="", db="")
    print('Connected!')
    return db

# query to the database store all selected table into list
def querydb(db):
    # Create a list to store the table which will be chosen
    chosen_table = []

    cursor = db.cursor()

    sql_GetTableName = "show tables"

    # Get all the table name that is choosen
    cursor.execute(sql_GetTableName)
    for row in cursor.fetchall():
        if ("ems_values" in row[0] or "energymeters" in row[0]):
            chosen_table.append(row[0])
            print(row[0])

    for tableName in chosen_table:
        sql = "select * from ihus."+tableName
        cursor.execute(sql)

        df = pd.DataFrame(columns=['timestamp', 'value'])
        for row in cursor.fetchall():
            converted_time = convert(row[0])
            print(converted_time,row[1])
        print(df)
        df.to_csv(tableName+'.csv', sep='\t', encoding='utf-8', index=False)
        df.iloc[0:0]

def createCsvEmptyFile(db):
    chosen_table = []

    cursor = db.cursor()

    sql_GetTableName = "show tables"

    # Get all the table name that is choosen.
    cursor.execute(sql_GetTableName)
    for row in cursor.fetchall():
        if ("ems_values" in row[0] or "energymeters" in row[0]):
            chosen_table.append(row[0])
            print(row[0])
    for tableName in chosen_table:
        df = pd.DataFrame()
        df.to_csv(tableName + '.csv', sep='\t', encoding='utf-8', index=False)

def convert(inputData):
    ticks = inputData
    # Change the time format from tick to ticktotime.
    converted_ticks = datetime.datetime(1, 1, 1) + datetime.timedelta(microseconds=ticks / 10)
    time = converted_ticks.strftime("%Y-%m-%d %H:%M:%S")
    return time

def closedb(db):
    db.close()

# writeCsv is to get the data from connected mysql database and store them into csv files.
def writeCsv(db):

    chosen_table = []
    cursor = db.cursor()
    sql_GetTableName = "show tables"

    # Get all the table name that is choosen.
    cursor.execute(sql_GetTableName)
    for row in cursor.fetchall():
        # Choose the table ems_value and energymeters to convert, get all the table names from mysql db and put into a list.
        if ("ems_values" in row[0] or "energymeters" in row[0]):
            chosen_table.append(row[0])

    for tableName in chosen_table:
        sql = "select * from ihus." + tableName
        cursor.execute(sql)
        # Open the created csv file and add the column name on the each file. Directly write and read csv is much faster.
        # than use list and pandas in this case. The deficit of pandas and list is too slow.
        with open(tableName+".csv", "w") as csvfile:
            writer = csv.writer(csvfile)
            # Write the columns_name.
            writer.writerow(["timestamp", "value"])

            # Iterate through the returned object, and write the converted data in eachrow. Be aware of the difference between writerow() and writerows().
            for row in cursor.fetchall():
                writer.writerow([convert(row[0]), row[1]])

# This function is to read the converted CSV file, and show a example of its format.
def readCsv():
    with open('trend_tag_ems_values_iv_v2_pv.csv', mode='r', encoding='utf-8') as f:
        reader = csv.DictReader(f, delimiter=',')
        for row in reader:
            print([col + '=' + row[col] for col in reader.fieldnames])


# This function is to divide the whole chunk of data to weekly data.
# Parameter: StartDate (Inputed date and start to split the csv chunk from that date).
def separateByWeek(db,StartDate):
    FileNameList = glob.glob("*.csv")

    for EachFileName in FileNameList:
        EachFile = np.loadtxt(EachFileName+'', delimiter=',', dtype='str')

        # count indicate when to 'split' the weekly data.
        count = 0
        # NewListIndex is the index of the new created list.
        NewListIndex = 0
        NewList = []
        Startindex = 0

        for i in range(len(EachFile)):
            timestamps = EachFile[i][0]
            print(timestamps)
            if timestamps[0:10] == StartDate:
                Startindex = i
                break

        for i in range(Startindex+1, len(EachFile)):
            NewList.insert(NewListIndex, (EachFile[i][0], EachFile[i][1]))
            # print(NewListIndex)
            NewListIndex = NewListIndex+1

            if EachFile[i-1][0][8:10] != EachFile[i][0][8:10]:
                print(EachFile[i][0])
                count = count+1

                if count % 6 == 0:
                    with open(EachFileName + EachFile[i][0][0:10]+'.csv', "w") as myfile:
                        wr = csv.writer(myfile)
                        # writerows can only have interable input like list.
                        wr.writerows(NewList)

                    print(NewList)
                    NewList = []
                    NewListIndex = 0

def main():
    db = connectdb()

    print("Which operation would you choose:\n")
    print("1)Convert the tick format to defined time format into the csv file.\n"
          "2)Please enter a date and cut the data by week.\n"
          "3)Read a sample csv data files.\n"
          "Please input a number:")

    chosenFunction = input()
    if chosenFunction == "1":
        createCsvEmptyFile(db)
        writeCsv(db)
    elif chosenFunction == "2":
        Start = input("Please enter the start date e.g. 2018-01-01 ")
        separateByWeek(db,Start)
    elif chosenFunction == "3":
        readCsv()

    closedb(db)

if __name__ == '__main__':
    main()