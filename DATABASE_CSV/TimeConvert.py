###########################################################
##########        tick to time Conversion 2nd version       ##########
###########################################################
import os,sys
import pandas as pd
import glob
import numpy as np
import datetime

CHUNKSIZE = 1000 # processing 100,000 rows at a time

def GetAllFile():
    """
    Definition: Get all the file name with .csv expansion and put them into a list
	Parameters:
	Returns:
    """
    FileNameList = glob.glob("*.csv")
    return FileNameList

def convert(inputData):
        ticks = inputData
        # Change the time format from tick to ticktotime
        converted_ticks = datetime.datetime(1, 1, 1) + datetime.timedelta(microseconds = ticks/10)
        time = converted_ticks.strftime("%Y-%m-%d %H:%M:%S")
        return time

def FindTheIndex(Data, StartDate, EndDate):
    """
    Definition: Go through the csv files and convert time format for each of the csv file
	Parameters: FileName: All the csv file name on current directory
	Returns: None
    """
    Sindex = 0
    Eindex = 0

    for i in range(len(Data)):
        timestamps = convert(Data["timestamp"][i])
        # print(timestamps)
        if timestamps[0:10] == StartDate:
            Sindex = i
            break

    for j in range(Sindex,len(Data)):
        timestamps = convert(Data["timestamp"][j])
        # print(timestamps)
        if timestamps[0:10] == EndDate:
            Eindex = j
            break

    print("The index of your date range: "+Sindex,Eindex)
    return Sindex, Eindex

def ConvertFiles(FileName, StartDate, EndDate):
    """
    Definition: Go through the csv files and convert time format for each of the csv file
	Parameters: FileName: All the csv file name on current directory
	Returns: None
    """
    CsvFile = pd.read_csv(FileName,sep = ",")

    #Handle different type of Csv table structure, choose the tables have timestamp and value
    if 'timestamp'in CsvFile.columns and 'value' in CsvFile.columns:

        StartIndex,EndIndex = FindTheIndex(CsvFile, StartDate, EndDate)

        if EndIndex == 0 :
            sys.exit("The date doesnot exit or you have a wrong input ")

        NewCsv = pd.DataFrame(columns=CsvFile.columns,data=None,index = np.arange(EndIndex-StartIndex))
        NewCsvIndex = 0

        for i in range(StartIndex, EndIndex):
            time =convert(CsvFile["timestamp"][i])
            # Store into a new dataframe
            NewCsv["timestamp"][NewCsvIndex] = time
            NewCsv["value"][NewCsvIndex] =  CsvFile["value"][i]
            NewCsvIndex+=1

        print(NewCsv)
        NewCsv.to_csv("test_"+FileName,sep='\t', encoding='utf-8',index=False)

    else:
        print(CsvFile.columns)
        CsvFile.to_csv("test_"+FileName,sep='\t', encoding='utf-8',index=False)

def FileToConvert(SelectedFiles):
    """
    Definition:
	Parameters:
	Returns:
    """
    if SelectedFiles == "All":
        AllCsvFile = GetAllFile()
        return AllCsvFile
    else :
        return [SelectedFiles+".csv"]

# call the main method
def main():
    files = input("Please enter the one \"Filename\" you want to convert or \"All\" to convert all the files: ")
    print("You selected :" + str(files))

    Start = input("Please enter the start date e.g. 2018-01-01 ")

    End = input("Please enter the end date e.g. 2018-01-21 ")

    for each in FileToConvert(files):
        ConvertFiles(each,Start,End)

if __name__ == '__main__':
    main()