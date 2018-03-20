###########################################################
##########        tick to time Conversion 2nd version       ##########
###########################################################
import os,sys
import pandas as pd
import glob
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

# def TickToTime(CsvChunk,ChunkIndex):
#     """
#     Definition: Go through all the current csv chunk and return the current chunk to be appended as one file
# 	Parameters: CsvChunk: Current chunk, index [n,n+CHUNKSIZE];ChunkIndex:  starting index
# 	Returns: CSV
#     """
#     # create a new dataframe to store df chunk
#     NewCsv = pd.DataFrame(data=None, columns=CsvChunk.columns, index=CsvChunk.index)
#
#     timestamps = convert(CsvChunk["timestamp"][ChunkIndex])
#
#     # if timestamps[5:7] == '11':
#     for i in range(len(CsvChunk)):
#
#         time = convert(CsvChunk["timestamp"][i+ChunkIndex])
#
#         # Store into a new dataframe
#         NewCsv["timestamp"][i+ChunkIndex] = time
#         NewCsv["value"][i+ChunkIndex] =  CsvChunk["value"][i+ChunkIndex]
#         # CsvFile["timestamp"][i] = time
#         # print(time)
#
#     print(NewCsv)
#     print()
#     # process data frame
#     return NewCsv
#     # else:
#     #     return pd.DataFrame(columns=['timestamp','value'])

def FindTheIndex(Data, StartDate, EndDate):

    Sindex = 0
    Eindex = 0

    for each in Data:
        timestamps = convert(Data["timestamp"][Sindex])
        if timestamps[5:7] == '05':

            break


    return Sindex, Eindex

def ConvertFiles(FileName, StartDate, EndDate):
    """
    Definition: Go through the csv files and convert time format for each of the csv file
	Parameters: FileName: All the csv file name on current directory
	Returns: None
    """
    CsvFile = pd.read_csv(FileName,sep = ",")
    NewCsv = pd.DataFrame(columns=CsvFile.columns,data=None)

    #Handle different type of Csv table structure, choose the tables have timestamp and value
    if 'timestamp'in CsvFile.columns and 'value' in CsvFile.columns:
        # reader = pd.read_csv(FileName,sep = ",", chunksize=CHUNKSIZE)
        # NewCsv = pd.DataFrame(columns=CsvFile.columns,data=None)

        # chunkIndex = 0
        # for df in reader:
        #     currentIndex = CHUNKSIZE * chunkIndex
        #     NewCsv = NewCsv.append(TickToTime(df,currentIndex))
        #     chunkIndex = chunkIndex + 1

        # StartIndex
        # EndIndex

        reader = pd.read_csv(FileName,sep = ",")
        StartIndex,EndIndex = FindTheIndex(reader, StartDate, EndDate)

    #     NewCsv.to_csv("new_"+FileName,sep='\t', encoding='utf-8',index=False)
    #
    # else:
    #     print(CsvFile.columns)
    #     CsvFile.to_csv("new_"+FileName,sep='\t', encoding='utf-8',index=False)

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