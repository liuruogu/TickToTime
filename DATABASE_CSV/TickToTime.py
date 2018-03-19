###########################################################
##########        tick to time Conversion       ##########
###########################################################
# Import necessary libraries
import os,sys
import pandas as pd
import glob
import datetime
import multiprocessing as mp

python_version = sys.version_info.major
CHUNKSIZE = 1000 # processing 100,000 rows at a time

def convert(inputData):
        ticks = inputData
        # Change the time format from tick to ticktotime
        converted_ticks = datetime.datetime(1, 1, 1) + datetime.timedelta(microseconds = ticks/10)
        time = converted_ticks.strftime("%Y-%m-%d %H:%M:%S")
        return time

def TickToTime(CsvChunk,ChunkIndex):
    """
    Definition: Go through all the current csv chunk and return the current chunk to be appended as one file
	Parameters: CsvChunk: Current chunk, index [n,n+CHUNKSIZE];ChunkIndex:  starting index
	Returns: None
    """
    # create a new dataframe to store df chunk
    NewCsv = pd.DataFrame(data=None, columns=CsvChunk.columns, index=CsvChunk.index)

    timestamps = convert(CsvChunk["timestamp"][ChunkIndex])

    if timestamps[5:7] == '11':
        for i in range(len(CsvChunk)):

            time =convert(CsvChunk["timestamp"][i+ChunkIndex])

            # Store into a new dataframe
            NewCsv["timestamp"][i+ChunkIndex] = time
            NewCsv["value"][i+ChunkIndex] =  CsvChunk["value"][i+ChunkIndex]
            # CsvFile["timestamp"][i] = time
            # print(time)

        print(NewCsv)
        print()
        # process data frame
        return NewCsv
    else:
        return pd.DataFrame(columns=['timestamp','value'])

def GetAllFile():
    # Get all the file name with .csv expansion and put them into a list
    FileNameList = glob.glob("*.csv")
    return FileNameList

def ConvertAllFiles(FileName):
    """
    Definition: Go through all the csv file and convert time format for each of the csv file
	Parameters: FileName: All the csv file name on current directory
	Returns: None
    """
    CsvFile = pd.read_csv(FileName,sep = ";")
    NewCsv = pd.DataFrame(columns=CsvFile.columns,data=None)

    #Handle different type of Csv structure, pick the tables have timestamp and value
    if 'timestamp'in CsvFile.columns and 'value' in CsvFile.columns:
        reader = pd.read_csv(FileName,sep = ";", chunksize=CHUNKSIZE)
        NewCsv = pd.DataFrame(columns=CsvFile.columns,data=None)
        # funclist = []
        chunkIndex = 0

        for df in reader:
            # Create 4 processes to execute TickToTime concurrently
            # pool = mp.Pool(4) # use 4 processes
            # f = pool.apply_async(TickToTime,[df])
            # funclist.append(f)

            currentIndex = CHUNKSIZE * chunkIndex
            NewCsv = NewCsv.append(TickToTime(df,currentIndex))
            chunkIndex = chunkIndex + 1

        NewCsv.to_csv("new_"+FileName,sep='\t', encoding='utf-8',index=False)

        # for f in funclist:
        #             NewCsv = NewCsv.append(f.get(timeout = 10), ignore_index=True)

    else:
        print(CsvFile.columns)
        CsvFile.to_csv("new_"+FileName,sep='\t', encoding='utf-8',index=False)

# call the main method
def main():
    AllCsvFile = GetAllFile()
    print(AllCsvFile)
    for EachFileName in AllCsvFile:
        ConvertAllFiles(EachFileName)


if __name__ == '__main__':
    main()