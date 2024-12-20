### Step 1: Import Required Libraries and Define Functions
import os
import shutil #deleting directories
import pandas as pd
from intradaydata import Intraday

def save_data(return_interval, IntradayObject, mysymboldict):
    start_date=IntradayObject.start_intraday
    end_date=IntradayObject.end_intraday
    alldatadict=IntradayObject.fetch_data_yfinance(specific_tickers=IntradayObject.tickers) #Get dictionary of specific intraday data that we want to store

    ### Step 4: In the "temp" folder, merge the new data with old data (old data is present in "Intraday_data_files")
    for key in alldatadict.keys():
        symbol=mysymboldict[key][0]
        newcsv=alldatadict[key]
        newcsv.drop_duplicates(inplace=True)
        newcsv.dropna(inplace=True)
        if (newcsv.index.to_list())!=[]:
            newstart=str(newcsv.index.to_list()[0])[:10]
            newend=str(newcsv.index.to_list()[-1])[:10]
            start_end_date=f'Intraday_{symbol}_{newstart}_to_{newend}.csv'
            alldatadict[key].to_csv(os.path.join(Daily_backup_files,start_end_date))# Save the new data file into "Daily_backup_files" folder
            print(f'New data fetched for {symbol}: {start_end_date}')
        else:
            print(f'No new data fetched for {symbol}')
            newcsv=pd.DataFrame()
    
        oldcsv=pd.DataFrame()
        flag=0
        for entry2 in os.scandir(Intraday_data_files):
            if entry2.is_file() and entry2.name.endswith('.csv'):
                oldcsvpath=os.path.join(Intraday_data_files,entry2.name)
                [_,_,ticker,interval,oldstart,_,oldend]=(entry2.name.replace('.csv',"")).split('_')
                if ticker==symbol and interval==return_interval:
                    oldcsvpath=os.path.join(Intraday_data_files,entry2.name)
                    oldcsv=pd.read_csv(oldcsvpath)
                    if 'Datetime' in list(oldcsv.columns):#index is in 0...... and not Datetime format->cause error in merging
                        oldcsv.index.name='Datetime'
                        oldcsv.columns.name='Price'
                        oldcsv.index=oldcsv['Datetime']
                        oldcsv.drop(columns=['Datetime'],axis=1,inplace=True)
                    flag=1
                    break
            if flag==0:
                print(f'Historical data for {symbol} not found.')
    
        if newcsv.empty and oldcsv.empty:
            print(f"No data available for {symbol}. Both old and new data are empty.")
            finalcsv = pd.DataFrame()  # Create an empty DataFrame
        
        elif newcsv.empty:
            print(f"No new data fetched for {symbol}. Using only historical data.")
            finalcsv = oldcsv.copy()  # Use only the historical data
    
        elif oldcsv.empty:
            print(f"No historical data found for {symbol}. Using only new data.")
            finalcsv = newcsv.copy()  # Use only the new data
    
        else:
            finalcsv = pd.concat([oldcsv,newcsv])
    
        finalcsv.drop_duplicates(inplace=True)
        finalcsv.dropna(inplace=True) 
        finalcsv.index = pd.to_datetime(finalcsv.index)
        finalcsv.sort_index(inplace=True)
        finalstart=str(finalcsv.index.to_list()[0])[:10]
        finalend=str(finalcsv.index.to_list()[-1])[:10]
        finalpath=os.path.join('temp',f'Intraday_data_{symbol}_{return_interval}_{finalstart}_to_{finalend}.csv')
        finalcsv.to_csv(finalpath,index=True)
        print(finalcsv)


### Step 2: Make Folders to Store Data
Intraday_data_files = "Intraday_data_files" # Read current dataset of historical data
os.makedirs(Intraday_data_files, exist_ok=True)
Daily_backup_files="Daily_backup_files"     # Store daily data for all tickers as backup
os.makedirs(Daily_backup_files, exist_ok=True)
os.makedirs('temp',exist_ok=True) # Temporary file to hold new Intraday data. Later gets renamed to "Intraday_data_files" after new and old data gets Merged


### Step 3.1: Set up Tickers and Fetch data from Yahoo Finance for '1m'
return_interval='1m'
IntradayObject1m=Intraday(start_intraday=3,end_intraday=0,interval=return_interval)# If start_intraday=end_intraday=-1, code fetches historical data till latest timestamp.
mysymboldict={
    "ZN=F":["ZN","10-Year T-Note Futures"],
    "ZB=F":["ZB","30-Year T-Bond Futures"],
    "ZF=F":["ZF","5-Year US T-Note Futures"],
    "ZT=F":["ZT","2-Year US T-Note Futures"],
    "DX-Y.NYB":["DXY","US Dollar Index"],
    "CL=F":["CL","Crude Oil futures"],
    "GC=F":["GC","Gold futures"],
    "NQ=F":["NQ","Nasdaq 100 futures"],
    "^DJI":["DJI","Dow Jones Industrial Average"],
    "^GSPC":["GSPC","S&P 500"]
    }
IntradayObject1m.update_dict_symbols(mysymboldict)

# start_date=IntradayObject1m.start_intraday
# end_date=IntradayObject1m.end_intraday
#alldatadict=IntradayObject1m.fetch_data_yfinance(specific_tickers=IntradayObject1m.tickers) #Get dictionary of specific intraday data that we want to store

### Step 4: In the "temp" folder, merge the new data with old data (old data is present in "Intraday_data_files")
save_data(return_interval, IntradayObject1m, mysymboldict)# start_date, end_date, alldatadict)

### Step 3.2: Set up Tickers and Fetch data from Yahoo Finance for '1h'
return_interval='1h'
IntradayObject1h=Intraday(interval=return_interval,start_intraday=720,end_intraday=0)# If start_intraday=end_intraday=-1, code fetches historical data till latest timestamp.
mysymboldict={
    "ZN=F":["ZN","10-Year T-Note Futures"]
    }
IntradayObject1h.update_dict_symbols(mysymboldict)

# start_date=IntradayObject1h.start_intraday
# end_date=IntradayObject1h.end_intraday
#alldatadict=IntradayObject1h.fetch_data_yfinance(specific_tickers=IntradayObject1m.tickers) #Get dictionary of specific intraday data that we want to store


### Step 4: In the "temp" folder, merge the new data with old data (old data is present in "Intraday_data_files")
save_data(return_interval, IntradayObject1h, mysymboldict)# start_date, end_date, alldatadict)

'''
for key in alldatadict.keys():
    symbol=mysymboldict[key][0]
    newcsv=alldatadict[key]
    newcsv.drop_duplicates(inplace=True)
    newcsv.dropna(inplace=True)
    if (newcsv.index.to_list())!=[]:
        newstart=str(newcsv.index.to_list()[0])[:10]
        newend=str(newcsv.index.to_list()[-1])[:10]
        start_end_date=f'Intraday_{symbol}_{newstart}_to_{newend}.csv'
        alldatadict[key].to_csv(os.path.join(Daily_backup_files,start_end_date))# Save the new data file into "Daily_backup_files" folder
    else:
        print(f'No new data fetched for {symbol}')
        newcsv=pd.DataFrame()

    oldcsv=pd.DataFrame()
    flag=0
    for entry2 in os.scandir(Intraday_data_files):
        if entry2.is_file() and entry2.name.endswith('.csv'):
            oldcsvpath=os.path.join(Intraday_data_files,entry2.name)
            [_,_,ticker,interval,oldstart,_,oldend]=(entry2.name.replace('.csv',"")).split('_')
            if ticker==symbol and interval==return_interval:
                oldcsvpath=os.path.join(Intraday_data_files,entry2.name)
                oldcsv=pd.read_csv(oldcsvpath)
                if 'Datetime' in list(oldcsv.columns):#index is in 0...... and not Datetime format->cause error in merging
                    oldcsv.index.name='Datetime'
                    oldcsv.columns.name='Price'
                    oldcsv.index=oldcsv['Datetime']
                    oldcsv.drop(columns=['Datetime'],axis=1,inplace=True)
                flag=1
                break
        if flag==0:
            print(f'Historical data for {symbol} not found.')

    if newcsv.empty and oldcsv.empty:
        print(f"No data available for {symbol}. Both old and new data are empty.")
        finalcsv = pd.DataFrame()  # Create an empty DataFrame
    
    elif newcsv.empty:
        print(f"No new data fetched for {symbol}. Using only historical data.")
        finalcsv = oldcsv.copy()  # Use only the historical data

    elif oldcsv.empty:
        print(f"No historical data found for {symbol}. Using only new data.")
        finalcsv = newcsv.copy()  # Use only the new data

    else:
        finalcsv = pd.concat([oldcsv,newcsv])

    finalcsv.drop_duplicates(inplace=True)
    finalcsv.dropna(inplace=True) 
    finalcsv.index = pd.to_datetime(finalcsv.index)
    finalcsv.sort_index(inplace=True)
    finalstart=str(finalcsv.index.to_list()[0])[:10]
    finalend=str(finalcsv.index.to_list()[-1])[:10]
    finalpath=os.path.join('temp',f'Intraday_data_{symbol}_{return_interval}_{finalstart}_to_{finalend}.csv')
    finalcsv.to_csv(finalpath,index=True)
    print(finalcsv)
'''

### Step 5: Delete the "Intraday_data_files directory" and rename "temp" as "Intraday_data_files directory"
#Delete "Intraday_data_files directory"
directory_path = Intraday_data_files
try:
    shutil.rmtree(directory_path)
    print(f"Directory {directory_path} and its contents deleted successfully.")
except FileNotFoundError:
    print("The directory does not exist.")
except PermissionError:
    print("You do not have the necessary permissions to delete this directory.")

#Rename "temp" as "Intraday_data_files directory" 
current_name = "temp"
new_name = "Intraday_data_files"

try:
    os.rename(current_name, new_name)
    print(f"Directory renamed from '{current_name}' to '{new_name}'")
except FileNotFoundError:
    print(f"Directory '{current_name}' not found!")
except PermissionError:
    print("You do not have permission to rename this directory.")
except Exception as e:
    print(f"An error occurred: {e}")




# ### Step 1: Import Required Libraries and Define Functions
# import os
# import shutil #deleting directories
# import pandas as pd
# from intradaydata import Intraday

# ### Step 2: Make Folders to Store Data
# Intraday_data_files = "Intraday_data_files" # Read current dataset of historical data
# os.makedirs(Intraday_data_files, exist_ok=True)
# Daily_backup_files="Daily_backup_files"     # Store daily data for all tickers as backup
# os.makedirs(Daily_backup_files, exist_ok=True)
# os.makedirs('temp',exist_ok=True) # Temporary file to hold new Intraday data. Later gets renamed to "Intraday_data_files" after new and old data gets Merged

# ### Step 3: Set up Tickers and Fetch data from Yahoo Finance
# IntradayObject1m=Intraday(interval='1m',start_intraday=2,end_intraday=0)
# mysymboldict={
#     "ZN=F":["ZN","10-Year T-Note Futures"],
#     "ZB=F":["ZB","30-Year T-Bond Futures"],
#     "ZF=F":["ZF","5-Year US T-Note Futures"],
#     "ZT=F":["ZT","2-Year US T-Note Futures"],
#     "DX-Y.NYB":["DXY","US Dollar Index"],
#     "CL=F":["CL","Crude Oil futures"],
#     "GC=F":["GC","Gold futures"],
#     "NQ=F":["NQ","Nasdaq 100 futures"],
#     "^DJI":["DJI","Dow Jones Industrial Average"],
#     "^GSPC":["GSPC","S&P 500"]
#     }
# start_date=IntradayObject1m.start_intraday
# end_date=IntradayObject1m.end_intraday
# IntradayObject1m.update_dict_symbols(mysymboldict)
# alldatadict=IntradayObject1m.fetch_data_yfinance(specific_tickers=IntradayObject1m.tickers) #Get dictionary of specific intraday data that we want to store

# ### Step 4: Merged the new data obtained with old data present in "Intraday_data_files" 
# for key in alldatadict.keys():
#     symbol=mysymboldict[key][0]
#     newcsv=alldatadict[key]
#     newcsv.drop_duplicates(inplace=True)
#     newcsv.dropna(inplace=True)
#     if (newcsv.index.to_list())!=[]:
#         newstart=str(newcsv.index.to_list()[0])[:10]
#         newend=str(newcsv.index.to_list()[-1])[:10]
#         start_end_date=f'Intraday_{symbol}_{newstart}_to_{newend}.csv'
#         alldatadict[key].to_csv(os.path.join(Daily_backup_files,start_end_date))# Save the new data file into "Daily_backup_files" folder
#     else:
#         print(f'No new data fetched for {symbol}')
#         newcsv=pd.DataFrame()

#     oldcsv=pd.DataFrame()
#     flag=0
#     for entry2 in os.scandir(Intraday_data_files):
#         if entry2.is_file() and entry2.name.endswith('.csv'):
#             oldcsvpath=os.path.join(Intraday_data_files,entry2.name)
#             [_,_,ticker,oldstart,_,oldend]=(entry2.name.replace('.csv',"")).split('_')
#             if ticker==symbol:
#                 oldcsvpath=os.path.join(Intraday_data_files,entry2.name)
#                 oldcsv=pd.read_csv(oldcsvpath)
#                 if 'Datetime' in list(oldcsv.columns):#index is in 0...... and not Datetime format->cause error in merging
#                     oldcsv.index.name='Datetime'
#                     oldcsv.columns.name='Price'
#                     oldcsv.index=oldcsv['Datetime']
#                     oldcsv.drop(columns=['Datetime'],axis=1,inplace=True)
#                 flag=1
#                 break
#         if flag==0:
#             print(f'Historical data for {symbol} not found.')

#     if newcsv.empty and oldcsv.empty:
#         print(f"No data available for {symbol}. Both old and new data are empty.")
#         finalcsv = pd.DataFrame()  # Create an empty DataFrame
    
#     elif newcsv.empty:
#         print(f"No new data fetched for {symbol}. Using only historical data.")
#         finalcsv = oldcsv.copy()  # Use only the historical data

#     elif oldcsv.empty:
#         print(f"No historical data found for {symbol}. Using only new data.")
#         finalcsv = newcsv.copy()  # Use only the new data

#     else:
#         finalcsv = pd.concat([oldcsv,newcsv])

#     finalcsv.drop_duplicates(inplace=True)
#     finalcsv.dropna(inplace=True) 
#     finalcsv.index = pd.to_datetime(finalcsv.index)
#     finalcsv.sort_index(inplace=True)
#     finalstart=str(finalcsv.index.to_list()[0])[:10]
#     finalend=str(finalcsv.index.to_list()[-1])[:10]
#     finalpath=os.path.join('temp',f'Intraday_data_{symbol}_{finalstart}_to_{finalend}.csv')
#     finalcsv.to_csv(finalpath,index=True)
#     print(finalcsv)

# ### Step 5: Delete the "Intraday_data_files directory" and rename "temp" as "Intraday_data_files directory"
# #Delete "Intraday_data_files directory"
# directory_path = Intraday_data_files
# try:
#     shutil.rmtree(directory_path)
#     print(f"Directory {directory_path} and its contents deleted successfully.")
# except FileNotFoundError:
#     print("The directory does not exist.")
# except PermissionError:
#     print("You do not have the necessary permissions to delete this directory.")

# #Rename "temp" as "Intraday_data_files directory" 
# current_name = "temp"
# new_name = "Intraday_data_files"

# try:
#     os.rename(current_name, new_name)
#     print(f"Directory renamed from '{current_name}' to '{new_name}'")
# except FileNotFoundError:
#     print(f"Directory '{current_name}' not found!")
# except PermissionError:
#     print("You do not have permission to rename this directory.")
# except Exception as e:
#     print(f"An error occurred: {e}")
