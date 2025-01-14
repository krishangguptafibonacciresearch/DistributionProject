### Step 1: Import Required Libraries and Define Functions
import os
import shutil #deleting directories
import pandas as pd
from intradaydata import Intraday

def save_data(Intraday_data_files,
              Daily_backup_files,
              return_interval, 
              IntradayObject,
              mysymboldict,
            
             ):
    start_date=IntradayObject.start_intraday
    end_date=IntradayObject.end_intraday
    alldatadict=IntradayObject.fetch_data_yfinance(specific_tickers=IntradayObject.tickers) #Get dictionary of specific intraday data that we want to store
    print(start_date,end_date)
    print(alldatadict)
    ## In the "temp" folder, merge the new data with old data (old data is present in "Intraday_data_files")
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
            print(alldatadict[key])
        else:
            print(f'No new data fetched for {symbol}')
            newcsv=pd.DataFrame()

        if 'Adj Close' not in newcsv.columns:
            newcsv['Adj Close']=newcsv['Close']
            # Define the desired column order
            required_columns = ['Adj Close', 'Close', 'High', 'Low', 'Open', 'Volume']
            
            # Reindex to reorder and fill missing columns with NaN
            newcsv = newcsv.reindex(columns=required_columns)
    
        flag=0
        for entry2 in os.scandir(Intraday_data_files):
            if entry2.is_file() and entry2.name.endswith('.csv'):
               
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
                else:
                    continue
        if flag==0:
            oldcsv=pd.DataFrame()
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
        finalcsv.dropna(inplace=True,how='all') 
        finalcsv.index = pd.to_datetime(finalcsv.index)
        finalcsv.sort_index(inplace=True)
        finalcsv.drop_duplicates(inplace=True)
        finalcsv.dropna(inplace=True,how='all') 
        finalcsv = finalcsv.loc[~finalcsv.index.duplicated(keep='last')]


        finalstart=str(finalcsv.index.to_list()[0])[:10]
        finalend=str(finalcsv.index.to_list()[-1])[:10]
        finalpath=os.path.join('temp',f'Intraday_data_{symbol}_{return_interval}_{finalstart}_to_{finalend}.csv')
        finalcsv.to_csv(finalpath,index=True)
        print(f'Old CSV for {symbol}')
        print(oldcsv)
        print(f'New CSV for {symbol}')
        print(newcsv)
        print(f'Final CSV for {symbol}')
        print(finalcsv)

   
def runner(start,
           end,
           ticker_interval,
           Intraday_data_files,
           Daily_backup_files,
           dic='default',
          ):
    my_intraday_obj=Intraday(start_intraday=start,
                             end_intraday=end,
                             interval=ticker_interval)
    if dic=='default':
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
    else:
        mysymboldict=dic

    my_intraday_obj.update_dict_symbols(mysymboldict)
    save_data(Intraday_data_files,
              Daily_backup_files,
              return_interval=ticker_interval, 
              IntradayObject=my_intraday_obj, 
              mysymboldict=mysymboldict,
             )
    
INTRADAY_FILES= "Intraday_data_files" # Read current dataset of historical data
if __name__=='__main__':
    ### Make Folders to Store Data
    os.makedirs(INTRADAY_FILES, exist_ok=True)

    DAILY_FILES="Daily_backup_files"     # Store daily data for all tickers as backup
    if os.path.exists(DAILY_FILES):
         # Remove the directory and its contents
        shutil.rmtree(DAILY_FILES)
        # Create the directory
    os.makedirs(DAILY_FILES)
    os.makedirs('temp',exist_ok=True) # Temporary file to hold new Intraday data. Later gets renamed to "Intraday_data_files" after new and old data gets Merged
    
    
    # Case:1
    runner(start=-1,
           end=-1,
           ticker_interval='1m',
           dic='default',
           Intraday_data_files=INTRADAY_FILES,
           Daily_backup_files=DAILY_FILES
          )

    
    # Case:2
    runner(start=720,
           end=0,
           ticker_interval='1h',
           dic={"ZN=F":["ZN","10-Year T-Note Futures"]},
           Intraday_data_files=INTRADAY_FILES,
           Daily_backup_files=DAILY_FILES
          )
    

    # Case:3
    runner(start=-1,
           end=-1,
           ticker_interval='15m',
           dic={"ZN=F":["ZN","10-Year T-Note Futures"]},
           Intraday_data_files=INTRADAY_FILES,
           Daily_backup_files=DAILY_FILES
          )
    

    # Case:4
    runner(start=-1,
           end=-1,
           ticker_interval='1d',
           dic={"ZN=F":["ZN","10-Year T-Note Futures"]},
           Intraday_data_files=INTRADAY_FILES,
           Daily_backup_files=DAILY_FILES
          )

        
    ### Delete the "Intraday_data_files directory" and rename "temp" as "Intraday_data_files directory"
    #Delete "Intraday_data_files directory"
    directory_path = INTRADAY_FILES
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
