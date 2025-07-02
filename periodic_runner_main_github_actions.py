### Step 1: Import Required Libraries and Define Functions
import os
import shutil #deleting directories
import pandas as pd
from intradaydata import Intraday
from intradaydata_investing_github_actions import Intraday_Investing
from preprocessing import ManipulateTimezone
from tzlocal import get_localzone  # Automatically detects system timezone

def _add_target_tz_col(intraday_csv,current_tz='UTC',final_tz='US/Eastern',tickerinterval=''):
    
    new_col=final_tz+' Timezone'
    intraday_csv[new_col]=intraday_csv.index

    if 'd' in tickerinterval: #Add time to DATE and make it "DATE + 23:59:00" if interval >=1d
            if len(str(intraday_csv[intraday_csv.columns[0]].iloc[0]))>=10:
                pass
                #time also mentioned
            else:
                #time not mentioned, assume day concluded, consider 11.59pm when converting to target timezone
                intraday_csv=ManipulateTimezone.add_time_for_d_intervals(intraday_csv,new_col)
    
    MyTimezoneObject=ManipulateTimezone(intraday_csv)
    intraday_target_tz_csv=MyTimezoneObject.change_timezone(checkdf=intraday_csv,
                                                            tz_col=new_col,
                                                            default_tz=current_tz,
                                                            target_tz=final_tz)
    return intraday_target_tz_csv


def _save_data(#Intraday_data_files,
              Intraday_data_files_pq,
              #Daily_backup_files,
              Daily_backup_files_pq,
              return_interval, 
              IntradayObject,
              mysymboldict,
              website='yahoo finance'
             ):
    
    #since start_intraday & end_intraday is not specified, the entire data is fetched.
    if website=='yahoo finance':
        #key = ticker, value = dataframe, with Datetime column as index.
        alldatadict=IntradayObject.fetch_data_yfinance(specific_tickers=IntradayObject.tickers) 
        fetched_tz='UTC'
    elif website=='investing':
        alldatadict={list(mysymboldict.values())[0][0]:IntradayObject.fetch_data_investing()}
        fetched_tz=get_localzone()

    #print(start_date,end_date)
    #print(alldatadict)
    ## In the "temp" folder, merge the new data with old data (old data is present in "Intraday_data_files")
    for key in alldatadict.keys():
        symbol=mysymboldict[key][0]
        newcsv=alldatadict[key]
        newcsv.drop_duplicates(inplace=True)
        newcsv.dropna(inplace=True)
        if (newcsv.index.to_list())!=[]:
            newstart=str(newcsv.index.to_list()[0])[:10] #starting timestamp
            newend=str(newcsv.index.to_list()[-1])[:10] #ending timestamp
            start_end_date=f'Intraday_{symbol}_{newstart}_to_{newend}'

            #updating the daily backup files folder with the latest data.
            # alldatadict[key].to_csv(os.path.join(Daily_backup_files,f"{start_end_date}.csv"))
            alldatadict[key].to_parquet(os.path.join(Daily_backup_files_pq, f"{start_end_date}.parquet"),engine="pyarrow")

            print(f'New data fetched for {symbol}: {start_end_date}')
            # print(alldatadict[key])

            if 'Adj Close' not in newcsv.columns:
                newcsv['Adj Close']=newcsv['Close']
                # Define the desired column order
                required_columns = ['Adj Close', 'Close', 'High', 'Low', 'Open', 'Volume']
                
                # Reindex to reorder and fill missing columns with NaN
                #existing index of the df is unchanged. Datetime is still the index (dtype='datetime64[ns, UTC]')
                newcsv = newcsv.reindex(columns=required_columns) 

        else:
            print(f'No new data fetched for {symbol}')
            newcsv=pd.DataFrame()

    
        flag=0
        # for entry2 in os.scandir(Intraday_data_files):
        #     if entry2.is_file() and entry2.name.endswith('.csv') and 'stats' not in entry2.name:
               
        #         [_,_,ticker,interval,oldstart,_,oldend]=(entry2.name.replace('.csv',"")).split('_')
        #         if ticker==symbol and interval==return_interval:
        #             oldcsvpath=os.path.join(Intraday_data_files,entry2.name)
        #             oldcsv=pd.read_csv(oldcsvpath)
        #             if 'Datetime' in list(oldcsv.columns):#index is in 0...... and not Datetime format->cause error in merging
        #                 oldcsv.index.name='Datetime'
        #                 # oldcsv.columns.name='Price'
        #                 oldcsv.index=oldcsv['Datetime'] #Datetime is now present both as an index and as a column.
        #                 oldcsv.drop(columns=['Datetime'],axis=1,inplace=True) #removing the column Datetime. not the index.
        #             flag=1
        #             break
        #         else:
        #             continue

        for entry2 in os.scandir(Intraday_data_files_pq):
            if entry2.is_file() and entry2.name.endswith('.parquet') and 'stats' not in entry2.name:
               
                [_,_,ticker,interval,oldstart,_,oldend]=(entry2.name.replace('.parquet',"")).split('_')
                if ticker==symbol and interval==return_interval:
                    oldcsvpath=os.path.join(Intraday_data_files_pq,entry2.name)
                    oldcsv=pd.read_parquet(oldcsvpath)
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
        # print(finalcsv)
        finalcsv.drop_duplicates(inplace=True)
        finalcsv.dropna(inplace=True,how='all') 
        if website=='yahoo finance': #default tz=utc.
            finalcsv.index = pd.to_datetime(finalcsv.index,utc=True) #Yahoo finance by default converts the data into utc and prints.
        else:
            finalcsv.index = pd.to_datetime(finalcsv.index)
        finalcsv.sort_index(inplace=True)
        finalcsv.drop_duplicates(inplace=True)
        finalcsv.dropna(inplace=True,how='all') 
        finalcsv = finalcsv.loc[~finalcsv.index.duplicated(keep='last')]


        finalstart=str(finalcsv.index.to_list()[0])[:10]
        finalend=str(finalcsv.index.to_list()[-1])[:10]
        # finalpath=os.path.join('temp',f'Intraday_data_{symbol}_{return_interval}_{finalstart}_to_{finalend}.csv')
        final_path_pq = os.path.join('temp_pq',f'Intraday_data_{symbol}_{return_interval}_{finalstart}_to_{finalend}.parquet')
        finalcsv=_add_target_tz_col(finalcsv,current_tz=fetched_tz,final_tz='US/Eastern',tickerinterval=return_interval)
        # finalcsv.to_csv(finalpath,index=True)
        finalcsv.to_parquet(final_path_pq , engine='pyarrow')
        # #print(f'Old CSV for {symbol}')
        # #print(f'New CSV for {symbol}')
        # print(f'Combined CSV for {symbol}')
        # print(finalcsv)

        # stored_csv_path_stats=finalpath.replace('.csv','_stats.csv')
        # final_stats_csv=_store_descriptive_stats(finalcsv,'Adj Close')
        # final_stats_csv.name=f'(Interval:{return_interval}, Symbol:{symbol})'
        # final_stats_csv.to_csv(stored_csv_path_stats)
        
   
def runner(start,
           end,
           ticker_interval,
        #    Intraday_data_files,
           Intraday_data_files_pq,
        #    Daily_backup_files,
           Daily_backup_files_pq,
           dic='default',
           mywebsite='yahoo finance'
          ):
    if mywebsite=='yahoo finance':
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

        _save_data(#Intraday_data_files,
        Intraday_data_files_pq,
        # Daily_backup_files,
        Daily_backup_files_pq,
        return_interval=ticker_interval,
        IntradayObject=my_intraday_obj,
        mysymboldict=mysymboldict,
        website=mywebsite
        )

    elif mywebsite=='investing':
        if dic=='default':
            mysymboldict={
                "FGBL":["FGBL","German 10 YR Bund Futures",
                'https://in.investing.com/rates-bonds/euro-bund-historical-data']
            }
        else:
            mysymboldict=dic

        print(len(list(mysymboldict)))
        my_intraday_obj=Intraday_Investing(url=list(mysymboldict.values())[0][2],
                                           interval=ticker_interval)
        
        _save_data(#Intraday_data_files,
            Intraday_data_files_pq,
            #Daily_backup_files,
            Daily_backup_files_pq,
            return_interval=ticker_interval,
            IntradayObject=my_intraday_obj,
            mysymboldict=mysymboldict,
            website=mywebsite
            )
        
   
# INTRADAY_FILES= "Intraday_data_files" # Read current dataset of historical data
INTRADAY_FILES_PQ = "Intraday_data_files_pq"
if __name__=='__main__':
    ### Make Folders to Store Data
    # os.makedirs(INTRADAY_FILES, exist_ok=True)
    os.makedirs(INTRADAY_FILES_PQ, exist_ok=True)

    # DAILY_FILES="Daily_backup_files"     # Store daily data for all tickers as backup
    DAILY_FILES_PQ = "Daily_backup_files_pq"

    # if os.path.exists(DAILY_FILES):
    #      # Remove the directory and its contents
    #     shutil.rmtree(DAILY_FILES)
    #     # Create the directory

    if os.path.exists(DAILY_FILES_PQ):
        shutil.rmtree(DAILY_FILES_PQ)

    # os.makedirs(DAILY_FILES)
    os.makedirs(DAILY_FILES_PQ)

    # Temporary file to hold new Intraday data. Later gets renamed to "Intraday_data_files" after new and old data gets Merged
    # os.makedirs('temp',exist_ok=True) 
    os.makedirs('temp_pq' , exist_ok = True)
    
    # Case:1
    runner(start=-1,
           end=-1,
           ticker_interval='1m',     #1m frequency available for all instruments.
           dic='default',
        #    Intraday_data_files=INTRADAY_FILES,
           Intraday_data_files_pq=INTRADAY_FILES_PQ,
        #    Daily_backup_files=DAILY_FILES,
           Daily_backup_files_pq=DAILY_FILES_PQ
          )

    
    # Case:2
    runner(start=710,
           end=-10,
           ticker_interval='1h',
           dic={"ZN=F":["ZN","10-Year T-Note Futures"]},
        #    Intraday_data_files=INTRADAY_FILES,
           Intraday_data_files_pq=INTRADAY_FILES_PQ,
        #    Daily_backup_files=DAILY_FILES,
           Daily_backup_files_pq=DAILY_FILES_PQ
          )
    

    # Case:3
    runner(start=15,
           end=-3,
           ticker_interval='15m',
           dic={"ZN=F":["ZN","10-Year T-Note Futures"]},
        #    Intraday_data_files=INTRADAY_FILES,
           Intraday_data_files_pq=INTRADAY_FILES_PQ,
        #    Daily_backup_files=DAILY_FILES,           
           Daily_backup_files_pq=DAILY_FILES_PQ
          )
    

    # Case:4
    runner(start=-1,
           end=-1,
           ticker_interval='1d',
           dic={"ZN=F":["ZN","10-Year T-Note Futures"]},
        #    Intraday_data_files=INTRADAY_FILES,
           Intraday_data_files_pq=INTRADAY_FILES_PQ,
        #    Daily_backup_files=DAILY_FILES,
           Daily_backup_files_pq=DAILY_FILES_PQ
          )
    
    # Case:5: FGBL from investing.com
    runner(
        start=None,
        end=None,
        ticker_interval='1d',
        dic={"FGBL":["FGBL","German 10 YR Bund Futures",'https://in.investing.com/rates-bonds/euro-bund-historical-data']},
        # Intraday_data_files=INTRADAY_FILES,
        Intraday_data_files_pq = INTRADAY_FILES_PQ,
        # Daily_backup_files=DAILY_FILES,
        Daily_backup_files_pq=DAILY_FILES_PQ,
        mywebsite='investing'
    )

        
    ### Delete the "Intraday_data_files directory" and rename "temp" as "Intraday_data_files directory"
    #Delete "Intraday_data_files directory"
    # directory_path = INTRADAY_FILES
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

    directory_path_pq = INTRADAY_FILES_PQ
    try:
        shutil.rmtree(directory_path_pq)
        print(f"Directory {directory_path_pq} and its contents deleted successfully.")
    except FileNotFoundError:
        print("The directory does not exist (pq).")
    except PermissionError:
        print("You do not have the necessary permissions to delete this directory (pq).")
    
    #Rename "temp_pq" as "Intraday_data_files_pq directory" 
    current_name = "temp_pq"
    new_name = "Intraday_data_files_pq"
    
    try:
        os.rename(current_name, new_name)
        print(f"Directory renamed from '{current_name}' to '{new_name}'")
    except FileNotFoundError:
        print(f"Directory '{current_name}' not found (pq)!")
    except PermissionError:
        print("You do not have permission to rename this directory (pq).")
    except Exception as e:
        print(f"An error occurred (pq): {e}")
