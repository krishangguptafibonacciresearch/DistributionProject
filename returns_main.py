import pandas as pd
from intradaydata import Intraday
from preprocessing import ManipulateTimezone
from events import Events
from returns import Returns
from nonevents import Nonevents
from periodic_runner_main import INTRADAY_FILES as Intraday_data_files
import shutil
import os
from tzlocal import get_localzone 

def _change_event_tiers(
    events_data_folder,
    processed_data_folder,
    events_data_path,
    default_tz="Asia/Kolkata",
    target_tz="US/Eastern",
    change_tiers_bool=True
):
    """
    Prepares and processes economic event data by combining, assigning tiers and flags,
    and converting timestamps to a target timezone.

    Args:
        event_folder (str): Path to the folder containing the input data file.
        processed_data_folder (str): Path to the folder where processed files will be saved.
        events_data (str): Name of the Excel file containing event data.
        default_tz (str): The default timezone of the input data. Default is "Asia/Kolkata".
        target_tz (str): The target timezone for the processed data. Default is "US/Eastern".

    Returns:
        str: Path to the final processed file with timestamps in the target timezone.
    """

    # Define tier and flag dictionaries
    tier1_events = ["CPI", "PPI", "PCE", "Core Inflation", "NFP", "Unemployment", "Payrolls"]
    tier2_events = ["JOLTs", "ADP", "PMI"]
    tier3_events = [
        "Consumer Confidence",
        "Weekly Jobless Claims",
        "Industrial Production",
        "Challenger Job Cuts",
        "Auction",
        "Consumer Inflation"
    ]
    my_macro_events = tier1_events + tier2_events + tier3_events

    # Tier dictionary
    my_tier_dic = {}
    for event in my_macro_events:
        tier = 4
        if event in tier1_events:
            tier = 1
        elif event in tier2_events:
            tier = 2
        elif event in tier3_events:
            tier = 3
        my_tier_dic[event] = tier

    # Flag dictionary
    my_flag_dic = {
        "IND_MACRO": my_macro_events,
        "IND_Tier1": tier1_events,
        "IND_Tier2": tier2_events,
        "IND_Tier3": tier3_events,
        "IND_FED": ["FOMC", "Speech", "Beige", "Speak"],
    }

    events_excel_path = os.path.join(events_data_folder, events_data_path)
   
    
    # Create Events class instance
    add_new_events_dic={'IST':['US']}
    myevents = Events(events_excel_path, my_tier_dic, my_flag_dic,new_events_folder=folder_events,
                      add_new_events_dic=add_new_events_dic,
                      change_tiers=change_tiers_bool)

    # Save combined events
    combined_excel = myevents.combined_excel
    start_date=str(combined_excel.loc[0,combined_excel.columns[0]]).split()[0]
    end_date=str(combined_excel.loc[len(combined_excel)-1,combined_excel.columns[0]]).split()[0]
    combined_excel_path = os.path.join(
        processed_data_folder, f'{events_data_path.split(".", maxsplit=1)[0]}_{start_date}_to_{end_date}_combined.csv'
    )

    myevents.save_sheet(combined_excel, combined_excel_path)

    # Manipulate the Timezone
    myeventsobject = ManipulateTimezone(pd.read_csv(combined_excel_path))
    combined_excel_target_tz = myeventsobject.change_timezone(
        myeventsobject.dataframe,
        "datetime",
        default_tz,
        target_tz,
    )
    # Save combined events with new timezone
    start_date=str(combined_excel_target_tz.loc[0,combined_excel.columns[0]]).split()[0]
    end_date=str(combined_excel_target_tz.loc[len(combined_excel)-1,combined_excel.columns[0]]).split()[0]

    combined_excel_target_tz_path = os.path.join(
        processed_data_folder,
        f'{events_data_path.split(".", maxsplit=1)[0]}_{start_date}_to_{end_date}_combined_target_tz.csv',
    )
    myevents.save_sheet(combined_excel_target_tz, combined_excel_target_tz_path)

    # Return the path to the final processed file
    return (combined_excel_target_tz, combined_excel_target_tz_path)

# scans the intraday data folder for the raw data & calls the next function.
def scan_folder_and_calculate_returns(
        ticker_match_tuple,
        input_folder,
        processed_folder,
        output_folder,
        final_events_data,
        ):
   
    for tickersymbol,tickerinterval,ticker_bps_factor in ticker_match_tuple:
        file_path = 'NA'
        for csvfile in os.scandir(input_folder):
            if 'stats' in csvfile.name:
                continue
            if csvfile.is_file() and csvfile.name.endswith('.parquet') and (csvfile.name.split('_'))[2]==tickersymbol and (csvfile.name.split('_'))[3]==tickerinterval:
                file_path = csvfile.path
        if file_path=='NA':
            continue
        csvdata=pd.read_parquet(file_path , engine = 'pyarrow')
        # csvdata['Datetime'] = pd.to_datetime(csvdata['Datetime'], utc=True) #redundant
        # csvdata.set_index('Datetime', inplace=True) #also redundant
        print(csvdata.columns)

        if 'd' in tickerinterval: #Add time to DATE and make it "DATE + 23:59:00" if interval >=1d
            csvdata=ManipulateTimezone.add_time_for_d_intervals(csvdata,csvdata.columns[0])


        csvdata.dropna(inplace=True,axis=0,how='all')
        csvdata['timestamp']=csvdata.index
        csvdata.reset_index(drop=True,inplace=True) #df does not have a Datetime column anymore & index is 0,1,2,3...
        print(csvdata.tail())

        (final_data, final_data_path) = _get_distribution_of_returns(
            ticker_bps_factor,
            combined_excel_target_tz=final_events_data,
            processed_data_folder=processed_folder,
            pre_fed_data=[csvdata, tickersymbol],
            skip_data_fetching=True,
            myoutput_folder=output_folder,
            interval=tickerinterval,
            month_day_filter=[],#[12, 15, 31] 12: December, 15: Start Date, 31: End Date
            
        )
        print(f"Processed files saved at: {final_data_path}")
        #print(final_data)

def _get_distribution_of_returns(
    bps_factor,
    mytickers='NotDefined',
    interval='NotDefined',
    start_intraday='NotDefined',
    end_intraday='NotDefined',
    combined_excel_target_tz='NotDefined',
    processed_data_folder='NotDefined',
    myoutput_folder="NotDefined",
    skip_data_fetching=False,
    pre_fed_data="",
    month_day_filter=[]#Don't filter dates by default
):
    """
    Processes intraday data for a given list of tickers, performs tagging, filtering, and generates output files.

    Args:
        tickers (list): List of tickers (e.g., ["ZN=F", "ZT=F"]).
        interval (str): Interval for intraday data (e.g., '1h').
        start_intraday (int): Start date offset in days for fetching intraday data.
        end_intraday (int): End date offset in days for fetching intraday data.
        combined_excel_target_tz (str): Path to the events Excel file with target timezone data.
        processed_data_folder (str): Folder path to save processed files.

    Returns:
        dict: Paths of the processed files.
    """
    data = None
    ticker_symbol = None

    if int(skip_data_fetching) == 0 and pre_fed_data == "":
        # Data Acquisition
        intraday_obj = Intraday(
            tickers=mytickers,
            interval=interval,
            start_intraday=start_intraday,
            end_intraday=end_intraday,
        )
        ticker_symbol = intraday_obj.dict_symbols["ZN=F"][0]
        data = intraday_obj.fetch_data_yfinance(["ZN=F"])
        data = list(data.values())[0]
        data = intraday_obj.data_acquisition(data)

    elif int(skip_data_fetching) == 1 and pre_fed_data != "":
        data = pre_fed_data[0]
        ticker_symbol = pre_fed_data[1]

    # Data Preprocessing: Change the timezone of the historical data to target timezone
    preprocessing_obj = ManipulateTimezone(data)

    if ticker_symbol=='FGBL':
        current_tz=get_localzone()
    else:
        current_tz='UTC'

    data_target_tz = preprocessing_obj.change_timezone(
        checkdf=data, tz_col="timestamp", default_tz=current_tz, target_tz="US/Eastern"
    )

    # Event Tagging
    returns_obj = Returns(dataframe=data_target_tz,output_folder=myoutput_folder)
    tagged_data = returns_obj.tag_events(
        (combined_excel_target_tz), returns_obj.dataframe.copy()
    )

    # Filtering Data
    if month_day_filter==[]:
        filtered_dates=""
    else:
        filtered_dates="_filtered_dates"
    filtered_data = returns_obj.filter_date(
        filter_df=tagged_data, month_day_filter=month_day_filter, to_sessions=True
    )

    if "Datetime" in (filtered_data.columns):
        filtered_data.drop(axis=1, columns=["Datetime"], inplace=True)

    # # saving the csv for event data.
    # filtered_data_path = os.path.join(
    #     processed_data_folder,
    #     f"{ticker_symbol}_{interval}{filtered_dates}_events_tagged_target_tz.csv",
    # )
    # filtered_data.to_csv(filtered_data_path, index=False)

    # saving the parquet for event data.
    filtered_data_path_pq = os.path.join(
        processed_data_folder,
        f"{ticker_symbol}_{interval}{filtered_dates}_events_tagged_target_tz.parquet",
    )
    filtered_data.to_parquet(filtered_data_path_pq , engine = 'pyarrow' ,  index = False)

    # Filtering Nonevents
    nonevents_obj = Nonevents(filtered_data)
    nonevents_data = nonevents_obj.filter_nonevents(nonevents_obj.dataframe)
    ne_filtered_data = nonevents_data[
        ((nonevents_data["IND_NE_remove"] == 0) & (~nonevents_data["Volume"].isnull()))
    ]

    # #saving the CSV for NE data.
    # ne_filtered_data_path = os.path.join(
    #     processed_data_folder,
    #     f"{ticker_symbol}_{interval}{filtered_dates}_events_tagged_target_tz_nonevents.csv",
    # )
    # ne_filtered_data.to_csv(ne_filtered_data_path, index=False)

    #saving the parquet for NE data.
    ne_filtered_data_path_pq = os.path.join(
        processed_data_folder,
        f"{ticker_symbol}_{interval}{filtered_dates}_events_tagged_target_tz_nonevents.parquet",
    )
    ne_filtered_data.to_parquet(ne_filtered_data_path_pq , engine = 'pyarrow' , index = False)

    _get_stats_plots(
        returns_obj,
        ne_filtered_data,
        bps_factor,
        tickersymbol=ticker_symbol,
        interval=interval,
    )

    return (ne_filtered_data, ne_filtered_data_path_pq)

def _get_stats_plots(my_returns_object,
                    ne_filtered_data, 
                    bps_factor,
                    tickersymbol, 
                    interval):
    
    # Data Visualization:
    # 1. Daily Session Returns

    if "m" in interval or "h" in interval:  # interval<1d
        daily_session_returns = my_returns_object.get_daily_session_returns(ne_filtered_data,bps_factor) # NEEDED??
        my_returns_object.plot_daily_session_returns(ne_filtered_data, tickersymbol, interval,bps_factor)

    elif "d" in interval:  # interval=1d
        # 1. Daily Returns
        daily_returns = my_returns_object.get_daily_returns(ne_filtered_data,bps_factor) # NEEDED?? 

        my_returns_object.plot_daily_session_returns(ne_filtered_data, tickersymbol, interval,bps_factor)

    # 2. Daily Session Volatility Returns
    my_returns_object.plot_daily_session_volatility_returns(ne_filtered_data, tickersymbol, interval,bps_factor
)
    
folder_events= 'Input_data'
folder_input = Intraday_data_files + '_pq'
folder_output = Intraday_data_files+'_stats_and_plots_folder'
folder_processed_pq = Intraday_data_files+'_processed_folder_pq'
ticker_match_tuple=(("ZN",'1m',16),
                        ("ZN",'15m',16),
                        ("ZN",'1h',16),
                        ('ZN','1d',16),
                        ("ZB",'1m',16),
                        ("ZT",'1m',16),
                        ("ZF",'1m',16),
                        ('FGBL','1d',100)
                        )

if __name__ == "__main__":
    folder_events= 'Input_data'
    folder_input = 'Intraday_data_files_pq'
    folder_output = Intraday_data_files+'_stats_and_plots_folder' 
    # folder_processed = Intraday_data_files+'_processed_folder'
    folder_processed_pq = Intraday_data_files+'_processed_folder_pq'

    # Intraday_data_files is not a string for the last 3 because it is imported from another folder.
   
    # Delete the directory and its contents
    try:
        shutil.rmtree(folder_output)
        print(f"Directory '{folder_output}' and its contents have been deleted successfully.")
    except FileNotFoundError:
        print(f"Directory '{folder_output}' does not exist.")
    except PermissionError:
        print(f"Permission denied to delete '{folder_output}'.")

    # # REMOVE THIS WHEN CONVERSION WORKS FINE.
    # try:
    #     shutil.rmtree(folder_processed)
    #     print(f"Directory '{folder_processed}' and its contents have been deleted successfully.")
    # except FileNotFoundError:
    #     print(f"Directory '{folder_processed}' does not exist.")
    # except PermissionError:
    #     print(f"Permission denied to delete '{folder_processed}'.")

    try:
        shutil.rmtree(folder_processed_pq)
        print(f"Directory '{folder_processed_pq}' and its contents have been deleted successfully.")
    except FileNotFoundError:
        print(f"Directory '{folder_processed_pq}' does not exist.")
    except PermissionError:
        print(f"Permission denied to delete '{folder_processed_pq}'.")


    # os.makedirs(folder_processed)#exist_ok=True)
    os.makedirs(folder_output)#exist_ok=True)
    os.makedirs(folder_processed_pq)
   
    myevents_path = "EconomicEventsSheet15-24.xlsx"
    ticker_match_tuple=(("ZN",'1m',16),
                        ("ZN",'15m',16),
                        ("ZN",'1h',16),
                        ('ZN','1d',16),
                        ("ZB",'1m',16),
                        ("ZT",'1m',16),
                        ("ZF",'1m',16),
                        ('FGBL','1d',100)
                        )

    # Create Events DataFrame
    (final_events_data, final_path) = _change_event_tiers(
        events_data_folder=folder_events,
        processed_data_folder=folder_processed_pq,
        events_data_path=myevents_path,
        change_tiers_bool=True
    )
    print(f"Processed Events file saved at: {final_path}")

    scan_folder_and_calculate_returns(
        ticker_match_tuple,
        folder_input,
        folder_processed_pq,
        folder_output,
        final_events_data
    )