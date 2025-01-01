import pandas as pd
from intradaydata import Intraday
from preprocessing import Preprocessing
from events import Events
from returns import Returns
from nonevents import Nonevents
import os

def prepare_events(input_data_folder, processed_data_folder, events_data, default_tz="Asia/Kolkata", target_tz="US/Eastern"):
    """
    Prepares and processes economic event data by combining, assigning tiers and flags,
    and converting timestamps to a target timezone.

    Args:
        input_data_folder (str): Path to the folder containing the input data file.
        processed_data_folder (str): Path to the folder where processed files will be saved.
        events_data (str): Name of the Excel file containing event data.
        default_tz (str): The default timezone of the input data. Default is "Asia/Kolkata".
        target_tz (str): The target timezone for the processed data. Default is "US/Eastern".

    Returns:
        str: Path to the final processed file with timestamps in the target timezone.
    """
    # Define file paths
    events_excel_path = os.path.join(input_data_folder, events_data)
    combined_excel_path = os.path.join(processed_data_folder, f'{events_data.split(".", maxsplit=1)[0]}_combined.csv')
    combined_excel_target_tz_path = os.path.join(processed_data_folder, f'{events_data.split(".", maxsplit=1)[0]}_combined_target_tz.csv')
    
    # Define tier and flag dictionaries
    tier1_events = ['CPI', 'PPI', 'PCE', 'Inflation', 'NFP', 'Unemployment', 'Payrolls']
    tier2_events = ['JOLTs', 'ADP', 'PMI']
    tier3_events = ['Consumer Confidence', 'Weekly Jobless Claims', 'Industrial Production', 'Challenger Job Cuts', 'Auction']
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
        'IND_MACRO': my_macro_events,
        'IND_Tier1': tier1_events, 'IND_Tier2': tier2_events, 'IND_Tier3': tier3_events,
        'IND_FED': ['FOMC', 'Speech', 'Beige', 'Speak']
    }
    
    # Create Events class instance
    myevents = Events(events_excel_path, my_tier_dic, my_flag_dic)
    
    # Save combined events
    combined_excel = myevents.combined_excel
    myevents.save_sheet(combined_excel, combined_excel_path)
    
    # Process with Preprocessing class
    myeventsobject = Preprocessing(pd.read_csv(combined_excel_path))
    combined_excel_target_tz = myeventsobject.check_timezone(
        myeventsobject.dataframe, tz_col="datetime", default_tz=default_tz, target_tz=target_tz
    )
    myevents.save_sheet(combined_excel_target_tz, combined_excel_target_tz_path)
    
    # Return the path to the final processed file
    return (combined_excel_target_tz,combined_excel_target_tz_path)


def process_intraday_data(tickers, interval, start_intraday, end_intraday, combined_excel_target_tz, processed_data_folder,
                          skip_data_fetching=False,pre_fed_data="",month_day_filter=[12, 15, 31],
                          output_folder=""):
    if output_folder=="":
        output_folder="stats_and_plots_folder"
    else:
        output_folder=output_folder
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

    if skip_data_fetching==False and pre_fed_data=="":
        # Data Acquisition
        intraday_obj = Intraday(tickers=tickers, interval=interval, start_intraday=start_intraday, end_intraday=end_intraday)
        ticker_symbol = intraday_obj.dict_symbols['ZN=F'][0]
        data = intraday_obj.fetch_data_yfinance(['ZN=F'])
        data = list(data.values())[0]
        data = intraday_obj.data_acquisition(data)

    elif skip_data_fetching==True and pre_fed_data!="":
        data=pre_fed_data[0]
        ticker_symbol=pre_fed_data[1]

    # Data Preprocessing
    preprocessing_obj = Preprocessing(data)
    data_target_tz = preprocessing_obj.check_timezone(
        checkdf=data, tz_col="timestamp", default_tz="UTC", target_tz="US/Eastern"
    )
  
    # Event Tagging
    returns_obj = Returns(dataframe=data_target_tz)
    tagged_data = returns_obj.tag_events((combined_excel_target_tz), returns_obj.dataframe.copy())
  
    # Filtering Data
    filtered_data = returns_obj.filter_data(
        filter_df=tagged_data, month_day_filter=month_day_filter, to_sessions=True
    )
    filtered_data_path = os.path.join(
        processed_data_folder, f"{ticker_symbol}_{interval}_filtered_events_target_tz_tagged.csv"
    )
    if 'Datetime' in (filtered_data.columns):
        filtered_data.drop(axis=1,columns=['Datetime'],inplace=True)
    filtered_data.to_csv(filtered_data_path,index=False)

    # Filtering Nonevents
    nonevents_obj = Nonevents(filtered_data)
    nonevents_data = nonevents_obj.filter_nonevents(nonevents_obj.dataframe)
    ne_filtered_data = nonevents_data[(
        (nonevents_data['IND_NE_remove'] == 0) & (~nonevents_data['Volume'].isnull()))
    ]
    ne_filtered_data_path = os.path.join(
        processed_data_folder, f"{ticker_symbol}_{interval}_filtered_events_target_tz_tagged_nonevents.csv"
    )
    ne_filtered_data.to_csv(ne_filtered_data_path,index=False)

    # Data Visualization (Optional)
    # Uncomment if plotting functionality is implemented in the Returns class
    # daily_session_returns = returns_obj.get_daily_session_returns(filtered_data)
    # returns_obj.plot_daily_session_returns(filtered_data, ticker_symbol, interval)
    # returns_obj.plot_daily_session_volatility_returns(filtered_data, ticker_symbol, interval)

    stats_and_plots(returns_obj, ne_filtered_data,tickersymbol=ticker_symbol,interval=interval,output_folder=output_folder)

    return (
      ne_filtered_data,
      ne_filtered_data_path
    )
    
def stats_and_plots(my_returns_object,ne_filtered_data,tickersymbol,interval,output_folder):
    if output_folder!="":
        my_returns_object.output_folder=output_folder
    
    # Data Visualization:
    # 1. Daily Session Returns
    if 'd' not in interval: #interval>=1d
        daily_session_returns=my_returns_object.get_daily_session_returns(ne_filtered_data)
    #print(daily_session_returns)
        my_returns_object.plot_daily_session_returns(ne_filtered_data, tickersymbol, interval)

    # 2. Daily Session Volatility Returns
        my_returns_object.plot_daily_session_volatility_returns(ne_filtered_data,tickersymbol,interval)
    elif 'd' in interval: #interval<1d
     # # 1. Daily Returns
        daily_returns=my_returns_object.get_daily_returns(ne_filtered_data)
        # print(ddata_daily_returns)

    # # 2. Daily Volatility Returns
        my_returns_object.plot_daily_volatility_returns(ne_filtered_data,tickersymbol,interval)

if __name__ == "__main__":
    input_folder = 'Input_data'
    processed_folder = 'Processed_data2'
    output_folder='stats_and_plots_folder2'
    events = 'ecoevent_latest.xlsx'
    os.makedirs(output_folder,exist_ok=True)
    os.makedirs(processed_folder,exist_ok=True)
    (final_events_data,final_path) = prepare_events(input_data_folder=input_folder, processed_data_folder=processed_folder, events_data=events)
    print(f"Processed Events file saved at: {final_path}")


    #Case1: 1hr interval ZN
    tickers = ["ZN=F", "ZT=F"]
    interval = '1h'
    start_intraday = 729
    end_intraday = 1
    combined_excel_target_tz = final_events_data
    
    (final_data,final_data_path)=process_intraday_data(tickers, interval, start_intraday, end_intraday, combined_excel_target_tz=final_events_data, 
                                   processed_data_folder=processed_folder,output_folder=output_folder)
    print(f"Processed files saved at: {final_data_path}")
    print(final_data)


    #Case2: 1d interval ZN
    tickers = ["ZN=F", "ZT=F"]
    interval = '1d'
    start_intraday = -1
    end_intraday = -1
    combined_excel_target_tz = final_events_data
    
    (final_data,final_data_path)=process_intraday_data(tickers, interval, start_intraday, end_intraday, combined_excel_target_tz=final_events_data, 
                                   processed_data_folder=processed_folder,output_folder=output_folder
    )
    print(f"Processed files saved at: {final_data_path}")
    print(final_data)

    # When we give our own custom data: pre_fed_data=[zn1data,tickersymbol],skip_data_fetching=True 
    #Case3: ZN Intraday, 1m interval data
    
    zn1data=pd.read_csv(os.path.join(input_folder,'ZN_1m.csv'))
    zn1data=zn1data.dropna(axis=0)
    zn1data['timestamp']=zn1data['Datetime']
    zn1data.reset_index(drop=True,inplace=True)
    tickersymbol='ZN'
    interval='1m'
    start_intraday=-1
    end_intraday=-1
    tickers=['ZN=F']

    (final_data,final_data_path)=process_intraday_data(tickers, interval, start_intraday, end_intraday, combined_excel_target_tz=final_events_data, 
                                   processed_data_folder=processed_folder,
                                   pre_fed_data=[zn1data,tickersymbol],skip_data_fetching=True,month_day_filter=[],
                                   output_folder=output_folder
    )
    print(f"Processed files saved at: {final_data_path}")
    print(final_data)

    #Case4: Zn Intraday, 15m interval data
    zn15data=pd.read_csv(os.path.join(input_folder,'ZN_15m.csv'))
    zn15data=zn1data.dropna(axis=0)
    zn15data.reset_index(drop=True,inplace=True)
    tickersymbol='ZN'
    interval='15m'
    start_intraday=-1
    end_intraday=-1
    tickers=['ZN=F']

    (final_data,final_data_path)=process_intraday_data(tickers, interval, start_intraday, end_intraday, combined_excel_target_tz=final_events_data, 
                                   processed_data_folder=processed_folder,
                                   pre_fed_data=[zn15data,tickersymbol],skip_data_fetching=True,
                                   month_day_filter=[],
                                   output_folder=output_folder
    )
    print(f"Processed files saved at: {final_data_path}")
    print(final_data)