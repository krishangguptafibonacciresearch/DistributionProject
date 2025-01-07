import pandas as pd
from intradaydata import Intraday
from preprocessing import ManipulateTimezone
from events import Events
from returns import Returns
from nonevents import Nonevents
from periodic_runner_main import Intraday_data_files
import os


def _change_event_tiers(
    events_data_folder,
    processed_data_folder,
    events_data_path,
    default_tz="Asia/Kolkata",
    target_tz="US/Eastern",
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
    # Define file paths
    events_excel_path = os.path.join(events_data_folder, events_data_path)
    combined_excel_path = os.path.join(
        processed_data_folder, f'{events_data_path.split(".", maxsplit=1)[0]}_sheets_combined.csv'
    )
    combined_excel_target_tz_path = os.path.join(
        processed_data_folder,
        f'{events_data_path.split(".", maxsplit=1)[0]}_sheets_combined_target_tz.csv',
    )

    # Define tier and flag dictionaries
    tier1_events = ["CPI", "PPI", "PCE", "Inflation", "NFP", "Unemployment", "Payrolls"]
    tier2_events = ["JOLTs", "ADP", "PMI"]
    tier3_events = [
        "Consumer Confidence",
        "Weekly Jobless Claims",
        "Industrial Production",
        "Challenger Job Cuts",
        "Auction",
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

    # Create Events class instance
    myevents = Events(events_excel_path, my_tier_dic, my_flag_dic)

    # Save combined events
    combined_excel = myevents.combined_excel
    myevents.save_sheet(combined_excel, combined_excel_path)

    # Manipulate the Timezone
    myeventsobject = ManipulateTimezone(pd.read_csv(combined_excel_path))
    combined_excel_target_tz = myeventsobject.change_timezone(
        myeventsobject.dataframe,
        "datetime",
        default_tz,
        target_tz,
    )

    myevents.save_sheet(combined_excel_target_tz, combined_excel_target_tz_path)

    # Return the path to the final processed file
    return (combined_excel_target_tz, combined_excel_target_tz_path)


def _get_distribution_of_returns(
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
    data_target_tz = preprocessing_obj.change_timezone(
        checkdf=data, tz_col="timestamp", default_tz="UTC", target_tz="US/Eastern"
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
    filtered_data_path = os.path.join(
        processed_data_folder,
        f"{ticker_symbol}_{interval}{filtered_dates}_events_tagged_target_tz.csv",
    )
    if "Datetime" in (filtered_data.columns):
        filtered_data.drop(axis=1, columns=["Datetime"], inplace=True)
    filtered_data.to_csv(filtered_data_path, index=False)

    # Filtering Nonevents
    nonevents_obj = Nonevents(filtered_data)
    nonevents_data = nonevents_obj.filter_nonevents(nonevents_obj.dataframe)
    ne_filtered_data = nonevents_data[
        ((nonevents_data["IND_NE_remove"] == 0) & (~nonevents_data["Volume"].isnull()))
    ]
    ne_filtered_data_path = os.path.join(
        processed_data_folder,
        f"{ticker_symbol}_{interval}{filtered_dates}_events_tagged_target_tz_nonevents.csv",
    )
    ne_filtered_data.to_csv(ne_filtered_data_path, index=False)

    # Data Visualization (Optional)
    # Uncomment if plotting functionality is implemented in the Returns class
    # daily_session_returns = returns_obj.get_daily_session_returns(filtered_data)
    # returns_obj.plot_daily_session_returns(filtered_data, ticker_symbol, interval)
    # returns_obj.plot_daily_session_volatility_returns(filtered_data, ticker_symbol, interval)

    _get_stats_plots(
        returns_obj, ne_filtered_data, tickersymbol=ticker_symbol, interval=interval
    )

    return (ne_filtered_data, ne_filtered_data_path)


def _get_stats_plots(my_returns_object, ne_filtered_data, tickersymbol, interval):

    # Data Visualization:
    # 1. Daily Session Returns
    if "m" in interval or "h" in interval:  # interval<1d
        daily_session_returns = my_returns_object.get_daily_session_returns(
            ne_filtered_data
        )

        # print(daily_session_returns)
        my_returns_object.plot_daily_session_returns(
            ne_filtered_data, tickersymbol, interval
        )

        # 2. Daily Session Volatility Returns
        my_returns_object.plot_daily_session_volatility_returns(
            ne_filtered_data, tickersymbol, interval
        )
    elif "d" in interval:  # interval=1d
        # # 1. Daily Returns
        daily_returns = my_returns_object.get_daily_returns(ne_filtered_data)
        # print(ddata_daily_returns)

        # # 2. Daily Volatility Returns
        my_returns_object.plot_daily_volatility_returns(
            ne_filtered_data, tickersymbol, interval
        )

def scan_folder_and_calculate_returns(
        ticker_match_tuple,
        input_folder,
        processed_folder,
        output_folder,
        final_events_data,
        ):
   
    for tickersymbol,tickerinterval in ticker_match_tuple:
        csvfile='NA'
        for csvfile in os.scandir(input_folder):
            if csvfile.is_file() and csvfile.name.endswith('.csv') and (csvfile.name.split('_'))[2]==tickersymbol and (csvfile.name.split('_'))[3]==tickerinterval:
                    break
        if csvfile=='NA':
            continue
        csvdata=pd.read_csv(csvfile)
        csvdata.dropna(inplace=True,axis=0)
        csvdata['timestamp']=csvdata['Datetime']
        csvdata.reset_index(drop=True,inplace=True)
        print(csvdata.tail())

        (final_data, final_data_path) = _get_distribution_of_returns(
            combined_excel_target_tz=final_events_data,
            processed_data_folder=processed_folder,
            pre_fed_data=[csvdata, tickersymbol],
            skip_data_fetching=True,
            myoutput_folder=output_folder,
            interval=tickerinterval,
            month_day_filter=[]#[12, 15, 31]
        )
        print(f"Processed files saved at: {final_data_path}")
        print(final_data)


if __name__ == "__main__":
    folder_events= 'Input_data'
    folder_input = Intraday_data_files
    folder_output = Intraday_data_files+'_stats_and_plots_folder'
    folder_processed = Intraday_data_files+'_processed_folder'
    os.makedirs(folder_processed,exist_ok=True)
    os.makedirs(folder_output,exist_ok=True)

    myevents_path = "EconomicEventsSheet15-24.xlsx"
    ticker_match_tuple=(("ZN",'1m'),("ZN",'15m'),("ZN",'1h'))

    (final_events_data, final_path) = _change_event_tiers(
        events_data_folder=folder_events,
        processed_data_folder=folder_processed,
        events_data_path=myevents_path,
    )
    print(f"Processed Events file saved at: {final_path}")
    scan_folder_and_calculate_returns(
        ticker_match_tuple,
        folder_input,
        folder_processed,
        folder_output,
        final_events_data
    )


    # Case3: ZN Intraday, 1m interval data

    # zn1data = pd.read_csv(
    #     r"Intraday_data_files/Intraday_data_ZN_1m_2024-09-30_to_2025-01-03.csv"
    # )
    # print("file loaded for 18th")
    # zn1data = zn1data.dropna(axis=0)
    # zn1data["timestamp"] = zn1data["Datetime"]
    # zn1data.reset_index(drop=True, inplace=True)
    # tickersymbol = "ZN"
    # interval = "1m"
    # print(zn1data.tail())

    
    #tickers = ["ZN=F"]

    

    # # Case1: 1hr interval ZN
    # tickers = ["ZN=F", "ZT=F"]
    # interval = "1h"
    # start_intraday = 729
    # end_intraday = 1
    # combined_excel_target_tz = final_events_data

    # (final_data, final_data_path) = process_intraday_data(
    #     tickers,
    #     interval,
    #     start_intraday,
    #     end_intraday,
    #     combined_excel_target_tz=final_events_data,
    #     processed_data_folder=processed_folder,
    # )
    # print(f"Processed files saved at: {final_data_path}")
    # print(final_data)

    # # Case2: 1d interval ZN
    # tickers = ["ZN=F", "ZT=F"]
    # interval = "1d"
    # start_intraday = -1
    # end_intraday = -1
    # combined_excel_target_tz = final_events_data

    # (final_data, final_data_path) = process_intraday_data(
    #     tickers,
    #     interval,
    #     start_intraday,
    #     end_intraday,
    #     combined_excel_target_tz=final_events_data,
    #     processed_data_folder=processed_folder,
    # )
    # print(f"Processed files saved at: {final_data_path}")
    # print(final_data)

    # # Case3: ZN Intraday, 1m interval data

    # zn1data = pd.read_csv(
    #     r"Intraday_data_files/Intraday_data_ZN_1m_2024-09-30_to_2025-01-03.csv"
    # )
    # print("file loaded for 18th")
    # zn1data = zn1data.dropna(axis=0)
    # zn1data["timestamp"] = zn1data["Datetime"]
    # zn1data.reset_index(drop=True, inplace=True)
    # tickersymbol = "ZN"
    # interval = "1m"
    # print(zn1data.tail())

    # start_intraday = -1
    # end_intraday = -1
    # tickers = ["ZN=F"]

    # (final_data, final_data_path) = process_intraday_data(
    #     tickers,
    #     interval,
    #     start_intraday,
    #     end_intraday,
    #     combined_excel_target_tz=final_events_data,
    #     processed_data_folder=processed_folder,
    #     pre_fed_data=[zn1data, tickersymbol],
    #     skip_data_fetching=True,
    #     month_day_filter=[],
    # )
    # print(f"Processed files saved at: {final_data_path}")
    # print(final_data)

    # # Case4: Zn Intraday, 15m interval data
    # zn15data = pd.read_csv(os.path.join(input_folder, "ZN_15m.csv"))
    # zn15data = zn1data.dropna(axis=0)
    # zn15data.reset_index(drop=True, inplace=True)
    # tickersymbol = "ZN"
    # interval = "15m"
    # start_intraday = -1
    # end_intraday = -1
    # tickers = ["ZN=F"]

    # (final_data, final_data_path) = process_intraday_data(
    #     tickers,
    #     interval,
    #     start_intraday,
    #     end_intraday,
    #     combined_excel_target_tz=final_events_data,
    #     processed_data_folder=processed_folder,
    #     pre_fed_data=[zn15data, tickersymbol],
    #     skip_data_fetching=True,
    #     month_day_filter=[],
    # )
    # print(f"Processed files saved at: {final_data_path}")
    # print(final_data)
