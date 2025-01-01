Distribution Project Repository

A. Description:
1. This repository is designed to manage, process, and analyze intraday data for various financial instruments.
2. The project automates the fetching, processing, and statistical analysis of instrument data using Python scripts. 


B. Components:
These are the two independent components of the project:
1. Data Fetching: Fetching and maintaining intraday data values for different financial instruments (can be modified in periodic_runner_main.py)
2. Distribution of Returns: Involves data cleaning, manipulation and plotting of returns data for different historical data. The plotting can be done for data fetched by component 1 or any external historical data file fetched from TradingView, Yahoo Finance, etc.


C. Methodology
1. Data Fetching: This fetches data from Yahoo Finance for different instruments for different intervals. Eg. ZN_1m: Historical data for ZN with 1m interval, ZN_1h, ZT_1m...
2. Distribution of Returns:

   This involves the following steps:
   1. Converting the fetched data into the target timezone. (UST, Eastern, IST, etc) 
   2. Maintaining an events file with important event details. 
      Only the US Events (2015-2024) have been maintained yet.
   3. Filtering the historical data based on events/non events to get trend of 
      returns during non events.
   4. Plotting the returns

D. Code Files
1. Data Fetching: "periodic_runner_main.py"
2. Distribution of Returns: "returns_main.py" handles all the other files except "temp.py"

E. Folders
1. Data Fetching: 
"Daily_backup_files" folder Just stores data collected regularly.

"Intraday_data_files" folder stores bundled intraday data files for the financial instruments fetched by "periodic_runner_main.py" file. The automation file i.e "main.yml" can be modified to increase the data fetching frequency.

2. Distribution of Returns:
"Input_data" folder contains the historical data that may be used if not from "Intraday_data_files".

"Processed_data" contains the manipulated data after manipulating the historical data. The manipulation could be changing the timezone, filtering the data based on events or tagging the data based on nonevents.

"stats_and_plots_folder" contains the plots for returns.



