import pandas as pd
from intradaydata import Intraday
from preprocessing import Preprocessing
from events import Events
from returns import Returns
# Preparing Events To Tag With Returns
excelpath='all_events_no_tz.xlsx'
combined_excel_path='all_events_combined_no_tz.csv'
combined_excel_target_tz_path='all_events_combined_target_tz.csv'
mydf=pd.read_excel(excelpath,sheet_name=None)
myevents=Events(excelpath)
combined_excel=myevents.combined_excel
myevents.save_sheet(combined_excel,combined_excel_path)

myeventsobject=Preprocessing(pd.read_csv(combined_excel_path))
combined_excel_target_tz=myeventsobject.check_timezone(myeventsobject.dataframe,
                                tz_col="datetime", default_tz = "Asia/Kolkata", target_tz = "US/Eastern")
combined_excel_target_tz=combined_excel_target_tz[['datetime','events','year']]
myevents.save_sheet(combined_excel_target_tz,combined_excel_target_tz_path)

# #1. Case1: ZN Intraday, 1h interval data
# # Data acquisition: 
hourly_data_object=Intraday(tickers=["ZN=F","ZT=F"],interval='1h',start_intraday=729,end_intraday=1)
tickersymbol=hourly_data_object.dict_symbols['ZN=F'][0]
interval    =hourly_data_object.interval
hdata       =hourly_data_object.fetch_data_yfinance(['ZN=F'])
hdata=list(hdata.values())[0]
hdata=hourly_data_object.data_acquisition(hdata)


# Data pre-processing:
# 1. Change timezone to eastern
hdata_preprocessed_object=Preprocessing(hdata)
hdata_target_tz=hdata_preprocessed_object.check_timezone(checkdf=hdata,tz_col='timestamp',default_tz = "UTC", target_tz = "US/Eastern")
#print(hdata_target_tz)

# 2. Filter only December last two week data for respective years
hdata_returns_object=Returns(dataframe=hdata_target_tz)
hdata_returns_outputfolder=hdata_returns_object.output_folder
hdata_returns_filtered_df=hdata_returns_object.filter_data(filter_df=hdata_target_tz, month_day_filter=[12,15,31],to_sessions=True)
#print(hdata_returns_filtered_df)

# 3. Tagging events with the data using the Events sheet 
tagged_hdata_returns_filtered_df=hdata_returns_object.tag_events(combined_excel_target_tz,hdata_returns_filtered_df)
myevents.save_sheet(tagged_hdata_returns_filtered_df,f'{tickersymbol}_{interval}_filtered_events_tagged.csv')



# Data Visualization:
# 1. Daily Session Returns
hdata_daily_session_returns=hdata_returns_object.get_daily_session_returns(tagged_hdata_returns_filtered_df)
print(hdata_daily_session_returns)
hdata_returns_object.plot_daily_session_returns(tagged_hdata_returns_filtered_df,tickersymbol,interval)

# 2. Daily Session Volatility Returns
hdata_returns_object.plot_daily_session_volatility_returns(tagged_hdata_returns_filtered_df,tickersymbol,interval)

#--------------------------------------
# CASE 2: ZN Intraday, 1d interval data
# Data acquisition: 
daily_data_object=Intraday(tickers=["ZN=F","ZT=F"],interval='1d')
tickersymbol=daily_data_object.dict_symbols['ZN=F'][0]
interval    =daily_data_object.interval
ddata       =daily_data_object.fetch_data_yfinance(['ZN=F'])
ddata=list(ddata.values())[0]
ddata=daily_data_object.data_acquisition(ddata)

# Data pre-processing:
# 1. Change timezone to eastern
ddata_preprocessed_object=Preprocessing(ddata)
ddata_target_tz=ddata_preprocessed_object.check_timezone(checkdf=ddata,tz_col='timestamp',default_tz = "UTC", target_tz = "US/Eastern")
#print(ddata_target_tz)

# 2. Filter only December last two week data for respective years
ddata_returns_object=Returns(dataframe=ddata_target_tz)
ddata_returns_outputfolder=ddata_returns_object.output_folder
ddata_returns_filtered_df=ddata_returns_object.filter_data(filter_df=ddata_target_tz, month_day_filter=[12,15,31],to_sessions=True)
#print(ddata_returns_filtered_df)

# 3. Tagging events with the data using the Events sheet 
tagged_ddata_returns_filtered_df=ddata_returns_object.tag_events(combined_excel_target_tz,ddata_returns_filtered_df)
myevents.save_sheet(tagged_ddata_returns_filtered_df,f'{tickersymbol}_{interval}_filtered_events_tagged.csv')


# Data Visualization:
# 1. Daily Returns
ddata_daily_returns=ddata_returns_object.get_daily_returns(tagged_ddata_returns_filtered_df)
print(ddata_daily_returns)

# 2. Daily Volatility Returns
ddata_returns_object.plot_daily_volatility_returns(tagged_ddata_returns_filtered_df,tickersymbol,interval)

#---------------------------
# CASE 3: ZN Intraday, 1m interval data
# Data acquisition: 
zn1data=pd.read_csv('ZN_1m.csv')
zn1data=zn1data.dropna(axis=0)
zn1data['timestamp']=zn1data['Datetime']
zn1data.reset_index(drop=True,inplace=True)
tickersymbol='ZN'
interval='1m'
# print(zn1data)

# Data Pre-processing:
#1. Change timezone to eastern
zn1data_preprocessed_object=Preprocessing(zn1data)
zn1data_target_tz=zn1data_preprocessed_object.check_timezone(checkdf=zn1data,tz_col="timestamp",default_tz="UTC",target_tz='US/Eastern')
print(zn1data_target_tz)

# # 2. Filter only December last two week data for respective years
zn1data_returns_object=Returns(dataframe=zn1data_target_tz)
zn1data_returns_outputfolder=zn1data_returns_object.output_folder
zn1data_returns_filtered_df=zn1data_returns_object.filter_data(filter_df=zn1data_target_tz, month_day_filter=[],to_sessions=True)
print(zn1data_returns_filtered_df)

# 3. Tagging events with the data using the Events sheet 
tagged_zn1data_returns_filtered_df=zn1data_returns_object.tag_events(combined_excel_target_tz,zn1data_returns_filtered_df)
myevents.save_sheet(tagged_zn1data_returns_filtered_df,f'{tickersymbol}_{interval}_filtered_events_tagged.csv')


# Data Visualization:
# 1. Daily session Returns
zn1data_daily_session_returns=zn1data_returns_object.get_daily_session_returns(tagged_zn1data_returns_filtered_df)
print(zn1data_daily_session_returns)
zn1data_returns_object.plot_daily_session_returns(tagged_zn1data_returns_filtered_df,tickersymbol,interval)

# 2. Daily session Volatility Returns
zn1data_returns_object.plot_daily_session_volatility_returns(tagged_zn1data_returns_filtered_df,tickersymbol,interval)


#---------------------------
# CASE 4: ZN Intraday, 15min interval data
# Data acquisition: 
zn15data=pd.read_csv('ZN_15m.csv')
zn15data=zn1data.dropna(axis=0)
zn15data.reset_index(drop=True,inplace=True)
tickersymbol='ZN'
interval='15m'


# Data Pre-processing:
#1. Change timezone to eastern
zn15data_preprocessed_object=Preprocessing(zn15data)
zn15data_target_tz=zn15data_preprocessed_object.check_timezone(checkdf=zn15data,tz_col="timestamp",default_tz="Unknown",target_tz='US/Eastern')
print(zn15data_target_tz)

# # 2. Filter only December last two week data for respective years
zn15data_returns_object=Returns(dataframe=zn15data_target_tz)
zn15data_returns_outputfolder=zn15data_returns_object.output_folder
zn15data_returns_filtered_df=zn15data_returns_object.filter_data(filter_df=zn15data_target_tz, month_day_filter=[],to_sessions=True)
print(zn15data_returns_filtered_df)

# 3. Tagging events with the data using the Events sheet 
tagged_zn15data_returns_filtered_df=zn15data_returns_object.tag_events(combined_excel_target_tz,zn15data_returns_filtered_df)
myevents.save_sheet(tagged_zn15data_returns_filtered_df,f'{tickersymbol}_{interval}_filtered_events_tagged.csv')


# Data Visualization:
# 1. Daily session Returns
zn15data_daily_session_returns=zn15data_returns_object.get_daily_session_returns(tagged_zn15data_returns_filtered_df)
print(zn15data_daily_session_returns)
zn15data_returns_object.plot_daily_session_returns(tagged_zn15data_returns_filtered_df,tickersymbol,interval)

# 2. Daily session Volatility Returns
zn15data_returns_object.plot_daily_session_volatility_returns(tagged_zn15data_returns_filtered_df,tickersymbol,interval)