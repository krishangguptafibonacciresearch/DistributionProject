# Custom Functions
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
from returns import Returns
from returns_main import ticker_match_tuple

def _calculate_return_bps(group):
        return (group["Close"].iloc[-1]-group["Open"].iloc[0]) * 16

def get_session_returns(df,name):
        returns = (
            df.groupby(["US/Eastern Timezone"], group_keys=False)
            .apply(_calculate_return_bps, include_groups=False)
            .reset_index()
        )
        returns.columns = ["US/Eastern Timezone",name]
        return returns

def movement(movement_type, df):
    df = df.copy()  # Ensures we don't modify original DataFrame

    if movement_type == 'Up':
        df = df[df[df.columns[-1]] >= 0]

    elif movement_type == 'Down':
        df = df[df[df.columns[-1]] <= 0]
        df.loc[:, df.columns[-1]] = np.where(df[df.columns[-1]] <0, df[df.columns[-1]] * -1, 0) #creates a copy

    elif movement_type == 'Absolute':
        df.loc[:, df.columns[-1]] = np.abs(df[df.columns[-1]]) #creates a copy

    else:
        raise ValueError("Invalid Movement Type. Choose 'Up', 'Down', or 'Absolute'.")

    return df.reset_index(drop=True)

     
def get_dataframe(interval,ticker_name,folder):
    for file in os.scandir(folder):
       if file.is_file():
          if all(x in str(file.name) for x in [interval, ticker_name]) and file.name.endswith('.csv'):
              df=pd.read_csv(os.path.join(folder,file.name))
              print('Mydf:',df)
              break
    return df

def filter_dataframe(pre_df,filter_list="",day_dict="",timezone_column="",target_timezone="",interval="",ticker=""):
    # Filters based on "US/Eastern Timezone" column at the end.
    if ticker not in ['FGBL']:
        pre_df[timezone_column] = pd.to_datetime(pre_df['Datetime'], errors='coerce')
        pre_df[timezone_column] = pre_df[timezone_column].dt.tz_convert(target_timezone)
        # Filter day and date
        pre_df['Day']=pre_df['US/Eastern Timezone'].dt.day_name()
    

    finaldf = []
    if filter_list:
        pre_df['Group']="Not Allotted"

        for se in filter_list:
            start=se[0]
            start_day=se[2]
            next_time=se[1]
            if next_time<=0:
                next_time=1

            ET_col=pre_df.columns[-3]
            
            unit = 'hours' if 'h' in interval else 'minutes' if 'm' in interval else None
            condition = pre_df[ET_col].dt.hour == start if 'h' in interval else pre_df[ET_col].dt.minute == start
            if start_day:
                condition &= pre_df[ET_col].dt.day_name() == start_day
            
            start_times = pre_df[condition][ET_col]
            if unit is not None: # interval is 'h' or 'm'
                for index, time in enumerate(start_times):
                    time_delta = pd.Timedelta(**{unit: next_time})
                    current_df = pre_df[(pre_df[ET_col] >= time) & (pre_df[ET_col] < time + time_delta)]
                    current_df.loc[:, 'Group'] = index
                    finaldf.append(current_df)
            else: # interval is 'd'
                pre_df['Group']=pre_df.index
                return pre_df.reset_index(drop=True)

        
        pre_df=pd.concat(finaldf)
        pre_df=pre_df[pre_df['Group']!="Not Allotted"]
        pre_df.drop_duplicates(inplace=True)
        pre_df.reset_index(drop=True,inplace=True)
        
    else:
        pre_df['Group']=pre_df.index
        print(pre_df)
            
    return pre_df.reset_index(drop=True)


def calculate_stats_and_plots(df,name,version,check_movement,interval,ticker,target_column):
    my_df=df.copy()
    # Calculate Session Return close(last entru) - open(first entry)
    my_returns_object=Returns(output_folder='tab4_files',dataframe=df)
    for tup in ticker_match_tuple:
        if tup[0]==ticker:
            bps_factor=tup[2]
            break
    if interval!='1d':
        returns=my_returns_object.get_daily_returns(my_df,bps_factor,target_column,columns=[target_column,name])
    else:
        returns=my_returns_object.get_daily_returns(my_df,bps_factor,target_column,columns=[target_column,name])

    if version in ['Absolute','Up','Down']:
        returns=movement(version,returns)
    print(f'{name} Session Returns: {returns}')
    
    # Calculate statistics for given scenario
    my_pctiles=[0.1,0.25,0.5,0.75,0.9,0.95,0.99]
    returns_stats=returns[name].describe(percentiles=my_pctiles)
    print(returns_stats)
    current_bps=check_movement
    percentile=(returns[name] <= current_bps).mean() * 100
    percentile=round(percentile,2)
    zscore= (current_bps-returns[name].mean())/returns[name].std()
    zscore=round(zscore,2)
    print(f'Z Score for bps<={current_bps}: {zscore}')
    print(f'Prob bps<={round(current_bps,2)}: {percentile}%ile')
    print(f'Prob bps>{round(current_bps,2)}: {100-percentile}%ile')

    # Plot the return probability along with ZScore
    plt.figure(figsize=(10, 6))
    sns.kdeplot(data=returns, x=f"{name}",cumulative=True,fill=True,color='blue')

    plt.title(f'{name}', fontdict={'fontsize': 8, 'fontweight': 'bold'})# 'fontname': 'Arial'})
    plt.xlabel('Return(bps)')
    plt.ylabel('Cumulative Probability')

    # Set the statistics on the graph
    y_value=percentile/100
    # Restrict the vertical line to stay within the graph
    plt.axvline(x=current_bps, color='red', linestyle='--', label=f'X: {current_bps}')

    # Add a horizontal line at the corresponding probability value
    plt.axhline(y=y_value, xmin=0, xmax=1, color='green', linestyle='--', label=f'P(bps<={current_bps}):{percentile}%')

    # Annotate the intersection point
    plt.annotate(f'(Pr bps<={current_bps}, {percentile:.2f}%ile)',
                xy=(current_bps, y_value),
                xytext=(current_bps + 2, y_value + 0.02),
                color='black',
                arrowprops=dict(facecolor='black', arrowstyle='->'))
    plt.scatter(x=current_bps,y=y_value,color='black',alpha=1)

    percentiles=returns_stats.to_frame()
    percentiles.columns=['bps']
    mean = percentiles.loc['mean', 'bps']
    std = percentiles.loc['std', 'bps']
    perc0 = percentiles.loc['min', 'bps']
    perc10 = percentiles.loc['10%', 'bps']
    perc25 = percentiles.loc['25%', 'bps']
    perc50 = percentiles.loc['50%', 'bps']
    perc75 = percentiles.loc['75%', 'bps']
    perc90 = percentiles.loc['90%', 'bps']
    perc95 = percentiles.loc['95%', 'bps']
    perc99 = percentiles.loc['99%', 'bps']
    perc100 = percentiles.loc['max', 'bps']
    stats_text = f"""Mean: {mean:.2f} bps
                    Std: {std:.1f} bps
                    0%ile (Min): {perc0:.2f} bps
                    10%ile: {perc10:.2f} bps
                    25%ile: {perc25:.2f} bps
                    50%ile (Median): {perc50:.2f} bps
                    75%ile: {perc75:.2f} bps
                    90%ile: {perc90:.1f} bps
                    95%ile: {perc95:.1f} bps
                    99%ile: {perc99:.1f} bps
                    100%ile (Max): {perc100:.2f} bps"""
    plt.text(
            0.95,
            0.75,
            stats_text,
            transform=plt.gca().transAxes,
            verticalalignment="top",
            horizontalalignment="right",
            bbox=dict(
                boxstyle="round",
                facecolor="#FFFFF0",
                edgecolor="#2F4F4F",
                alpha=0.8,
            ),
            color="#000000",
            fontsize=10,
        )
    plt.show()
    
    custom_dic={}
    for key,val in zip(['df','stats','%<=','%>','zscore<=','plot'],
                       [returns,returns_stats,percentile,100-percentile,zscore,plt]):
        custom_dic[key]=val
    return custom_dic

if __name__=='__main__':
    df=(filter_dataframe(get_dataframe(interval='1h',ticker_name='ZN',folder='Intraday_data_files'),timezone_column='US/Eastern Timezone',target_timezone='US/Eastern'))
    calculate_stats_and_plots(df=df,name='testing',version='No-Version',check_movement=1,interval='1h',ticker='ZN',target_column=
                              df.columns[-3])
