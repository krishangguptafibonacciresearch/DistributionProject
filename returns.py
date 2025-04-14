import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from events import Events
from scipy.stats import percentileofscore
from datetime import datetime

class Returns:
    def __init__(
        self, output_folder="stats_and_plots_folder", dataframe=pd.DataFrame()
    ):
        self.colors = {
            "deep_black": "#000000",
            "golden_yellow": "#FFD700",
            "dark_slate_gray": "#2F4F4F",
            "ivory": "#FFFFF0",
            "fibonacci_blue": "#0066CC",
            "sage_green": "#8FBC8F",
            "light_gray": "#D3D3D3",
        }
        self.sessions = [
            "London 0-7 ET",
            "US Open 7-10 ET",
            "US Mid 10-15 ET",
            "US Close 15-17 ET",
            "Asia 18-24 ET",
            "All day",
        ]
        self.output_folder = output_folder
        self.dataframe = dataframe
        os.makedirs(self.output_folder, exist_ok=True)

    def get_session(self, timestamp):
        hour = timestamp.hour
        # minute = timestamp.minute
        if 18 <= hour < 24:
            return "Asia 18-24 ET"
        elif 0 <= hour < 7:  # or (hour == 6 and minute < 30):
            return "London 0-7 ET"
        elif 7 <= hour < 10:  # or (hour == 6 and minute >= 30):
            return "US Open 7-10 ET"
        elif 10 <= hour < 15:
            return "US Mid 10-15 ET"
        elif 15 <= hour < 17:
            return "US Close 15-17 ET"
        else:
            return "Other"


    def filter_date(
        self,
        filter_df,
        start_date="",
        end_date="",
        month_day_filter=[],
        to_sessions=True,
    ):
        self.month_day_filter=month_day_filter
        df = filter_df.copy()
        if to_sessions == True:
            df["session"] = df["timestamp"].apply(self.get_session)

        if month_day_filter == []:
            if start_date == end_date == "":
                finaldf = df
            elif start_date == "" and end_date != "":
                end_date = pd.to_datetime(end_date).date()
                finaldf = df[(df["timestamp"].dt.date <= end_date)]
            elif start_date != "" and end_date == "":
                start_date = pd.to_datetime(start_date).date()
                finaldf = df[(df["timestamp"].dt.date >= start_date)]
            else:
                start_date = pd.to_datetime(start_date).date()
                end_date = pd.to_datetime(end_date).date()
                finaldf = df[
                    (df["timestamp"].dt.date >= start_date)
                    & (df["timestamp"].dt.date <= end_date)
                ]

        else:
            month = month_day_filter[0]
            day1 = month_day_filter[1]
            day2 = month_day_filter[2]
            finaldf = df[
                (df["timestamp"].dt.month == month)
                & (df["timestamp"].dt.day >= day1)
                & (df["timestamp"].dt.day <= day2)
            ]
        finaldf["year"] = (finaldf["timestamp"].dt.year).astype("Int64")
        finaldf = finaldf.sort_values("timestamp")
        

        return finaldf
    
    def get_descriptive_stats(self,target_csv,target_column):
        
        stats_csv=target_csv[target_column].describe(percentiles=[0.1,0.25,0.5,0.75,0.95,0.99])

        # Add additional statistics to the DataFrame
        stats_csv.loc['mean'] = target_csv[target_column].mean()
        stats_csv.loc['skewness'] = target_csv[target_column].skew()
        stats_csv.loc['kurtosis'] = target_csv[target_column].kurtosis()

        stats_csv.index.name = 'Volatility of Returns Statistic'
        return stats_csv

    # Close Price when the session ended - Open Price when the session started
    def _calculate_return_bps(self, group,bps_factor):
        return abs(group["Close"].iloc[-1]-group["Open"].iloc[0]) * bps_factor
    def _calculate_return_bps2(self, group,bps_factor):
        return (group["Close"].iloc[-1]-group["Open"].iloc[0]) * bps_factor

    def get_daily_session_returns(self, df,bps_factor,target_column='timestamp',columns='NA'):
        
        if columns=='NA':
            returns = (
                df.groupby([df[target_column].dt.date, "session"], group_keys=False)
                .apply(self._calculate_return_bps, bps_factor=bps_factor,include_groups=False)
                .reset_index()
            )
            returns.columns = ["date", "session", "return"]
        else:
            returns.columns=columns
        return returns

    def get_daily_returns(self, df, bps_factor,target_column='timestamp',columns='NA'):
        
        if columns=='NA':
            daily_returns_all = (
            df.groupby(df[target_column].dt.date)
            .apply(self._calculate_return_bps,bps_factor)
            .reset_index()
        )
            daily_returns_all.columns = ["date", "return"]
        else:
            daily_returns_all = (
            df.groupby(df[target_column])
                .apply(self._calculate_return_bps2,bps_factor)
                .reset_index()
            )
             
            daily_returns_all.columns = columns

        return daily_returns_all

    def plot_daily_session_returns(self, filtered_df, tickersymbol_val, interval_val,bps_factor):

        start_date = (filtered_df["timestamp"].dt.date.tolist())[0]
        end_date = (filtered_df["timestamp"].dt.date.tolist())[-1]
      
        sessions = self.sessions

        #fetching the intraday data to make the red dot.
        intraday_data = self.dataframe.copy()
        intraday_data['session'] = intraday_data['timestamp'].apply(self.get_session)
        intraday_data['date'] = intraday_data['timestamp'].dt.date

        plt.figure(figsize=(24, 18))
        sns.set_style("darkgrid")
        list_stats = []

        if 'd' in interval_val:
            sessions=['All day']

        for i, session in enumerate(sessions, 1):

            plt.subplot(3, 2, i)

            latest_return = -1
            latest_date = None

            if session != "All day":
                daily_session_returns = self.get_daily_session_returns(filtered_df,bps_factor)
                session_returns = daily_session_returns[daily_session_returns["session"] == session]["return"]

                # take unfiltered intraday day for the red dot so that actual movement is compared to non-evemt distro.
                session_ret_intraday = self.get_daily_session_returns(intraday_data , bps_factor)
                session_ret_intraday = session_ret_intraday[session_ret_intraday['session'] == session]
                latest_return = session_ret_intraday['return'].iloc[-1]
                latest_date = session_ret_intraday['date'].iloc[-1]
                print('latest date for red dot' , latest_date)


            else:

                daily_returns_all = self.get_daily_returns(filtered_df,bps_factor)
                session_returns = daily_returns_all["return"]

                daily_ret_intraday = self.get_daily_returns(intraday_data ,bps_factor)
                latest_return = session_returns.iloc[-1]
                latest_date = daily_returns_all["date"].iloc[-1]
                print('latest date for red dot' , latest_date)

            # Calculate the percentile of the latest return
            latest_percentile = percentileofscore(
                session_returns.squeeze(), latest_return, kind="rank"
            )

            # Calculate descriptive stats
            mean = session_returns.mean()
            median = session_returns.median()
            perc95 = session_returns.quantile(0.95)
            perc99 = session_returns.quantile(0.99)
            std = session_returns.std()
            skew = session_returns.skew()
            kurt = session_returns.kurtosis()
            zscore=(latest_return-mean)/std
            latest_zscore=round(zscore,2)

            sns.histplot(
                session_returns, kde=True, stat="density", linewidth=0, color="skyblue"
            )
            sns.kdeplot(session_returns, color="darkblue", linewidth=2)

           
            # Add the latest return as a red point
            plt.scatter(latest_return, 0, color="red", s=150, zorder=5)
            plt.annotate(
                f"({latest_date}, Return:{latest_return:.2f}, Zscore: {latest_zscore}, %ile:{latest_percentile:.1f}%)",
                (latest_return, 0),
                xytext=(10, 10),  # Offset text slightly more for clarity
                textcoords="offset points",
                color="red",
                fontweight="bold",
                fontsize=14,  # Increased font size for readability
            )

            # Add a red dotted vertical line to highlight the latest return
            plt.axvline(
                x=latest_return,
                color="red",
                linestyle="--",
                linewidth=1.5,
                alpha=0.7,
                label="Latest Return",
            )
            plt.title(f"{session}", fontsize=18)
            plt.xlabel("Session return in TV bps", fontsize=16)
            plt.ylabel("Density", fontsize=16)

           
            stats_text = f"Mean: {mean:.2f}\nMedian: {median:.2f}\nStd: {std:.1f}\n95%ile: {perc95:.1f}\n99%ile: {perc99:.1f}\nSkew: {skew:.1f}\nKurt: {kurt:.1f}"
            plt.text(
                0.95,
                0.95,
                stats_text,
                transform=plt.gca().transAxes,
                verticalalignment="top",
                horizontalalignment="right",
                bbox=dict(
                    boxstyle="round",
                    facecolor=self.colors["ivory"],
                    edgecolor=self.colors["dark_slate_gray"],
                    alpha=0.8,
                ),
                color=self.colors["dark_slate_gray"],
                fontsize=20,
            )

            list_stats.append(
                session_returns.describe(
                    percentiles=[0.05, 0.25, 0.5, 0.68, 0.90, 0.95, 0.99, 0.997]
                )
            )
        plt.tight_layout()
        month_to_name = (lambda a, b, c: f"Dates filtered: {datetime.strptime(str(a), '%m').strftime('%B')}: {b}-{c}")
        if self.month_day_filter==[]:
            filtered_string=""
        else:
            filtered_string = month_to_name(self.month_day_filter[0],self.month_day_filter[1], self.month_day_filter[2])
        plt.suptitle(
            f"Distribution of Returns {tickersymbol_val} with interval of {interval_val}: ABS(End - Start) across trading sessions: {start_date} to {end_date}.{filtered_string}",
            fontsize=20,
            y=1.02,
            x=0.01,
            ha='left'
        )
        plt.savefig(
            os.path.join(
                self.output_folder,
                f"{tickersymbol_val}_{interval_val}_Returns_Distribution.png", #_{start_date}_{end_date}
            ),
            dpi=300,
            bbox_inches="tight",
        )
        plt.close()

        df_stats = pd.concat(list_stats, axis=1)
        df_stats.columns = sessions
        df_stats.index.name=f'(Return Stats:- Interval:{interval_val}, Symbol:{tickersymbol_val})'
        df_stats.to_csv(
            os.path.join(
                self.output_folder,
                f"{tickersymbol_val}_{interval_val}_Returns_stats.csv",
            )
        )
        #print(df_stats.round(1))

    

    def get_daily_session_volatility_returns(self, df,bps_factor , target_col = 'timestamp'):
        
        session_volatility_df = df.groupby([df[target_col].dt.date, "session"]).agg(
            {"High": ["max"], "Low": ["min"]}
        )
        session_volatility_df["return"] = bps_factor * (
            session_volatility_df["High"]["max"] - session_volatility_df["Low"]["min"]
        )
        session_volatility_df = session_volatility_df.reset_index()
        session_volatility_df.columns = ["date", "session", "high", "low", "return"]
        session_volatility_df = session_volatility_df.sort_values(["date", "session"])
        return session_volatility_df

    def get_daily_volatility_returns(self, df,bps_factor , target_col = 'timestamp'):
        all_df = df.groupby([df[target_col].dt.date]).agg(
            {"High": ["max"], "Low": ["min"]}
        )
        all_df["return"] = bps_factor * (all_df["High"]["max"] - all_df["Low"]["min"])
        all_df = all_df.reset_index()
        all_df.columns = ["date", "high", "low", "return"]
        all_df = all_df.sort_values(["date"])
        return all_df

    def plot_daily_session_volatility_returns(
        self, filtered_df, tickersymbol_val, interval_val,bps_factor
    ):
                
        start_date = (filtered_df["timestamp"].dt.date.tolist())[0]
        end_date = (filtered_df["timestamp"].dt.date.tolist())[-1]

        #fetching intraday data for the red dot.
        # intraday_data = self.fetch_intraday_data(tickersymbol_val , interval_val)
        intraday_data = self.dataframe.copy()
        intraday_data['session'] = intraday_data['timestamp'].apply(self.get_session)
        intraday_data['date'] = intraday_data['timestamp'].dt.date

        latest_return = -1
        latest_date = None
        
        # Analyze distributions
        list_stats = []
        plt.figure(figsize=(24, 18))
        sns.set_style("darkgrid")

        skip_sessions=False
        if 'd' in interval_val:
            sessions=['All day']
            skip_sessions=True
        else:
            sessions = self.sessions
            
        for i, session in enumerate(sessions, 1):

            if skip_sessions==False:
                plt.subplot(3, 2, i)

            if session == "All day":

                all_volatility_df = self.get_daily_volatility_returns(filtered_df,bps_factor=bps_factor)
                session_returns = all_volatility_df["return"]
                latest_custom_days_return = all_volatility_df.iloc[:].loc[  #-15
                    :, ["date", "return"]
                ]

                latest_custom_days_return['ZScore wrt All Days']=(latest_custom_days_return['return']-session_returns.mean())/session_returns.std()
                latest_custom_days_return['ZScore wrt Given Days']=(latest_custom_days_return['return']-latest_custom_days_return['return'].mean())/latest_custom_days_return['return'].std()
                
                latest_custom_days_return_stats=self.get_descriptive_stats(latest_custom_days_return,'return')
                latest_custom_days_return_stats.name=f'(Session:{session}, Interval:{interval_val}, Symbol:{tickersymbol_val})'

                latest_custom_days_return.rename(columns={'date':'Date','return':f'Volatility of Returns (Session:{session}, Interval:{interval_val}, Symbol:{tickersymbol_val})'},inplace=True)
                latest_custom_days_return.to_csv(os.path.join(self.output_folder,
                    f"{"_".join(str(session).split())}_latest_custom_days_Volatility_Returns_{interval_val}_{tickersymbol_val}.csv"),index=False
                )
                latest_custom_days_return_stats.to_csv(os.path.join(self.output_folder,
                    f"{"_".join(str(session).split())}_latest_custom_days_Volatility_Returns_{interval_val}_{tickersymbol_val}_stats.csv")
                )
                
                # data for the red dot.
                vol_ret_all_day_intraday = self.get_daily_volatility_returns(intraday_data , bps_factor)
                latest_date = vol_ret_all_day_intraday['date'].iloc[-1]
                latest_return = vol_ret_all_day_intraday['return'].iloc[-1]

                # latest_return = session_returns.iloc[-1]
                latest_zscore=latest_custom_days_return['ZScore wrt All Days'].iloc[-1]
                # latest_date = all_volatility_df["date"].iloc[-1]
                
            else:

                session_volatility_df = self.get_daily_session_volatility_returns(filtered_df,bps_factor)
                
                # distribution is a plot of session_returns.
                session_returns = session_volatility_df.loc[session_volatility_df["session"] == session, ["return"]]
                
                # latest_return = session_returns.iloc[-1, 0]
                # latest_date = session_volatility_df.loc[
                #     session_volatility_df["session"] == session, "date"
                # ].iloc[-1]

                latest_custom_days_return = session_volatility_df.loc[session_volatility_df['session']==session].iloc[:].loc[ 
                    :, ["date", "return"]
                ] #-15
               
                latest_custom_days_return['ZScore wrt All Days']=(latest_custom_days_return['return']-session_returns['return'].mean())/session_returns['return'].std()
                latest_custom_days_return['ZScore wrt Given Days']=(latest_custom_days_return['return']-latest_custom_days_return['return'].mean())/latest_custom_days_return['return'].std()

                latest_custom_days_return_stats=self.get_descriptive_stats(latest_custom_days_return,'return')
                latest_custom_days_return_stats.name=f'(Session:{session}, Interval:{interval_val}, Symbol:{tickersymbol_val})'
                
                latest_custom_days_return.rename(columns={'date':'Date','return':f'Volatility of Returns (Session:{session}, Interval:{interval_val}, Symbol:{tickersymbol_val})'},inplace=True)
                latest_custom_days_return.to_csv(os.path.join(self.output_folder,
                    f"{"_".join(str(session).split())}_latest_custom_days_Volatility_Returns_{interval_val}_{tickersymbol_val}.csv"),index=False
                )

                latest_custom_days_return_stats.to_csv(os.path.join(self.output_folder,
                    f"{"_".join(str(session).split())}_latest_custom_days_Volatility_Returns_{interval_val}_{tickersymbol_val}_stats.csv")
                )

                latest_zscore=latest_custom_days_return['ZScore wrt All Days'].iloc[-1]
                
                # Data for the red dot.
                session_vol_ret_intraday = self.get_daily_session_volatility_returns(intraday_data , bps_factor)
                session_vol_ret_intraday = session_vol_ret_intraday[session_vol_ret_intraday['session'] == session]
                latest_return = session_vol_ret_intraday['return'].iloc[-1]
                latest_date = session_vol_ret_intraday['date'].iloc[-1]

            # Calculate the percentile of the latest return
            latest_percentile = percentileofscore(session_returns.squeeze(), latest_return, kind="rank")

            # Descriptive Statistics
            mean = session_returns.mean()
            median = session_returns.median()
            std = session_returns.std()
            perc95 = session_returns.quantile(0.95)
            perc99 = session_returns.quantile(0.99)
            skew = session_returns.skew()
            kurt = session_returns.kurtosis()

            # zscore=(session_returns-mean)/std
            latest_zscore=round(latest_zscore,2)

            sns.histplot(
                session_returns, kde=True,stat="density",linewidth=0, color="skyblue"
            )
            sns.kdeplot(session_returns, color="darkblue", linewidth=2)
            # Add the latest return as a red point
            plt.scatter(latest_return, 0, color="red", s=150, zorder=5)
            #plt.scatter(mean,0,color='black',s=150,zorder=5)

            plt.annotate(
                f"({latest_date}, VoltyReturn:{latest_return:.2f}, Zscore:{latest_zscore}, {latest_percentile:.1f}%ile)",
                (latest_return, 0),
                xytext=(10, 10),  # Offset text slightly more for clarity
                textcoords="offset points",
                color="red",
                fontweight="bold",
                fontsize=14,  # Increased font size for readability
            )
            # Add a red dotted vertical line to highlight the latest return
            plt.axvline(
                x=latest_return,
                color="red",
                linestyle="--",
                linewidth=1.5,
                alpha=0.7,
                label="Latest Volty. Return",
            )


            plt.title(f"{session}", fontsize=18)
            plt.xlabel("Session return in TV bps", fontsize=16)
            plt.ylabel("Density", fontsize=16)
            plt.legend("", frameon=False)

            


            if isinstance(session_returns, pd.DataFrame):
                mean, median, std, perc95, perc99, skew, kurt = [
                    x.iloc[0] for x in [mean, median, std, perc95, perc99, skew, kurt]
                ]

            list_stats.append(
                session_returns.describe(
                    percentiles=[0.05, 0.25, 0.5, 0.68, 0.90, 0.95, 0.99, 0.997]
                )
            )

            stats_text = f"Mean: {mean:.2f}\nMedian: {median:.2f}\nStd: {std:.1f}\n95%ile: {perc95:.1f}\n99%ile: {perc99:.1f}\nSkew: {skew:.1f}\nKurt: {kurt:.1f}\n"
            plt.text(
                0.95,
                0.95,
                stats_text,
                transform=plt.gca().transAxes,
                verticalalignment="top",
                horizontalalignment="right",
                bbox=dict(
                    boxstyle="round",
                    facecolor=self.colors["ivory"],
                    edgecolor=self.colors["dark_slate_gray"],
                    alpha=0.8,
                ),
                color=self.colors["dark_slate_gray"],
                fontsize=20,
            )

        
        plt.tight_layout()
        month_to_name = lambda a, b, c: f"Dates filtered: {datetime.strptime(str(a), '%m').strftime('%B')}: {b}-{c}"
        if self.month_day_filter==[]:
            filtered_string=""
        else:
            filtered_string = month_to_name(self.month_day_filter[0],self.month_day_filter[1], self.month_day_filter[2])
        plt.suptitle(
            f"Distribution of Volatility {tickersymbol_val} with interval of {interval_val}: (High - Low) across trading sessions: {start_date} to {end_date}.{filtered_string}",
            fontsize=20,
            y=1.02,
            x=0.01,
            ha='left'
        )

        
        plt.savefig(
            os.path.join(
                self.output_folder,
                f"{tickersymbol_val}_{interval_val}_Volatility_Distribution.png",
            ),
            dpi=300,
            bbox_inches="tight",
        )
        plt.close()

        df_stats = pd.concat(list_stats, axis=1)
        df_stats.columns = sessions
        df_stats.index.name=f'(Vol Return Stats:- Interval:{interval_val}, Symbol:{tickersymbol_val})'
        df_stats.to_csv(
            os.path.join(
                self.output_folder,
                f"{tickersymbol_val}_{interval_val}_Volatility_Returns_stats.csv",
            )
        )
        #print(df_stats.round(1))


    def tag_events(self, ev, pc):
        events_df = ev.copy()
        price_df = pc.copy()

        # Prepare events DataFrame
        events_df["timestamp"] = events_df["datetime"]
       

        # Prepare price DataFrame
        price_df.index.name = None
        price_df.reset_index(inplace=True)
        price_df["timestamp"] = price_df["timestamp"]
        price_df = price_df.sort_values("timestamp")
        events_df = events_df.sort_values("timestamp")

        # Outer merge based on timestamp
        # print('pricedf',price_df)
        # print('events_df',events_df)
        price_df["timestamp"] = price_df["timestamp"].dt.tz_localize(None)
        events_df['timestamp'] = events_df["timestamp"].dt.tz_localize(None)
        
        final_df = pd.merge(price_df, events_df, on="timestamp", how="outer")

        # Sort the final DataFrame by timestamp
        final_df = final_df.sort_values("timestamp")
        final_df.dropna(how="all", inplace=True)
        final_df.index = final_df["index"]
        final_df.index.name = pc.index.name
        final_df.drop("index", axis=1, inplace=True)

        final_df["year"] = (final_df["timestamp"].dt.year).astype("Int64")
        final_df = final_df.sort_values("timestamp")

        common_columns = ["timestamp", "year", "session"]
        # Separate the columns of the two DataFrames
        events_columns = [col for col in events_df.columns if col not in common_columns]
        price_columns = [col for col in price_df.columns if col not in common_columns]
        remove_columns = ["datetime", "index"]

        # Combine the desired order
        ordered_columns = common_columns + events_columns + price_columns
        ordered_columns = [i for i in ordered_columns if i not in remove_columns]
        final_df = final_df.reindex(columns=ordered_columns, fill_value="na")
        return final_df