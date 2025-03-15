import pandas as pd
import numpy as np
class Nonevents:
    """
    Filters the data based on events and non events

    """
    def __init__(self,dataframe):
        self.dataframe=dataframe    #dataframe contains the event time stamps, sessions, tiers only. No price data included (filtered_df is this).

    def filter_nonevents(self,df):
        df['timestamp'] = pd.to_datetime(df['timestamp'])  # Ensure timestamp is datetime
        df['date'] = df['timestamp'].dt.date  # Extract date
        df['IND_NE_remove'] = 0  # Initialize with 0

        # Flag 'ind_ne' for entire day when IND_Tier1 is 1
        tier1_dates = df.loc[df['IND_Tier1'] == 1, 'date'].unique()
        df.loc[df['date'].isin(tier1_dates), 'IND_NE_remove'] = 1

        # Handle time windows for IND_Tier2, IND_Tier3, and IND_FED
        def flag_time_window(tier_col, time_window):
            flags = np.zeros(len(df), dtype=int)
            for _, row in df.iterrows():
                if row[tier_col] == 1:
                    start_time = row['timestamp'] - time_window
                    end_time = row['timestamp'] + time_window
                    within_window = (df['timestamp'] >= start_time) & (df['timestamp'] <= end_time)
                    flags |= (within_window).values #bit-wise or
            return flags

        # Apply time-based flagging
        df['ind_tier2'] = flag_time_window('IND_Tier2', pd.Timedelta(minutes=30))
        df['ind_tier3'] = flag_time_window('IND_Tier3', pd.Timedelta(minutes=15))
        df['ind_fed'] = flag_time_window('IND_FED', pd.Timedelta(minutes=30))

        # Combine all flags
        df['IND_NE_remove'] = df[['IND_NE_remove', 'ind_tier2', 'ind_tier3', 'ind_fed']].max(axis=1)

        # Clean up intermediate columns
        df.drop(['ind_tier2', 'ind_tier3', 'ind_fed', 'date'], axis=1, inplace=True)
        
        return df
