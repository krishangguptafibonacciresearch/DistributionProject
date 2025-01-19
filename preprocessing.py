import pandas as pd
class ManipulateTimezone:
    """
    Pre-process the Historical Data to convert to desired timezone
    if default data is not in the desired timezone
    """
    def __init__(self,data):
        self.dataframe=pd.DataFrame(data)

    def _check_timezone(self, checkdf="", tz_col="", default_tz = "Asia/Kolkata", target_tz = "US/Eastern"):
        """
        Checks the timezone of a timestamp column in the DataFrame and 
        converts it to target timezone

        Args:
            dataframe (pd.DataFrame): Instrument Data with intra-day data in a Pandas DataFrame.
            tz_col (str, optional): Name of the column containing datetime values.
                                    If not provided, the method will attempt to detect it.

        Returns:
            pd.DataFrame: DataFrame with the timezone converted to US/Eastern.
        """
        # Automatically detect the timestamp column if not provided
        dataframe=checkdf.copy()

        if not tz_col:
            for col in dataframe.columns:
                col_name = str(col).strip().lower()
                if col_name in ['timestamp', 'datetime']:
                    tz_col = col
                    break

        if not tz_col:
            raise ValueError("No timestammp column found. Please specify the 'tz_col' argument.")

        # Convert column to datetime format
        dataframe[tz_col]=pd.to_datetime(dataframe[tz_col])

        # Apply timezone conversion
        dataframe[tz_col]=dataframe[tz_col].apply(
            lambda tz_info:self._convert_timezone(tz_info,default_tz,target_tz))
        return dataframe
    
    @staticmethod
    def _convert_timezone(tz_info, default_tz, target_tz):
        """
        Converts a single timestamp to the target timezone.

        Args:
            tz_info (pd.Timestamp): A timestamp value.
            default_tz (str): The default timezone to localize naive timestamps.
            target_tz (str): The target timezone for conversion.

        Returns:
            pd.Timestamp: Timestamp converted to the target timezone.
        """
        if tz_info.tzinfo is None:  # Check if timezone is missing
            tz_info = tz_info.tz_localize(default_tz)
        tz_info=tz_info.tz_convert(target_tz)
        return tz_info
    

    def change_timezone(self,checkdf,tz_col, default_tz,target_tz):
        return self._check_timezone(checkdf,tz_col, default_tz,target_tz)
    

    @staticmethod
    def add_time_for_d_intervals(day_interval_dataframe,target_col):
        # Convert 1d interval dataframe to datetime. It adds  00:00:00 by default since no time value.
        day_interval_dataframe[target_col] = pd.to_datetime(day_interval_dataframe[target_col], errors='coerce')

        # Change time of rows to 23:59:59
        day_interval_dataframe[target_col] = day_interval_dataframe[target_col].apply(lambda x: x.replace(hour=23, minute=59, second=59))
        
        return day_interval_dataframe
    

    
if __name__=='__main__':
    myobj=ManipulateTimezone(pd.DataFrame())
    print(myobj)