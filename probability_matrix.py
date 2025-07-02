import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import os
from returns_main import folder_processed_pq
import re

def GetMatrix(target_bps,target_hrs,interval,ticker_name , data_type , version='NA'):
    df=pd.DataFrame()

    # used when unfiltered intraday data is to be taken,
    pattern = re.compile(r"Intraday_data_ZN_1h_2022-12-20_to_(\d{4}-\d{2}-\d{2})\.parquet")

    # Scan the desired folder for the non-events file with 1 hr interval and converted to target timezone
    if(data_type == 'Non-Event'):
      for file in os.scandir(folder_processed_pq):
        if file.is_file():
            print(file.name)
            if all(x in str(file.name) for x in [interval, ticker_name, 'nonevents','target_tz']) and file.name.endswith('.parquet'):
              print("data used for Probabilty Matrix: " , file.name)
              df=pd.read_parquet(os.path.join(folder_processed_pq,file.name))
    else:
       for file in os.scandir("Intraday_data_files_pq"):
          if file.is_file():
            match = pattern.match(file.name)
            if match:
               print("data used for Probabilty Matrix: " , file.name)
               df = pd.read_parquet(os.path.join("Intraday_data_files_pq" , file.name))

    # Store probability, graph and probability matrix for all the three versions
    if version=='NA':
        version_dic={'Absolute':{},
                    'Up':{},
                    'Down':{},
                    "OH_OL_plot":{}}
    else:
       version_dic={version:{} , 
                    "OH_OL_plot":{}}

    my_matrix=ProbabilityMatrix(df)
    for ver in list(version_dic.keys()):
        print(ver)
        if(ver != "OH_OL_plot"):
          my_grph1 , my_grph2 = my_matrix.calc_prob(target_bps,target_hrs,ver)
          version_dic[ver]['<=%']=my_matrix.less_than_equal_percentile
          version_dic[ver]['>%']=my_matrix.greater_than_percentile
          version_dic[ver]['Matrix']=my_matrix.greater_than_prob_matrix
          version_dic[ver]['Plot']=my_grph1  #my_matrix.my_prob_graph 
          version_dic['OH_OL_plot']['Plot'] = my_grph2 
    return version_dic
        
    
class ProbabilityMatrix:
    def __init__(self, df):
        self.df = df
        self.N=len(df)
        self.less_than_equal_percentile=None 
        self.greater_than_percentile=None
        self.greater_than_prob_matrix = None

    def _round_off(self,np_data):
      # Round off the decimal part to the nearest half
      rounded = (np.floor(np_data) + np.round((np_data - np.floor(np_data)) * 2) / 2) #for quartile->4
      return rounded

    def _plot_prob(self,bps_df,percentile,percentiles,target_bps,target_hrs,version):
      # Plot the histogram
      plt.figure(figsize=(10, 6))
      sns.kdeplot(bps_df['bps'], color='blue', fill=True, cumulative=True) #Now shows cumulative probability i.e cdf

      # Add title and labels
      plt.title(f'Probability Distribution for BPS ({version}): {target_bps} bps in {target_hrs} hrs')
      plt.xlabel('Basis Points (bps)')
      plt.ylabel('Cumulative Probability')

      # Get the probability (y-value) at 'current_bps' from KDE
      y_value=percentile/100

      # Restrict the vertical line to stay within the graph
      plt.axvline(x=target_bps, color='blue', linestyle='--', label=f'Target bps: {abs(target_bps)}')

      # Add a horizontal line at the corresponding probability value
      plt.axhline(y=y_value, xmin=0, xmax=1, color='green', linestyle='--', label=f'Pr(bps ({version}) > {abs(target_bps)} ) = {100-percentile:.2f}%')

      # Annotate the intersection point
      plt.annotate(f'Pr(bps ({version}) <= {abs(target_bps)} ) = {percentile:.2f}%',
                  xy=(abs(target_bps), y_value),
                  xytext=(target_bps +2, y_value +0.03),
                  color='red',
                  arrowprops=dict(facecolor='red', arrowstyle='->'))
      plt.scatter(x=target_bps,y=y_value,color='red',alpha=1)

      # Add stats into the plot
      mean = percentiles.loc['mean', 'bps']
      std = percentiles.loc['std', 'bps']
      perc0 = percentiles.loc['min', 'bps']
      perc10 = percentiles.loc['10%', 'bps']
      perc25 = percentiles.loc['25%', 'bps']
      perc50 = percentiles.loc['50%', 'bps']
      perc75 = percentiles.loc['75%', 'bps']
      perc95 = percentiles.loc['95%', 'bps']
      perc99 = percentiles.loc['99%', 'bps']
      perc100 = percentiles.loc['100%', 'bps']
      stats_text = f"""Mean: {mean:.2f} bps
                        Std: {std:.1f} bps
                        0%ile (Min): {perc0:.2f} bps
                        10%ile: {perc10:.2f} bps
                        25%ile: {perc25:.2f} bps
                        50%ile (Median): {perc50:.2f} bps
                        75%ile: {perc75:.2f} bps
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
                    edgecolor = "#2F4F4F",
                    alpha=0.8,
                ),
                color="#000000",
                fontsize=10,
            )

      # Show legend
      plt.legend()
      plt.show()
      # Show/return the plot
      return plt
    

    def calc_prob_helper(self ,hrs):
      data_df = self.df.copy()
      bps_oh_ol = []
      for i in range(len(data_df)-hrs-1):
        open_price = data_df['Open'].iloc[i]
        max_high = data_df.iloc[i:i+hrs+1]['High'].max()
        min_low = data_df.iloc[i:i+hrs+1]['Low'].min()
        bps_oh_ol.append(max((max_high-open_price)*16 , (open_price - min_low)*16))
      return bps_oh_ol
       

    def calc_prob(self,target_bps,target_hrs,version):
      if version not in ['Absolute','Up','Down','No-Version']:
        raise ValueError("Invalid version. Use 'Down', 'Absolute', 'Up' or 'No-Version'.")

      bps_movements = []
      bps_oh_ol_movements = []

      prob_matrix_list=[]
      prob_matrix2_list = [] # for bps_oh_ol

      for i in range(1, target_hrs + 1):
          bps = (self.df['Open'].iloc[:self.N - i].values - self.df['Close'].iloc[i:self.N].values) * 16  #Get it checked.
          bps=self._round_off(bps)

          bps_oh_ol = self.calc_prob_helper(i)  # for the distribution of max(abs(open-high) , (abs(open-low))
          bps_oh_ol = self._round_off(bps_oh_ol)

          if version=='Down': #Where the movement is down movement,consider only those values; Convert the values to positive (but down movements)
            bps= bps[np.where(bps<=0)]
            bps=-1*bps[np.where(bps<0)]
          elif version=='Up': #Where the movement is up movement, consider only those values;
            bps= bps[np.where(bps>=0)]
          elif version=='Absolute':
            bps=np.abs(bps)

          # Store the number of hours and corresponding movements for that many hours in a dictionary;
          # Append the dictionary to the list for prob matrix
          prob_matrix_list.append({i:bps.tolist()})
          prob_matrix2_list.append({i:bps_oh_ol.tolist()})

          # Store all the movements untill i<= number of hours for final percentile
          bps_movements.extend(bps)
          bps_oh_ol_movements.extend(bps_oh_ol)

      bps_movements=np.array(bps_movements)
      bps_df = pd.DataFrame(bps_movements, columns=['bps'])

      bps_oh_ol = np.array(bps_oh_ol)
      bps_oh_ol_df = pd.DataFrame(bps_oh_ol_movements, columns=['bps'])

      percentile1 = (bps_df['bps'] <= target_bps).mean() * 100
      print(f"Percentile (wrt all {version} movements) for {abs(target_bps)} bps: {percentile1}%ile")
      percentiles1 = bps_df.describe(percentiles=[0.1,0.25, 0.5, 0.75, 0.9, 0.95, 0.99, 1])

      percentile2 = (bps_oh_ol_df['bps'] <= target_bps).mean() * 100
      print(f"Percentile (wrt all max movements from open) for {abs(target_bps)} bps: {percentile2}%ile")
      percentiles2 = bps_oh_ol_df.describe(percentiles=[0.1,0.25, 0.5, 0.75, 0.9, 0.95, 0.99, 1])
      
      prob_matrix=self._calc_prob_matrix(prob_matrix_list,bps_movements,version)
      prob_matrix2 = self._calc_prob_matrix(prob_matrix2_list,bps_oh_ol_movements,'Absolute')

      self.less_than_equal_percentile=percentile1
      self.greater_than_percentile=100-percentile1
      self.greater_than_prob_matrix = prob_matrix
      return self._plot_prob(bps_df,percentile1,percentiles1,target_bps,target_hrs,version) , self._plot_prob(bps_oh_ol_df , percentile2 , percentiles2 , target_bps , target_hrs , version = "Absolute")


    def _calc_prob_matrix_helper(self, prob_matrix_list, unique_hrs_array, unique_bps_array):
        # Convert to NumPy arrays and sort
        unique_bps_array = np.sort(np.array(unique_bps_array))
        unique_hrs_array = sorted(unique_hrs_array)

        # Initialize an empty list for each hour (fixes shallow copy issue)
        total_bps_array_for_all_hours = [[] for _ in unique_hrs_array]

        # Find the corresponding BPS movements for every hour
        total_bps = []
        for dic in prob_matrix_list:
            total_bps.extend(list(dic.values())[0])  # Flatten values
            hour = list(dic.keys())[0]
            
            if hour in unique_hrs_array:
                hour_index = unique_hrs_array.index(hour)
                total_bps_array_for_all_hours[hour_index] = total_bps.copy()  # Store a copy

        # Store the percentiles for all hours
        percentile_bps_array_for_all_hours = []

        for total_bps in total_bps_array_for_all_hours:
            # Convert total_bps to NumPy array
            bps_values = np.array(total_bps)

            # Compute percentiles for each unique BPS value
            percentile_bps_array = 100 - np.array([
                np.mean(bps_values <= target_bps) * 100
                for target_bps in unique_bps_array
            ])

            # Round and convert to string format with '%'
            percentile_bps_array = np.char.add(np.round(percentile_bps_array, 2).astype(str), '%')

            # Append the computed array for this hour
            percentile_bps_array_for_all_hours.append(percentile_bps_array)

        return percentile_bps_array_for_all_hours


    def _calc_prob_matrix(self, prob_matrix_list, all_movements,version):
        # Unique movements (index) and hours (columns)
        unique_bps = sorted(list(set(all_movements)))
        all_hours = [list(dic.keys())[0] for dic in prob_matrix_list]

        # Create an empty DataFrame for the probability matrix
        prob_matrix = pd.DataFrame(columns=all_hours, index=unique_bps)

        # Apply the helper function to get percentile
        percentile_bps_array=self._calc_prob_matrix_helper(prob_matrix_list, all_hours,unique_bps)
        for index,col in enumerate(prob_matrix):
          prob_matrix[col]=percentile_bps_array[index]
        
        prob_matrix.index.name=f'bps Pr(bps ({version}) > )'
        prob_matrix.columns.name=f'hrs'
        return prob_matrix


if __name__=='__main__':
   target_bps=2
   target_hrs=60
   interval='1h'
   ticker_name='ZN'
   a=GetMatrix(target_bps,target_hrs,interval,ticker_name)