import streamlit as st
import os
import pandas as pd
import requests
import openpyxl
from io import BytesIO
from zipfile import ZipFile
from PIL import Image
from concurrent.futures import ThreadPoolExecutor
from probability_matrix import GetMatrix,ProbabilityMatrix
import custom_filtering_dataframe
from returns_main import folder_input,folder_processed_pq
import requests
import re
from datetime import datetime
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np


st.cache_data.clear()

# Defining custom functions to modify generated data as per user input
def get_volatility_returns_csv_stats_custom_days(target_csv,target_column):
        
    stats_csv=target_csv[target_column].describe(percentiles=[0.1,0.25,0.5,0.75,0.95,0.99])
    # Add additional statistics to the DataFrame
    stats_csv.loc['mean'] = target_csv[target_column].mean()
    stats_csv.loc['skewness'] = target_csv[target_column].skew()
    stats_csv.loc['kurtosis'] = target_csv[target_column].kurtosis()

    stats_csv.index.name = 'Volatility of Returns Statistic'
    return stats_csv

def get_volatility_returns_csv_custom_days(target_csv,target_column):
    target_csv['ZScore wrt Given Days']=(target_csv[target_column]-target_csv[target_column].mean())/target_csv[target_column].std()
    return target_csv

# Defining functions to download the data

# 1. Function to convert DataFrame to Excel file with multiple sheets
def download_combined_excel(df_list,sheet_names,skip_index_sheet=[]):
    # Create a BytesIO object to hold the Excel file
    output = BytesIO()

    # Create a Pandas Excel writer using openpyxl as the engine
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        for sheetname,mydf in zip(sheet_names,df_list):
            if sheetname in skip_index_sheet:
                mydf.to_excel(writer, sheet_name=sheetname,index=False)
            else:
                mydf.to_excel(writer, sheet_name=sheetname)
    # Save the Excel file to the BytesIO object
    output.seek(0)
    return output


# 2. Main function to read image url and download as png files
def process_images(image_url_list):
    # Logic for downloading image bytes
    st.session_state["image_bytes_list"] = get_image_bytes(image_url_list)
    st.session_state["button_clicked"] = False  # Reset the button state after processing is complete

# 2.1 Function to get image bytes from list of images.
def get_image_bytes(image_url_list):
    image_bytes = []
    with ThreadPoolExecutor() as executor:
        results = executor.map(fetch_image, image_url_list)
        for result in results:
            if result:
                image_bytes.append(result)
    return image_bytes

# 2.2 Function to fetch image url
def fetch_image(url):
    try:
        response = requests.get(url, timeout=10)  # Add a timeout to prevent hanging
        response.raise_for_status()  # Raise HTTP errors if any
        image = Image.open(BytesIO(response.content))  # Open the image
        output = BytesIO()
        image.save(output, format='PNG')  # Save the image in PNG format
        output.seek(0)
        return output
    except Exception as e:
        st.error(f"Error processing image {url}: {e}")
        return None
    
# 2.3 Function to download image created via matplotlib.
def download_img_via_matplotlib(plt_object):
    buf=BytesIO()
    plt_object.savefig(buf, format="png",bbox_inches='tight')
    buf.seek(0)  # Go to the beginning of the buffer
    return buf

# 3. Function to create a ZIP file (not used)
def create_zip(excel_file_list, image_bytes_list):
    zip_buffer = BytesIO()
    with ZipFile(zip_buffer, 'w') as zip_file:
        # Add Excel file
        for excel_file in excel_file_list:
            zip_file.writestr('combined_data.xlsx', excel_file.getvalue())
        # Add image file
        for image_bytes in image_bytes_list:
            zip_file.writestr('example_image.png', image_bytes.getvalue())
    zip_buffer.seek(0)
    return zip_buffer

#5.0 helper function for 5.1
def add_start_end_ts(all_event_ts , delta):

    if(delta < 0):  # pre event + custom with delta < 0

        all_event_ts['end'] = all_event_ts['timestamp'].apply(lambda x: x.replace(minute=0, second=0, microsecond=0)) 
        all_event_ts['start'] = all_event_ts['end'] + pd.Timedelta(hours = delta)

    else:   # immediate reaction + custom with delta > 0

        all_event_ts['start'] = all_event_ts['timestamp'].apply(lambda x: x.replace(minute=0, second=0, microsecond=0))
        all_event_ts['end'] = all_event_ts['start'] + pd.Timedelta(hours = delta)

    return all_event_ts
    
#5.1 calculating the returns for event specific distros
def calc_event_spec_returns(selected_event , all_event_ts , ohcl_1h , mode , event_list, delta = 0, filter_out_other_events=False,  time_gap_hours=2):

    event_ts = all_event_ts.copy()

    event_ts.events = event_ts.events.astype(str)


    ############## Added by Yaman #######################################################################################################

    event_list_lower = [e.strip().lower() for e in event_list]

    def pick_event(x):
        x_l = x.lower()
        for e in event_list_lower:
            if e in x_l:
                return e
        return None

    event_ts['events'] = event_ts['events'].apply(pick_event)

    event_ts = event_ts.dropna(subset=['events'])

    event_ts = event_ts.drop_duplicates(subset=['timestamp','events'], keep='first')

    cutoff_time = pd.to_datetime('2022-12-20 00:00:00-05:00', errors='coerce')
    event_ts = event_ts[event_ts['timestamp'] >= cutoff_time]


    event_ts_filtered = event_ts.loc[event_ts['events'].str.strip().str.lower().str.contains(selected_event , case=False, na=False)]

    if filter_out_other_events:
            
            event_list_lower = [e.strip().lower() for e in event_list if e.strip().lower() != selected_event.lower()]

            clean_rows = []
            counter1 = 0
            counter2 = 0
            for _, row in event_ts_filtered.iterrows():
                t = row['timestamp']
                t_minus = t - pd.Timedelta(hours=time_gap_hours)
                t_plus = t + pd.Timedelta(hours=time_gap_hours)

                # Get full window, including current row
                nearby_events = event_ts[
                    (event_ts['timestamp'] >= t_minus) &
                    (event_ts['timestamp'] <= t_plus)
                ]

                # Now: Check if any disallowed event appears in the entire window
                contains_unwanted_event = (
                    nearby_events['events']
                    .str.lower()
                    .apply(lambda x: any(e in x for e in event_list_lower))
                    .any()
                )

                if not contains_unwanted_event:
                    clean_rows.append(row)
                    counter1 += 1
                else:
                    counter2 += 1

            print(f"Out of {counter1+counter2} times we see this event, only {counter2} times do we see another major event within +- 2 hours interval of it.")

            # Keep only rows with no unwanted overlap
            event_ts_filtered = pd.DataFrame(clean_rows)

    event_ts = event_ts_filtered     #Setting event_ts to event_ts_filtered so that the rest of the code below works as it was.

    ########################################################################################################################################

    #pre event
    if(mode == 1):
        event_ts = add_start_end_ts(event_ts , -8)

    #during event (may have to be changed for future events)
    elif(mode == 2):
        event_ts = add_start_end_ts(event_ts , 1)

    #custom (delta will be non-zero in this case)
    else:
        event_ts = add_start_end_ts(event_ts , delta)  

    event_ts = event_ts.drop_duplicates(subset=['start'], keep='first')
    cutoff_time = pd.to_datetime('2022-12-20 00:00:00-05:00', errors='coerce')
    event_ts = event_ts[event_ts['start'] >= cutoff_time]

    final_df=pd.DataFrame()
    vol_ret = []
    abs_ret = []
    ret = []
    start_date = []
    end_date = []

    for end , start in zip(event_ts['end'], event_ts['start']):

        temp_df = ohcl_1h[(ohcl_1h['US/Eastern Timezone'] >= start) & (ohcl_1h['US/Eastern Timezone'] < end)] #equality removed for 'end'. Otherwise 1 extra hour in taken.
        
        if(temp_df.empty):
            vol_ret.append(np.nan)
            abs_ret.append(np.nan)
            ret.append(np.nan)
            start_date.append(np.nan)
            end_date.append(np.nan)
        else:
            vol_ret.append((temp_df['High'].max() - temp_df['Low'].min())*16)
            abs_ret.append(abs(temp_df['Close'].iloc[-1] - temp_df['Open'].iloc[0])*16)
            ret.append((temp_df['Close'].iloc[-1] - temp_df['Open'].iloc[0])*16)
            start_date.append(temp_df['US/Eastern Timezone'].iloc[0])
            end_date.append(temp_df['US/Eastern Timezone'].iloc[-1])

    final_df['Volatility Return'] = vol_ret
    final_df['Absolute Return'] = abs_ret
    final_df['Return'] = ret
    final_df['Start_Date'] = start_date
    final_df['End_Date'] = end_date
    # print(final_df.head())

    final_df.dropna(inplace=True)

    print("SELECTED EVENT: ", selected_event)
    print('No of Data points: ' , len(final_df))
    # print(final_df)

    return final_df
        
#5.2 plot the event specific returns
def plot_event_spec_returns(final_df , selected_event , dur):
        
    figures = {}

    for col in final_df.columns[:3]:
        fig, ax = plt.subplots(figsize=(6, 4))
        
        # Plot histogram and KDE
        sns.histplot(final_df[col], kde=True, stat="density", linewidth=0, color="skyblue", ax=ax)
        sns.kdeplot(final_df[col], color="darkblue", linewidth=2, ax=ax)

        #number of instances.
        print(len(final_df[col]))  

                # Annotate histogram bars with bin edges (left-right) on top of each bar
        for patch in ax.patches:
            height = patch.get_height()
            if height == 0:
                continue  # skip bars with zero height
            left = patch.get_x()
            right = left + patch.get_width()
            x_center = left + patch.get_width() / 2
            label = f"{left:.2f} - {right:.2f}"
            ax.annotate(
                label,
                xy=(x_center, 0),
                xytext=(0, 5),
                textcoords="offset points",
                ha='center',
                fontsize=4,
                color='black'
            )


        # Statistics
        stats = final_df[col].describe()
        mean = stats['mean']
        std = stats['std']
        current_value = final_df[col].iloc[-1]
        current_date = final_df['Start_Date'].iloc[-1].date()

        # idx_val = final_df.index[-1]
        # if isinstance(idx_val, pd.Timestamp):
        #     current_date = idx_val.strftime('%Y-%m-%d')
        # else:
        #     current_date = str(idx_val)  # fallback if index is not datetime

        zscore = (current_value - mean) / std if std != 0 else 0

        # Red dot just above x-axis
        _ , y_max = ax.get_ylim()
        dot_y = y_max * 0.02

        # Red dot and vertical line
        ax.plot(current_value, dot_y, 'ro', label='Current Value')
        ax.axvline(x=current_value, color='red', linestyle='dotted', linewidth=1)

        # Annotate with current value, z-score, percentile, and date
        annotation_text = (
            f"Value: {current_value:.2f}, "
            f"Z: {zscore:.2f}, "
            f"Date: {current_date}"
        )
        ax.annotate(
            annotation_text,
            xy=(current_value, dot_y),
            xytext=(current_value, y_max * 0.15),
            arrowprops=dict(facecolor='red', arrowstyle='->'),
            fontsize=9,
            fontweight='bold',
            color='red',
            ha='center'
        )

        # Add statistics box
        textstr = (
            f"Mean: {mean:.2f}\n"
            f"Std: {std:.2f}\n"
            f"Min: {stats['min']:.2f}\n"
            f"25%: {stats['25%']:.2f}\n"
            f"Median: {stats['50%']:.2f}\n"
            f"75%: {stats['75%']:.2f}\n"
            f"95%: {final_df[col].quantile(0.95):.2f}\n"
            f"99%: {final_df[col].quantile(0.99):.2f}\n"
            f"Max: {stats['max']:.2f}"
        )

        ax.text(0.75, 0.75, textstr, transform=ax.transAxes, fontsize=10, 
                verticalalignment='top', bbox=dict(boxstyle='round,pad=0.3', edgecolor='black', facecolor='white'))

        ax.set_xlabel("Value")
        ax.set_ylabel("Density")
        ax.set_title(f"{col}")

        figures[col] = fig
        
    st.title("Distribution Analysis")
    col1, col2, col3 = st.columns(3)

    # Display each figure in a separate column
    with col1:
        st.pyplot(figures["Absolute Return"])
        st.write("**Absolute Return = [abs(close-open)]**")

    with col2:
        st.pyplot(figures["Return"])
        st.write("**Return = [close - open]**")

    with col3:
        st.pyplot(figures["Volatility Return"])
        st.write("**Volatility Return = [high - low]**")

    
# Setting up page configuration
st.set_page_config(
    page_title="FR Live Plots",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)




# Setting up tabs
tab1, tab2, tab3,tab4,tab5 = st.tabs(["Session and Volatility Returns for all sessions", 
                                 "Latest X days of Volatility Returns for each session",
                                 "Probability Matrix",
                                 "Custom Normalised Returns",
                                 'Event Specific Distro'])


# Defining GitHub Repo
repo_name='DistributionProject'
branch='main'
plots_directory="Intraday_data_files_stats_and_plots_folder"
plot_url_base=f"https://raw.githubusercontent.com/krishangguptafibonacciresearch/{repo_name}/{branch}/{plots_directory}/"

# Storing data in the form of links to be displayed later in separate tabs.
plot_urls=[]
intervals=[]
instruments=[]
return_types=[]

sessions=[]
latest_custom_days_urls=[]
for file in os.scandir(plots_directory):
    if file.is_file():
        if file.name.endswith('.png'):
            plotfile_content=file.name.split('_')
            plot_url=plot_url_base+file.name
            instrument=plotfile_content[0]
            interval=plotfile_content[1]
            return_type=plotfile_content[2]

            intervals.append(interval)
            instruments.append(instrument)
            plot_urls.append({
                "url": plot_url,
                "instrument": instrument,
                "interval": interval,
                "return_type": return_type,
                "stats_url": 
                (plot_url_base+f'{instrument}_{interval}_{return_type}_stats.csv').replace('Volatility', 'Volatility_Returns')
            })
        elif file.name.endswith('.csv') and 'latest_custom_days' in file.name:
            if 'stats' not in str(file.name):
                latest_custom_days_content=file.name.split('_')
                latest_custom_days_url=plot_url_base+file.name
                joined_session="_".join((latest_custom_days_content[0:-7:1]))
                spaced_session=" ".join(joined_session.split('_'))
                instrument=(latest_custom_days_content[-1])
                instrument=instrument.replace('.csv','')
                interval=latest_custom_days_content[-2]
                return_type=latest_custom_days_content[-4]

                sessions.append(spaced_session)
                latest_custom_days_urls.append({
                "url": latest_custom_days_url,
                'stats_url':plot_url_base+(file.name).split('.')[0]+'_stats.csv',
                "instrument": instrument,
                "interval": interval,
                "return_type": return_type,
                "session": [joined_session,spaced_session]
            })
            
# Storing unique lists to be used later in separate drop-downs
unique_intervals=list(set(intervals)) #Interval drop-down (1hr,15min,etc)
unique_instruments=list(set(instruments)) #Instrument/ticker drop-down (ZN, ZB,etc)
unique_sessions=list(set(sessions)) #Session drop-downs (US Mid,US Open,etc)
unique_versions=['Absolute','Up','Down','No-Version']#Version drop-downs for Probability Matrix
latest_days=[14,30,60,120,240,'Custom']
data_type = ['Non-Event' , 'All data']  #type of data to use when forming the Probability Matrix


# The  default option when opening the app
desired_interval = '1h'
desired_instrument='ZN'
desired_version='Absolute'


# Set the desired values in respective drop-downs.
# Interval drop-down
if desired_interval in unique_intervals:
    default_interval_index = unique_intervals.index(desired_interval)  # Get its index
else:
    default_interval_index = 0  # Default to the first element

# Instrument drop-down
if desired_instrument in unique_instruments:
    default_instrument_index = unique_instruments.index(desired_instrument)  # Get its index
else:
    default_instrument_index = 0  # Default to the first element

# Version drop-down
if desired_version in unique_versions:
    default_version_index = unique_versions.index(desired_version)  # Get its index
else:
    default_version_index = 0 # Default to the first element


#Define tabs:
with tab1:

        # Set title
        st.title("Combined Plots for all sessions")

        # Create drop-down and display it on the left permanantly
        x= st.sidebar.selectbox("Select Interval",unique_intervals,index=default_interval_index)
        y= st.sidebar.selectbox("Select Instrument",unique_instruments,index=default_instrument_index)

        # Create checkboxes for type of return
        vol_return_bool = st.checkbox("Show Volatility Returns (bps)")
        return_bool = st.checkbox("Show Session Returns (bps)")

        
        # Store in session state
        st.session_state.x = x
        st.session_state.y = y

    
        # Get urls of the returns and volatility returns plot.
        filtered_plots = [plot for plot in plot_urls if plot["interval"] == x and plot["instrument"] == y]

        # Set volatility returns on 0th index and returns on 1st index. (False gets sorted first)
        filtered_plots = sorted(
            filtered_plots,
            key=lambda plot: (plot["return_type"] == "Returns", plot["return_type"])
        ) 

        # As per checkbox selected, modify the filtered_plots list.
    

        if vol_return_bool and return_bool:
            display_text='Displaying plots for all available Returns type.'
            return_type='Session_and_Volatility_Returns'

        elif vol_return_bool:
            display_text='Displaying plots for Volatility Returns only.'
            for index,fname in enumerate(filtered_plots):
                if 'Volatility' not in fname['return_type']:
                    filtered_plots.pop(index)
            return_type='Volatility_Returns'
        
        elif return_bool:
            display_text='Displaying plots for Session Returns only.'
            for index,fname in enumerate(filtered_plots):
                if 'Returns' not in fname['return_type']:
                    filtered_plots.pop(index)
            return_type='Session_Returns'
        
        else:
            filtered_plots=[]
            display_text=''
        st.markdown(f"<p style='color:red;'>{display_text}</p>", unsafe_allow_html=True)


        # Display plots and stats
        try:
            if filtered_plots:
                all_dataframes=[]
                tab1_sheet_names=[]
                image_url_list=[]
                tab1_image_names=[]
                for plot in filtered_plots:
                    caption = f"{plot['return_type'].replace('Returns', 'Returns Distribution').replace('Volatility', 'Volatility Distribution')}"
                    st.subheader(caption + ' Plot')
                    st.image(plot['url'],caption=caption,use_container_width=True)
                    st.subheader('Descriptive Statistics')
                    st.dataframe(
                        pd.read_csv(plot['stats_url']),
                        use_container_width=True
                    )

                    # Save Stats dataframes into a list
                    all_dataframes.append(pd.read_csv(plot['stats_url']))
                    tab1_sheet_names.append(caption+' Stats')

                    # Save images into a list
                    image_url_list.append(plot['url'])
                    tab1_image_names.append(f'{y}_{x}_{caption}')

                # Download Stats dataframes as Excel
                excel_file = download_combined_excel(
                    df_list=all_dataframes,
                    sheet_names=tab1_sheet_names,
                    skip_index_sheet=tab1_sheet_names
                )

                # Provide the Excel download link
                st.download_button(
                    label="Download Descriptive Statistics Data for selected Return type(s)",
                    data=excel_file,
                    file_name=f'{return_type}_{x}_{y}_stats.xlsx',
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )

                # Provide plots download link

                if "button_clicked" not in st.session_state:
                    st.session_state["button_clicked"] = False  # To track if the button is clicked
                    st.session_state["image_bytes_list"] = None  # To store downloaded images

                # Display the button
                if st.button("Download Image Plots"):
                    # Show the "Please wait..." message in red
                    st.session_state["button_clicked"] = True
                    wait_placeholder = st.empty()

                    # Display "Please wait..." in red
                    wait_placeholder.markdown("<span style='color: green;'>Please wait...</span>", unsafe_allow_html=True)

                    process_images(image_url_list)
                        
                    # Remove the "Please wait..." message
                    wait_placeholder.empty()
                # Handle the state when button is clicked and images are ready
                if st.session_state["image_bytes_list"] is not None:
                    st.markdown(
                        "<span style='color: white;'>(Following images are ready for download):</span>",
                        unsafe_allow_html=True
                    )
                    for img_byte, img_name in zip(st.session_state["image_bytes_list"], tab1_image_names):
                        st.download_button(
                            label=f"Download {img_name.split('_')[-1]} plot",
                            data=img_byte,
                            file_name=img_name + ".png",
                            mime="image/png"
                        )

            else:
                if vol_return_bool or return_bool:
                    st.write("No plots found for the selected interval and instrument.")
                else:
                    st.write('Please select Return type!')

        except FileNotFoundError as e:
            print(f'File not found: {e}. Please try again later.')

with tab2:
    
        st.title("Get Volatility Returns for custom days")
        
        # Use stored values from session state
        x = st.session_state.get("x", list(unique_intervals)[0])
        y = st.session_state.get("y", list(unique_instruments)[0])
        
        # Show the session dropdown
        z = st.selectbox("Select Session", unique_sessions)

        # Select number of days to analyse
        get_days=st.selectbox("Select number of days to analyse", latest_days,index=0)
        get_days_val=get_days

        if get_days=='Custom':
            enter_days=st.number_input(label="Enter the number of days:",min_value=1, step=1)
            get_days_val=enter_days

            
        filtered_latest_custom_days_csvs = [data for data in latest_custom_days_urls if data["interval"] == x  and data["instrument"] ==y and data['session'][1]==z]
        try:
            if filtered_latest_custom_days_csvs:
                for latest_custom_day_csv in filtered_latest_custom_days_csvs:
                    st.subheader(f"Volatility Returns for Latest {get_days_val} day(s) of the session: {(latest_custom_day_csv['session'])[1]}")
        
                    df=(pd.read_csv(latest_custom_day_csv['url']))
                    latest_custom_data_csv=get_volatility_returns_csv_custom_days(target_csv=df.iloc[-1*get_days_val:],
                                                                                target_column=df.columns[1]
                    )
                    latest_custom_data_csv.reset_index(inplace=True,drop=True)
                    st.dataframe(latest_custom_data_csv,use_container_width=True)

                    st.subheader("Descriptive Statistics")
                    whole_data_stats_csv=(pd.read_csv(latest_custom_day_csv['stats_url'])) #originally generated

                    latest_custom_data_stats_csv=get_volatility_returns_csv_stats_custom_days(target_csv=latest_custom_data_csv,
                                                                    target_column=latest_custom_data_csv.columns[1])

                    st.dataframe(latest_custom_data_stats_csv,use_container_width=True)

                
                    # Combine the DataFrames into an Excel file
                    excel_file = download_combined_excel(
                        df_list=[latest_custom_data_csv, latest_custom_data_stats_csv],
                        sheet_names=['Volatility Returns', 'Descriptive Statistics'],
                        skip_index_sheet=['Volatility Returns'],
                    )

                    # Provide the download link
                    st.download_button(
                        label="Download Returns and Statistical Data",
                        data=excel_file,
                        file_name=f'{z}_latest_{get_days_val}_Volatility_Returns_{x}_{y}.xlsx',
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )
                
            else:
                st.write("No data found for the selected session.")
        except FileNotFoundError as e:
            print(f'File not found: {e}. Please try again later.')
        

with tab3:
        try:
            st.title("Probability Matrix (Unconditional)")
            # Use stored values from session state
            x = st.session_state.get("x", list(unique_intervals)[0])
            y = st.session_state.get("y", list(unique_instruments)[0])
            if 'h' in x:
                # Show the version dropdown
                version_value = st.selectbox("Select Version",unique_versions,index=default_version_index)

                data_type = st.selectbox("Select type of data to use", data_type , index=default_version_index)

                # Select bps to analyse
                enter_bps=st.number_input(label="Enter the number of bps:",min_value=0.0, step=0.5)
                st.caption("Note: The value must be a float and increases in steps of 0.5. Eg 1, 1.5, 2, 2.5, etc") 
                st.caption("The probability matrix rounds offs any other bps value into this format in the output.")

                # Select number of hours to analyse
                enter_hrs=st.number_input(label="Enter the number of hours:",min_value=1, step=1)
                st.caption("Note: The value must be an integer and increase in steps of 1. Eg 1, 2, 3, 4, etc.")
            
                # Get the probability matrix
                v=version_value
                
                prob_matrix_dic=GetMatrix(enter_bps,enter_hrs,x,y, data_type , version=version_value)
                st.subheader(f"Probability of bps ({v})  > {abs(enter_bps)} bps within {enter_hrs} hrs")

                # Store > probability in a small dataframe
                prob_df=pd.DataFrame(columns=['Description','Value'],
                            data=[[f'Probability of bps ({v})  > {abs(enter_bps)} bps within {enter_hrs} hrs',
                                str(round(prob_matrix_dic[v]['>%'],2))+'%'] ]
                )
                # Store <= probability in the dataframe
                prob_df.loc[len(prob_df)] = [f'Probability of bps ({v})  <= {abs(enter_bps)} bps within {enter_hrs} hrs',
                                            str(round(prob_matrix_dic[v]['<=%'],2))+'%']
                
                # Display the probability dataframe
                st.dataframe(prob_df,use_container_width=True)

                # Display the probability plots
                st.subheader(f"Probability Plot for {enter_bps} bps ({v}) movement in {enter_hrs} hrs")
                st.pyplot(prob_matrix_dic[v]['Plot'])

                st.subheader("Probability Plot for max(high-open , open-low)")
                st.pyplot(prob_matrix_dic['OH_OL_plot']['Plot'])

                # Display the probability matrix
                my_matrix=prob_matrix_dic[v]['Matrix']
                my_matrix.columns=[str(i)+' hr' for i in my_matrix.columns]
                my_matrix.index=[str(i)+' bps' for i in my_matrix.index]
                st.subheader(f"Probability Matrix of Pr(bps ({v}) >)")
                st.dataframe(my_matrix)


                # Combine the DataFrames into an Excel file
                my_matrix_list=[]
                my_matrix_ver=[]
                for ver in list(prob_matrix_dic.keys()):
                    if(ver != 'OH_OL_plot'):
                        my_matrix_list.append(prob_matrix_dic[ver]['Matrix'])
                        my_matrix_ver.append(f'{ver} bps Probability Matrix (> form)')
            
                excel_file = download_combined_excel(
                    df_list=my_matrix_list,
                    sheet_names=my_matrix_ver,
                    skip_index_sheet=[]
                )

                # Provide the download link for plots
                valid_keys = []  #first remove the OH_OL_plot key.
                for ver in prob_matrix_dic.keys():
                    if(ver != "OH_OL_plot"):
                        valid_keys.append(ver)
                st.download_button(
                    label=f"Download the Probability Matrices for version(s): bps {", bps ".join(list(valid_keys))}",
                    data=excel_file,
                    file_name=f"Probability Matrix_{'_'.join(my_matrix_ver)}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
                
                # Provide plots download link
                if "tab3_button_clicked" not in st.session_state:
                    st.session_state["tab3_plots_button_clicked"] = False  # To track if the button is clicked
                    st.session_state["tab3_plots_ready"] = None 

                # Display the button
                if st.button("Download Image Plots",key='tab3_button'):
                    # Show the "Please wait..." message in red
                    st.session_state["tab3_plots_button_clicked"] = True
                    wait_placeholder2 = st.empty()

                    # Display "Please wait..." in red
                    wait_placeholder2.markdown("<span style='color: green;'>Please wait...</span>", unsafe_allow_html=True)
            
                    
                    # Handle the state when button is clicked and images are ready
                    if st.session_state["tab3_plots_ready"] is not None:
                        st.markdown(
                            "<span style='color: white;'>(Following images are ready for download):</span>",
                            unsafe_allow_html=True
                        )
        
                    for ver,_ in prob_matrix_dic.items():
                        if(ver != 'OH_OL_plot'):
                            my_img_data = download_img_via_matplotlib(prob_matrix_dic[ver]['Plot'])
                            st.download_button(
                                label=f"Download the Probability Plots for version: bps {ver}",
                                data=my_img_data,
                                file_name=f"Probability Matrix_{ver}.png",
                                mime="image/png"
                            )
                    
                    # Remove the "Please wait..." message
                    wait_placeholder2.empty()
                
            else:
                st.write("Please select 1h interval.")
        except:
            display_text='1h interval data unavailable for the current ticker.'
            st.markdown(f"<p style='color:red;'>{display_text}</p>", unsafe_allow_html=True)

with tab4:
            try:
                # Protected tab
                # Add password
                PASSWORD = "distro" 

                # Initialize authentication state
                if "authenticated" not in st.session_state:
                    st.session_state.authenticated = False

                if not st.session_state.authenticated:
                    st.header("This tab is Password Protected🔒")
                    password = st.text_input("Enter Password:", type="password")
                    
                    if st.button("Login"):
                        if password == PASSWORD:
                            st.session_state.authenticated = True
                            st.rerun()
                        else:
                            st.error("Incorrect password. Try again.")
                else:
                    st.header("Authorised ✅")
                    st.write("This tab contains sensitive information.")
                    
                    if st.button("Logout"):
                        st.session_state.authenticated = False
                        st.rerun()
                    

                if st.session_state.authenticated==True:
                    # Use stored values from session state
                    x = st.session_state.get("x", list(unique_intervals)[0])
                    y = st.session_state.get("y", list(unique_instruments)[0])

                    st.title("Custom Filtering")

                    # Default sessions:
                    
                    # Show the version dropdown
                    version_value = st.selectbox("Select Version",unique_versions.copy(),index=default_version_index,
                                                key='tab4_v')

                    # Select bps to analyse
                    enter_bps=st.number_input(label="Enter the Observed movement in bps:",min_value=0.00,key='tab4_bps')

                    # Select Multiple Sessions

                    # Add custom session via button
                    default_text=f'Distribution of bps ({version_value}) Returns {y} with returns calculated for every {x}'
                    finalname=default_text
                    final_list=[]
                    
                    filter_sessions=False
                    
                    # Not include intervals
                    if 'd' not in x:
                        st.subheader('Add Custom Session')
                        tab4check=st.checkbox(label='Add Custom Session',key='tab4check')

                        if tab4check:
                            # Add Checkbox to filter by starting day
                            tab4check1=st.checkbox(label='Calculate Custom Time Difference',key='tab4check1')
                            if tab4check1:
                                # Date inputs
                                start_date = st.date_input(label="Start Date (YYYY/MM/DD)", value=datetime.today().date())
                                end_date = st.date_input(label="End Date (YYYY/MM/DD)", value=datetime.today().date())

                                # Time inputs
                                start_time = st.time_input(label="Start Time (HH:MM)",value='now',help='Directly Type Time in HH:MM')
                                end_time = st.time_input(label="End Time (HH:MM)",value='now',help='Directly Type Time in HH:MM')
                            
                                # Combine date and time into datetime objects
                                start_datetime = datetime.combine(start_date, start_time)
                                end_datetime = datetime.combine(end_date, end_time)

                                # Calculate time difference
                                time_diff = end_datetime - start_datetime

                                # Extract hours and minutes
                                hours, remainder = divmod(time_diff.total_seconds(), 3600)
                                minutes = remainder / 60

                                display_text1=(f"Time Difference: {int(hours)} hours and {int(minutes)} minutes")
                                display_text2=(f"Approx Difference (Hrs): {round(hours+minutes/60,1)} hours")
                                display_text3=(f"Approx Difference (Mins): {int(hours*60+minutes)} minutes")
                                st.markdown(f"<p style='color:red; font-size:14px;'>{display_text1}</p>", unsafe_allow_html=True)
                                st.markdown(f"<p style='color:red; font-size:14px;'>{display_text2}</p>", unsafe_allow_html=True)
                                st.markdown(f"<p style='color:red; font-size:14px;'>{display_text3}</p>", unsafe_allow_html=True)

                            # 1. Select Start time in ET
                            enter_start=st.number_input(label="Enter the start time in ET",min_value=0, max_value=23, step=1)
                            st.caption("Note: The value must be an integer and increase in steps of 1. Eg 1, 2, 3, 4, etc.")
                            

                            # 2. Select number of hours to analyse post the start time
                            enter_hrs=st.number_input(label=f"Enter the time (multiple of {x}) to be searched post the selected time",min_value=0, step=1)
                            st.caption("Note: The value must be an integral multiple of the interval selected")


                            # Add Checkbox to filter by starting day
                            tab4check2=st.checkbox(label='Filter by Starting Day',key='tab4check2')
                            day_list=['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday']
                            
                            # Add Selectbox to select the starting day
                            if tab4check2==True:
                                enter_start_day=st.selectbox("Select Starting Day",day_list,index=0,
                                                key='tab4_sd')
                            else:
                                enter_start_day=""


                        
                        # Combine default and custom time filters. filter_sessions1=default, filter_sessions2=custom
                            filter_sessions1=[]
                            filter_sessions2=[]
                            filter_sessions1.append((enter_start,enter_hrs,enter_start_day))
                    
                        # Combine the two
                            filter_sessions=list(set(filter_sessions1+filter_sessions2))


                    # Give the name to include ticker,interval,time,day,start_date and end_date.
                    if filter_sessions==False:
                        filename=default_text
                    else:
                        mysession=f'{filter_sessions[0][2]} {filter_sessions[0][0]} ET to {filter_sessions[0][0]} ET+{filter_sessions[0][1]}{x[-1]}'
                        finalname=f'{default_text} for session:{mysession}'

                    # Select the dataframe for Hour interval
                    selected_df=custom_filtering_dataframe.get_dataframe(x,y,'Intraday_data_files_pq')

                    # Extract start and end dates
                    finalcsv=selected_df.copy()
                    finalcsv.index=finalcsv[finalcsv.columns[-1]]
                    finalcsv.drop_duplicates(inplace=True)
                    finalcsv.dropna(inplace=True,how='all') 
                    finalcsv.sort_index(inplace=True)
                    finalcsv = finalcsv.loc[~finalcsv.index.duplicated(keep='last')]
                    finalstart=str(finalcsv.index.to_list()[0])[:10]
                    finalend=str(finalcsv.index.to_list()[-1])[:10]


                    if filter_sessions:
                        # Filter the dataframe as per selections
                        filtered_df=custom_filtering_dataframe.filter_dataframe(selected_df,
                                                                                filter_sessions,
                                                                                day_dict="",#time_day_dict,
                                                                                timezone_column='US/Eastern Timezone',
                                                                                target_timezone='US/Eastern',
                                                                                interval=x,
                                                                                ticker=y)
                        finalname+=f' for dates:{finalstart} to {finalend}'
                        # Stats and Plots
                        stats_plots_dict=custom_filtering_dataframe.calculate_stats_and_plots(filtered_df,
                                                                            finalname,
                                                                            version=version_value,
                                                                            check_movement=enter_bps,
                                                                            interval=x,
                                                                            ticker=y,
                                                                            target_column='Group')

                    else:
                        finalname=f'{default_text} for dates:{finalstart} to {finalend}'
                        filtered_df=custom_filtering_dataframe.filter_dataframe(selected_df,
                                                                                "",
                                                                                "",
                                                                                'US/Eastern Timezone',
                                                                                'US/Eastern',
                                                                                x,
                                                                                y)
                        # Stats and Plots
                        stats_plots_dict=custom_filtering_dataframe.calculate_stats_and_plots(filtered_df,
                                                                            finalname,
                                                                            version=version_value,
                                                                            check_movement=enter_bps,
                                                                            interval=x,
                                                                            ticker=y,
                                                                            target_column='US/Eastern Timezone')

        
                    
                    # Add Widgets:
                    # Dataframe
                    st.subheader('Filtered Dataframe')
                    st.text(f'Ticker: {y}')
                    st.text(f'Interval: {x}')
                    st.text(f'Dates: {finalstart} to {finalend}')
                    # if filter_sessions==False:
                    #     session_text="None"
                    # else:
                    #     session_text=f'Start Time:{filter_sessions[0]}, Start Day:{filter_sessions[2]}, Filter for next {filter_sessions[1]} units post '
                    # st.text(f'Filters Applied: {session_text}')
                    st.dataframe(filtered_df,use_container_width=True)


                    # Display the  stats dataframe
                    stats_df=stats_plots_dict['stats']
                    st.dataframe(stats_df,use_container_width=True)

                    # Store > probability in a small dataframe
                    prob_df=pd.DataFrame(columns=['Description','Value'],
                                data=[[f'Probability of bps ({version_value})  > {abs(enter_bps)}',
                                    str(round(stats_plots_dict['%>'],2))+'%'] ]
                    )
                    # Store <= ZScore
                    prob_df.loc[len(prob_df)] =[f'Probability of bps ({version_value})  <= {abs(enter_bps)}',
                                    str(round(stats_plots_dict['%<='],2))+'%']
                    
                    prob_df.loc[len(prob_df)] =[f'ZScore for ({version_value}) bps <=  {enter_bps} bps',
                                    str((stats_plots_dict['zscore<=']))]
                

                    # Display the probability dataframe
                    st.dataframe(prob_df,use_container_width=True)


                    # Display the probability plot
                    st.subheader(f"Probability Plot for {enter_bps} bps ({version_value}) movement")
                    st.pyplot(stats_plots_dict['plot'])
                

                    # Combine the DataFrames into an Excel file (Convert datetime values to text)
                    filtered_df[filtered_df.columns[-3]]=filtered_df[filtered_df.columns[-3]].astype(str) # Datetime column
                    my_matrix_list=[filtered_df,
                                    prob_df,
                                    stats_df]
                    my_matrix_ver=[f'{x}_{y}_{finalstart} to {finalend}','Probability','Descriptive Statistics']
                
                    excel_file = download_combined_excel(
                        df_list=my_matrix_list,
                        sheet_names=my_matrix_ver,
                        skip_index_sheet=[]
                    )

                    # Provide the download link for plots
                    st.download_button(
                        label="Download Excels",
                        data=excel_file,
                        file_name=f"Probability_Stats_Excel_{finalname}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )

                    my_img_data= download_img_via_matplotlib(stats_plots_dict['plot'])
                    st.download_button(
                            label=f"Download the Probability Plots",
                            data=my_img_data,
                            file_name=f"Probability Plot.png",
                            mime="image/png"
                        )
            except UnboundLocalError as uble:
                display_text=f'{y} Data unavailable for {x} interval.'
                st.markdown(f"<p style='color:red;'>{display_text}</p>", unsafe_allow_html=True)

            except Exception as e:
                display_text='Some error occured. Please try some other parameters and re-run.'
                st.text(e)
                st.markdown(f"<p style='color:red;'>{display_text}</p>", unsafe_allow_html=True)

with tab5:
        
    events = ['CPI', 'PPI', 'PCE Price Index', 'Non Farm Payrolls', 'ISM Manufacturing PMI', 'ISM Services PMI',
              'S&P Global Manufacturing PMI', 'S&P Global Services PMI', 'Michigan',
              'Jobless Claims' , 'ADP' , 'JOLTs' , 'Challenger Job Cuts' , 'Fed Interest Rate Decision' , 
              'GDP Price Index QoQ Adv' , 'Retail Sales' , 'Fed Press Conference', 'FOMC Minutes']
    
    selected_event = st.selectbox("Select an event:" , events)
    duration = ['pre event (8 hr before event)' , 'immediate reaction (1 hr after the event)']
    dur = st.selectbox("Select duration: " , duration)

    ############## Added by Yaman ########################################################################
    filter_isolated = st.checkbox(
    "Exclude events with any other announcement ±2 hours",
    help="Only show events that have no other events in the surrounding time window."
    )
    ######################################################################################################

    # getting the data for the timestamps of the event
    # fname='ZN_1h_events_tagged_target_tz.csv'
    # repo_name='DistributionProject'
    # branch='main'
    # plots_directory="Intraday_data_files_processed_folder"
    # link=f"https://raw.githubusercontent.com/krishangguptafibonacciresearch/{repo_name}/{branch}/{plots_directory}/{fname}"

    # all event timestamps
    for file in os.scandir("Intraday_data_files_processed_folder_pq"):
        if file.name == "ZN_1h_events_tagged_target_tz.parquet":
            all_event_ts = pd.read_parquet(file.path , engine = 'pyarrow')

    # all_event_ts['US/Eastern Timezone'] = pd.to_datetime(all_event_ts.timestamp,errors='coerce',utc=True)
    # all_event_ts['US/Eastern Timezone'] = all_event_ts['US/Eastern Timezone'].dt.tz_convert('US/Eastern')

    all_event_ts['timestamp'] = pd.to_datetime(all_event_ts.timestamp , errors='coerce').dt.tz_localize('US/Eastern')

    # finding the price movements:
    repo_name = "DistributionProject"
    branch = "main"
    plots_directory2 = "Intraday_data_files"

    # GitHub API URL to list contents of the directory
    # api_url = f"https://api.github.com/repos/krishangguptafibonacciresearch/{repo_name}/contents/{plots_directory2}?ref={branch}"

    # Regular expression to match file pattern. Has to be used since the file name changes.
    pattern = re.compile(r"Intraday_data_ZN_1h_2022-12-20_to_(\d{4}-\d{2}-\d{2})\.parquet")
    ohcl_1h = pd.DataFrame()

    for file in os.scandir('Intraday_data_files_pq'):
        if file.is_file():
            match = pattern.match(file.name)
            if match:
               print("File used:" , file.name)
               ohcl_1h = pd.read_parquet(os.path.join("Intraday_data_files_pq" , file.name) , engine = 'pyarrow')

    # # Fetch file list from GitHub
    # response = requests.get(api_url)
    # if response.status_code != 200:
    #     print("Failed to retrieve file list:", response.json())
    #     exit()

    # # Extract filenames and find the latest date
    # files = response.json()
    # matching_files = []

    # for file in files:
    #     filename = file["name"]
    #     match = pattern.match(filename)
    #     if match:
    #         date_str = match.group(1)
    #         try:
    #             file_date = datetime.strptime(date_str, "%Y-%m-%d")
    #             matching_files.append((file_date, filename))
    #         except ValueError:
    #             continue
    # if matching_files:
    #     latest_fname2 = max(matching_files)[1]
    #     link2 = f"https://raw.githubusercontent.com/krishangguptafibonacciresearch/{repo_name}/{branch}/{plots_directory2}/{latest_fname2}"
    # else:
    #     print("No matching files found.")

    # # OHCL data for 1h freq
    # ohcl_1h = pd.read_csv(link2)

    # convert US/Eastern Timezone from string data type to a [datetime , ET] datatype. (str --> UTC --> ET)
    ohcl_1h['US/Eastern Timezone'] = pd.to_datetime(ohcl_1h.index,errors='coerce',utc=True)  #Datetime col has strings. so first convert that to UTC datetime.
    ohcl_1h['US/Eastern Timezone'] = ohcl_1h['US/Eastern Timezone'].dt.tz_convert('US/Eastern')

    my_dict = {"pre event (8 hr before event)": 1 , "immediate reaction (1 hr after the event)": 2}


    ############################################### Changed a bit by Yaman ##########################################################################
    custom = st.checkbox('Custom time')
    if(custom):
        delta = st.number_input("Enter the number of hours:", min_value=-1000, max_value=1000 , value=0, step=1)
        final_df = calc_event_spec_returns(selected_event, all_event_ts, ohcl_1h , 3 , events, delta, filter_isolated , 2)
    else:
        final_df = calc_event_spec_returns(selected_event, all_event_ts, ohcl_1h , my_dict[dur], events, 0 , filter_isolated , 2)

    plot_event_spec_returns(final_df , selected_event , dur)

##########################################################