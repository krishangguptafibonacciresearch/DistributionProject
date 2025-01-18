import streamlit as st
import os
import pandas as pd
from io import BytesIO
import openpyxl
from zipfile import ZipFile
from PIL import Image
import requests
from concurrent.futures import ThreadPoolExecutor


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


# 2. Function to read image url and download as png files
def process_images(image_url_list):
    # Logic for downloading image bytes
    st.session_state["image_bytes_list"] = get_image_bytes(image_url_list)
    st.session_state["button_clicked"] = False  # Reset the button state after processing is complete

def get_image_bytes(image_url_list):
    image_bytes = []
    with ThreadPoolExecutor() as executor:
        results = executor.map(fetch_image, image_url_list)
        for result in results:
            if result:
                image_bytes.append(result)
    return image_bytes

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

    
# Setting up page configuration
st.set_page_config(
    page_title="FR Live Plots",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Setting up tabs
tab1, tab2 = st.tabs(["Session and Volatility Returns for all sessions", "User-defined Latest Volatility Returns"])


# Defining GitHub Repo
repo_name='DistributionProject'
branch='branch24'
plots_directory="Intraday_data_files_stats_and_plots_folder"
plot_url_base=f"https://raw.githubusercontent.com/krishangguptafibonacciresearch/{repo_name}/{branch}/{plots_directory}/"


# Tabwise content
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
            print(file.name)
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
            
# Get unique details related to instruments
unique_intervals=list(set(intervals))
unique_instruments=list(set(instruments))
unique_sessions=list(set(sessions))
latest_days=[14,30,60,120,240,'Custom']



# The  default option when opening the app
desired_interval = '1m'

# Check if the desired option exists in the list
if desired_interval in unique_intervals:
    default_index = unique_intervals.index(desired_interval)  # Get its index
else:
    default_index = 0  # Default to the first element



#Define tab1:
with tab1:

    # Set title
    st.title("Combined Plots for all sessions")

    # Create drop-down and display it on the left permanantly
    x= st.sidebar.selectbox("Select Interval",unique_intervals,index=default_index)
    y= st.sidebar.selectbox("Select Instrument",unique_instruments)

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
                print(latest_custom_day_csv['url'])
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


    

   

