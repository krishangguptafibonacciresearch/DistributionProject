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
from returns_main import Intraday_data_files,folder_processed


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
    plt_object.savefig(buf, format="png")
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

    
# Setting up page configuration
st.set_page_config(
    page_title="FR Live Plots",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Setting up tabs
tab1, tab2, tab3,tab4, tab5 = st.tabs(["Session and Volatility Returns for all sessions", 
                                 "Latest X days of Volatility Returns for each session",
                                 "Probability Matrix",
                                 "Custom Normalised Returns",
                                 "Event Specific Distro"])


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
unique_versions=['Absolute','Up','Down']#Version drop-downs for Probability Matrix
latest_days=[14,30,60,120,240,'Custom'] 



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


# t_i_pair=[]
# for t in unique_instruments:
#         for i in unique_intervals:
#                 t_i_pair.append(t+'_'+i)
# for ti in t_i_pair:
#         if ti in [f.name for f in os.scandir(Intraday_data_files)]:
#                 st.sidebar.text(f'Available Combination: {t_i.split('_')[0]}:{t_i.split('_')[1]}')
                

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
        st.title("Probability Matrix")
        # Use stored values from session state
        x = st.session_state.get("x", list(unique_intervals)[0])
        y = st.session_state.get("y", list(unique_instruments)[0])
        if 'h' in x:
            # Show the version dropdown
            version_value = st.selectbox("Select Version",unique_versions,index=default_version_index)

            # Select bps to analyse
            enter_bps=st.number_input(label="Enter the number of bps:",min_value=0.0, step=0.5)
            st.caption("Note: The value must be a float and increases in steps of 0.5. Eg 1, 1.5, 2, 2.5, etc") 
            st.caption("The probability matrix rounds offs any other bps value into this format in the output.")

            # Select number of hours to analyse
            enter_hrs=st.number_input(label="Enter the number of hours:",min_value=1, step=1)
            st.caption("Note: The value must be an integer and increase in steps of 1. Eg 1, 2, 3, 4, etc.")
        
            # Get the probability matrix
            v=version_value
            
            prob_matrix_dic=GetMatrix(enter_bps,enter_hrs,x,y,version=version_value)
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

            # Display the probability plot
            st.subheader(f"Probability Plot for {enter_bps} bps ({v}) movement in {enter_hrs} hrs")
            st.pyplot(prob_matrix_dic[v]['Plot'])

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
                my_matrix_list.append(prob_matrix_dic[ver]['Matrix'])
                my_matrix_ver.append(f'{ver} bps Probability Matrix (> form)')
        
            excel_file = download_combined_excel(
                df_list=my_matrix_list,
                sheet_names=my_matrix_ver,
                skip_index_sheet=[]
            )

            # Provide the download link for plots
            st.download_button(
                label=f"Download the Probability Matrices for version(s): bps {", bps ".join(list(prob_matrix_dic.keys()))}",
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

with tab4: #Protected tab
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
            if 'h' not in x:
                st.write("Please select 1h interval.")
                st.stop()

            st.title("Custom Filtering")

            # Default sessions:
            mysessions=[('All day 0-24 ET' if s=='All day' else s) for s in unique_sessions]
            default_session_index=mysessions.index('All day 0-24 ET')
            
            # Show the version dropdown
            version_value = st.selectbox("Select Version",unique_versions.copy(),index=default_session_index,
                                        key='tab4_v')

            # Select bps to analyse
            enter_bps=st.number_input(label="Enter the Observed movement in bps:",min_value=0.000,key='tab4_bps')

            # Select Multiple Sessions
            selected_sessions = st.multiselect("Select Session",mysessions,default=['All day 0-24 ET'])

            # Add custom session via button
            default_text=f'Distribution of bps ({version_value}) Returns {y} with interval of {x}'
            finalname=default_text
            final_list=[]
            
            
            # Add custom session heading
            st.subheader('Add Custom Session')

            # 1. User input for time
            new_option_original = st.text_input("Enter a new option for Time in ET (Format: 3-12ET, 10-15ET, etc.):",value='0-5 ET,0-9 ET')
            st.caption('Session spanning across 2 days like 21-4 ET shall be entered as 21-24 ET, 0-4 ET')
            new_option_original=new_option_original.strip()
            new_options=new_option_original.replace('ET','')
            new_options=new_options.split(',')
            try:
                if len(new_options)>0:
                    final_list=[(int(se.split('-')[0]), int(se.split('-')[1])) for se in new_options]
                else:
                    final_list=[]
            except Exception as e:
                pass

           
     
            # Combine default and custom time filters. filter_sessions1=default, filter_sessions2=custom
            filter_sessions1=[]
            for my_selection in selected_sessions:
                session_val=list((str(my_selection)).split())[-2] #'0-7'
                session_val=session_val.split('-') #['0','7']
                filter_sessions1.append((int(session_val[0]),int(session_val[1])))

            filter_sessions2=[]
            filter_sessions2+=final_list

            # Combine the two
            filter_sessions=list(set(filter_sessions1+filter_sessions2))

            # Declare empty dict to store days as values to time as keys
            time_day_dict={}
            if filter_sessions:
                st.text(f'Selected Sessions: {filter_sessions}')
                try:
                    if len(new_options)>0:
                        final_list=[(int(se.split('-')[0]), int(se.split('-')[1])) for se in new_options]
                    else:
                        final_list=[]
                except Exception as e:
                    pass

            # 2. User input for days
                time_day_dict={}
                for k in filter_sessions:
                    time_day_dict[k]=""
                new_day_original= st.text_input("Enter a new option for corrsponding day (Format: Monday, Tuesday....) with respective Time selection(s) being shown above.")
                new_day_original=new_day_original.strip()
                day_val=new_day_original.split(',')
                for k,d in zip(filter_sessions,day_val):
                    time_day_dict[k]=d

                st.text(f'Selected Days corresponding to above Time period(s):{time_day_dict}')

            # Give the name to include ticker,interval,time,day,start_date and end_date.
            finalname=f'{default_text} for sessions:{", ".join(selected_sessions+ list(set([str(i[0])+'-'+str(i[1])+f'ET' for i in filter_sessions2])))}'


            # # 3.  User input for event filtering (independent of time and day)
            # st.text_input('Enter the specific event keywords you are looking for:')
            # new_event_original= st.text_input("Enter a new option for an event in comma separated format. (Format: PMI, Auctions,)")
            # st.caption('The events get filtered based on the keywords you enter.')
            # new_event_original=new_event_original.strip()
            # event_val=new_event_original.split(',')
            # st.text(f'Selected Events :{event_val}')


            # Select the dataframe for Hour interval
            selected_df=custom_filtering_dataframe.get_dataframe(interval=x,ticker_name=y,folder=Intraday_data_files)

            #Extract start and end dates
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
                                                                        time_day_dict,
                                                                        timezone_column='US/Eastern Timezone',
                                                                        target_timezone='US/Eastern')
                finalname+=f' for dates:{finalstart} to {finalend}'

            else:
                finalname=f'{default_text} for dates:{finalstart} to {finalend}'
                filtered_df=custom_filtering_dataframe.filter_dataframe(selected_df,
                                                                        "",
                                                                        "",
                                                                        
                                                                        timezone_column='US/Eastern Timezone',
                                                                        target_timezone='US/Eastern')


            # Stats and Plots
            stats_plots_dict=custom_filtering_dataframe.calculate_stats_and_plots(filtered_df,
                                                                finalname,
                                                                version=version_value,
                                                                check_movement=enter_bps)
            
            # Add Widgets:
            # Dataframe
            st.subheader('Filtered Dataframe')
            st.text(f'Ticker: {y}')
            st.text(f'Interval: {x}')
            st.text(f'Sessions: {", ".join(selected_sessions + list(set([str(i[0])+'-'+str(i[1])+'ET'+f':{time_day_dict[i]}' for i in filter_sessions2])))}')
            st.text(f'Corresponding Days:{time_day_dict}')
            st.text(f'Dates: {finalstart} to {finalend}')
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
            filtered_df[filtered_df.columns[-1]]=filtered_df[filtered_df.columns[-1]].astype(str)
            filtered_df[filtered_df.columns[-2]]=filtered_df[filtered_df.columns[-2]].astype(str)
            my_matrix_list=[filtered_df,
                            prob_df,
                            stats_df]
            my_matrix_ver=[f'{instrument}_{interval}_{finalstart} to {finalend}','Probability','Descriptive Statistics']
        
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
with tab5:
        events = ['core inflation rate']
        selected_event = st.selectbox("Select an event:" , events)
        duration = ['pre event' , 'during event' , 'post event']
        dur = st.selectbox("Select duration: " , duration)

        # getting the data for the timestamps of the event
        fname='ZN_1h_events_tagged_target_tz.csv'
        repo_name='DistributionProject'
        branch='main'
        plots_directory="Intraday_data_files_processed_folder"
        link=f"https://raw.githubusercontent.com/krishangguptafibonacciresearch/{repo_name}/{branch}/{plots_directory}/{fname}"

        df=pd.read_csv(link)

        # Get days
        df['US/Eastern Timezone']=pd.to_datetime(df.timestamp,errors='coerce',utc=True)
        df['US/Eastern Timezone']=df['US/Eastern Timezone'].dt.tz_convert('US/Eastern')
        df['pre_time']=df['US/Eastern Timezone']-pd.Timedelta(hours = 8)
        df_events=df
        df_events.events=df_events.events.astype(str)

        event_timestamps = df_events.loc[df_events['events'].str.strip().str.lower().str.contains(selected_event , case=False, na=False)]
        event_timestamps = event_timestamps.drop_duplicates(subset=['pre_time'], keep='first')
        cutoff_time = pd.to_datetime('2022-12-20 00:00:00-05:00', utc=True)
        event_timestamps = event_timestamps[event_timestamps['US/Eastern Timezone'] >= cutoff_time]
        event_timestamps

        # finding the price movements
        fname2='Intraday_data_ZN_1h_2022-12-20_to_2025-02-19.csv'
        repo_name='DistributionProject'
        branch='main'
        plots_directory2="Intraday_data_files"
        link2=f"https://raw.githubusercontent.com/krishangguptafibonacciresearch/{repo_name}/{branch}/{plots_directory2}/{fname2}"

        df2=pd.read_csv(link2)
        df2['US/Eastern Timezone']=pd.to_datetime(df2.Datetime,errors='coerce',utc=True)
        df2['US/Eastern Timezone']=df2['US/Eastern Timezone'].dt.tz_convert('US/Eastern')
        df2.head()

        final_df=pd.DataFrame()
        vol_ret = []
        abs_ret = []
        ret = []

        for end , start in zip(event_timestamps['US/Eastern Timezone'], event_timestamps['pre_time']):
            temp_df = df2[(df2['US/Eastern Timezone'] >= start) & (df2['US/Eastern Timezone'] <= end)]
            vol_ret.append((temp_df['High'].max() - temp_df['Low'].min())*16)
            abs_ret.append(abs(temp_df['Close'].iloc[-1] - temp_df['Open'].iloc[0])*16)
            ret.append((temp_df['Close'].iloc[-1] - temp_df['Open'].iloc[0])*16)

        final_df['Volatility Return'] = vol_ret
        final_df['Absolute Return'] = abs_ret
        final_df['Return'] = ret

        for col in final_df:
            fig, ax = plt.subplots(figsize=(6, 4))

            sns.histplot(final_df[col], kde=True, stat="density", linewidth=0, color="skyblue", ax=ax)
            sns.kdeplot(final_df[col], color="darkblue", linewidth=2, ax=ax)

            stats = pd.Series(final_df[col]).describe()
            textstr = f"Mean: {stats['mean']:.2f}\nStd: {stats['std']:.2f}\nMin: {stats['min']:.2f}\nMax: {stats['max']:.2f}"

            ax.text(0.75, 0.75, textstr, transform=ax.transAxes, fontsize=10, 
                     verticalalignment='top', bbox=dict(boxstyle='round,pad=0.3', edgecolor='black', facecolor='white'))

            ax.set_xlabel("Value")
            ax.set_ylabel("Frequency")
            ax.set_title(f"{col}")

            st.pyplot(fig)
            



        
            

            




            

