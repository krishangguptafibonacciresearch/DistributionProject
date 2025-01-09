import streamlit as st
import os
import pandas as pd

# Setting up page configuration
st.set_page_config(
    page_title="FR Live Plots",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Setting up tabs
tab1, tab2 = st.tabs(["Returns and Volatility Plots for All Sessions", "Latest 14 days Session-wise Descriptive Statistics"])


# Defining GitHub Repo
repo_name='distro_project'
branch='main'
plots_directory="Intraday_data_files_stats_and_plots_folder"
plot_url_base=f"https://raw.githubusercontent.com/krishangguptafibonacciresearch/{repo_name}/{branch}/{plots_directory}/"


# App Title

# Tabwise content
plot_urls=[]
intervals=[]
instruments=[]
return_types=[]

sessions=[]
latest14_urls=[]
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
                "return_type": return_type
            })
        elif file.name.endswith('.csv') and 'latest14' in file.name:
            
            if 'stats' not in str(file.name):
                latest14_content=file.name.split('_')
                latest14_url=plot_url_base+file.name
                joined_session="_".join((latest14_content[0:-5:1]))
                spaced_session=" ".join(joined_session.split('_'))
                instrument=(latest14_content[-1])[0:2]
                interval=latest14_content[-2]
                return_type=latest14_content[-4]

                sessions.append(spaced_session)
                latest14_urls.append({
                "url": latest14_url,
                'stats_url':plot_url_base+(file.name).split('.')[0]+'_stats.csv',
                "instrument": instrument,
                "interval": interval,
                "return_type": return_type,
                "session": [joined_session,spaced_session]
            })
            


unique_intervals=set(intervals)
unique_instruments=set(instruments)
unique_sessions=set(sessions)

print(latest14_urls)



# filtered_latest14_csvs = [data for data in latest14_urls if data["interval"] == '1m' and data["instrument"] =='ZN'  and data['session']=='All day']
# print(filtered_latest14_csvs)


#Define tab1:
with tab1:
    st.session_state.tab='TAB1'
    st.title("Combined Plots for all sessions")

    x = st.sidebar.selectbox("Select Interval",unique_intervals)
    y= st.sidebar.selectbox("Select Instrument",unique_instruments)
    
    # Store in session state
    st.session_state.x = x
    st.session_state.y = y

   
    filtered_plots = [plot for plot in plot_urls if plot["interval"] == x and plot["instrument"] == y]
    filtered_plots = sorted(
        filtered_plots,
        key=lambda plot: (plot["return_type"] == "Returns", plot["return_type"])
    )

    # Display plots
    try:
        if filtered_plots:
            for plot in filtered_plots:
                caption = f"{plot['return_type'].replace('Returns', 'Returns Distribution').replace('Volatility', 'Volatility Distribution')}"
                st.image(plot['url'],caption=caption)

        else:
            st.write("No plots found for the selected interval and instrument.")
    except FileNotFoundError as e:
        print(f'File not found: {e}. Please try again later.')

with tab2:
    #st.session_state.tab='TAB2'
    st.title("Latest 14 days Volatility Returns")
    
    # # Use stored values from session state
    x = st.session_state.get("x", list(unique_intervals)[0])
    y = st.session_state.get("y", list(unique_instruments)[0])
    #b= st.sidebar.selectbox("Select Instrumentss",unique_instruments)

    z = st.sidebar.selectbox("Select Session",unique_sessions)


    filtered_latest14_csvs = [data for data in latest14_urls if data["interval"] == x  and data["instrument"] ==y and data['session'][1]=='All day']
    try:
        if filtered_latest14_csvs:
            for l14_csv in filtered_latest14_csvs:
                st.subheader(f"Volatility Returns for Latest 14 days of the session: {(l14_csv['session'])[1]}")
                print(l14_csv['url'])
                df=(pd.read_csv(l14_csv['url']))
                st.dataframe(df)

                st.subheader("Descriptive Statistics")
                df2=(pd.read_csv(l14_csv['stats_url']))
                st.dataframe(df2)
            
        else:
            st.write("No data found for the selected session.")
    except FileNotFoundError as e:
        print(f'File not found: {e}. Please try again later.')

