# For github Actions:

from event_calendar import StartWebscrapper,all_timezones,time,webdriver,os

def runner(**kwargs):
    # Initialize the driver
    session = webdriver.Chrome()

    # Open the webpage
    URL = 'https://tradingeconomics.com/calendar'
    session.get(URL)
    time.sleep(30)

    # Set Parameters
    OutputDirectory=kwargs.get('OutputDirectory')
    TargetTimezone=kwargs.get('TargetTimezone')
    SelectCountries=kwargs.get('SelectCountries')
    
    script_dir = os.path.dirname(os.path.abspath(__file__))  # Directory of the script
    OutputDirectory = os.path.join(script_dir, OutputDirectory)

    TargetTimezoneName=str(list(TargetTimezone.keys())[0])
    TargetTimezoneValue=str(list(TargetTimezone.values())[0])
    OutputFile=TargetTimezoneName+'_'+('_').join(SelectCountries)+'_trad_eco_cal_'


    # Fetch data from Trading Economics Calendar
    original_df,filtered_df= StartWebscrapper(driver=session,
                get_country=SelectCountries,
                TargetTimezone=TargetTimezone,
                TargetTimezoneName=TargetTimezoneName,
                TargetTimezoneValue=TargetTimezoneValue,
                output_directory_name=OutputDirectory,
                output_file_name=OutputFile,
                all_timezones=all_timezones,
                display_country=False)
    print('Data fetching completed.')
    print(original_df)
    print(filtered_df)


#Scatter events by country and timezone
SelectCountries =  ['US']
TargetTimezone = {'IST':'UTC +5:30'}
OutputDirectory='Input_data'
runner(OutputDirectory=OutputDirectory,
       TargetTimezone=TargetTimezone,
       SelectCountries=SelectCountries)

SelectCountries =  ['US']
TargetTimezone = {'EST':'UTC -5'}
OutputDirectory='Input_data'
runner(OutputDirectory=OutputDirectory,
       TargetTimezone=TargetTimezone,
       SelectCountries=SelectCountries)
