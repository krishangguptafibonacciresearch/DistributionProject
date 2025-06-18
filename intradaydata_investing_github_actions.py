# Get all tables with class 'historical-data'
import pandas as pd
import numpy as np
from selenium import webdriver
from selenium.webdriver.common.by import By
import traceback as trb
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
import chromedriver_autoinstaller
from pyvirtualdisplay import Display


class Intraday_Investing:
    
    def __init__(self,url,interval):
        self.url=url
        self.interval=interval
    
    def fetch_data_investing(self):
        MaxAttempts=20
        for _ in range(MaxAttempts):
            try:
                my_fgbl=self.StartWebScrapper(self.url,self.interval)
                return my_fgbl
            except Exception as e:
                print('Some error occurred in fetching data. Retrying...')
                print(e)
                trb.print_exc()
                continue

        return pd.DataFrame(columns=['Datetime','Adj Close','Close','High','Low','Open','Volume','%Change'])

    def StartWebScrapper(self,myurl="",interval=""): 

        display = Display(visible=0, size=(800, 800))  
        display.start()
        chromedriver_autoinstaller.install()  # Check if the current version of chromedriver exists
                                            # and if it doesn't exist, download it automatically,
                                            # then add chromedriver to path
        chrome_options = webdriver.ChromeOptions()    
        # Add your options as needed    
        options = [
            # Define window size here
            "--window-size=1200,1200",
            "--ignore-certificate-errors",
            "--headless=new",
            "--disable-gpu",
            #"--window-size=1920,1200",
            "--ignore-certificate-errors",
            "--disable-extensions",
            "--no-sandbox",
            "--disable-dev-shm-usage",
            '--remote-debugging-port=9222'
        ]
        for option in options:
            chrome_options.add_argument(option)
   
        # Set Chrome options to disable JavaScript
        prefs = {
            "profile.managed_default_content_settings.javascript": 2  # Disable JavaScript
        }
        chrome_options.add_experimental_option("prefs", prefs)

        # Initialize WebDriver with options
        driver = webdriver.Chrome(options=chrome_options)

        # URL of the page to scrape
        if myurl=="":
            url = "https://in.investing.com/rates-bonds/euro-bund-historical-data"

        # Initialize WebDriver
        # driver = webdriver.Chrome()

        # Open the page
        driver.get(url=myurl)

        # Get the column headings for fgbl data
        fgbl_columns=['Date', 'Price', 'Open', 'High', 'Low', 'Vol.', 'Change %']
        try:
            table_heading = driver.find_elements(By.XPATH, "//thead")
            for idx, table in enumerate(table_heading):
                if table.text:
                    fgbl_columns=table.text.split('\n')
        except Exception as e:
            fgbl_columns=['Date', 'Price', 'Open', 'High', 'Low', 'Vol.', 'Change %']

        else:
            print('Successfully extracted the columns:\n')

        finally:
            print('The columns are:\n')
            print(fgbl_columns)
                        
        # Get the fgbl historical data
        table_data = driver.find_elements(By.XPATH, "//tr[contains(@class, 'historical-data')]")

        # Store each row separately
        fgbl_row_data=[]
        # Check if any tables are found and print their text
        if table_data:
            for idx, row in enumerate(table_data):
                #print(f"row {idx + 1}:")
                #print(row.text)  # Print the HTML of each table
                row_data=row.text.replace(',','')
                fgbl_row_data.append(row_data.split(' '))
        else:
            print("No fgbl data found.")
        
        driver.close()
        return self.ExportCSV(fgbl_columns,fgbl_row_data,interval)
    

    def ExportCSV(self,table_heading_list,table_row_list,interval):
        table_new_row_list=[]
        for row in table_row_list:
            templist=[]
            date="-".join(row[0:3])
            templist.append(date)
            templist+=row[3:]
            table_new_row_list.append(templist)
        fgbl_df=pd.DataFrame(table_new_row_list,columns=table_heading_list)
        return self.ProcessCSV(fgbl_df,interval)


    def ProcessCSV(self,csv_file,interval):
        fgbl_csv=csv_file.copy()
        #fgbl_csv.index=fgbl_csv[fgbl_csv.columns[0]]
        fgbl_csv.columns=['Datetime','Close','Open','High','Low','Vol.','Change %']
        fgbl_csv['Adj Close']=fgbl_csv['Close']
        fgbl_csv['Volume']=fgbl_csv['Vol.']
        fgbl_csv=fgbl_csv[['Datetime','Adj Close','Close','High','Low','Open','Volume']]

        if interval in ['1d','1w','1mo']:
            fgbl_csv['Datetime']=pd.to_datetime(fgbl_csv['Datetime'])
            fgbl_csv['Datetime']=fgbl_csv['Datetime'].dt.strftime("%Y-%m-%d")
            fgbl_csv.index=fgbl_csv['Datetime']
            fgbl_csv.drop(columns=['Datetime'],inplace=True,axis=1)
            fgbl_csv.index.name='Datetime'
        #fgbl_csv.reset_index(drop=True,inplace=True)
        fgbl_csv.sort_index(inplace=True,ascending=True,axis=0)
        return fgbl_csv
    

if __name__=='__main__':
    fgbl_object=Intraday_Investing(url="https://in.investing.com/rates-bonds/euro-bund-historical-data",interval='1d')
    print(fgbl_object.fetch_data_investing())