import pandas as pd
from preprocessing import Preprocessing
class Events:
    """Combines Events from all_events_no_tz.xlsx (Economic Events sheet) and
    converts the timestamp to US/Eastern.
    """
    def __init__(self,excel):
        self.excel=excel
        self.sheets_dic=pd.read_excel(self.excel,sheet_name=None)
        self.combined_excel=self.merge_sheets(self.sheets_dic)
    def format_sheet(self,key,df):
        df.columns = df.columns.str.strip().str.lower()
        df=df.dropna(how='all')
        df['year'] = key#track the sheet name
        df['is_date'] = df['time'].str.contains(key, na=False)
        df['datetime']=df['time'].where(df['is_date'] == True)
        df['datetime']=pd.to_datetime(df['datetime'])
        df['datetime']=df['datetime'].ffill()
        df['date']=df['datetime']
        df['onlytime']=df['time'].where(df['is_date'] == False)
        df['onlytime'] = df['onlytime'].astype(str).str.split().str[1]
        df['onlytime']=df['onlytime'].fillna("")
        df['datetime'] = df.apply(lambda row: str(row['date']) + ' ' + str(row['onlytime'])
                                  if str(row['date']) != "" and str(row['onlytime']) != ""
                                  else str(row['date']), axis=1)
        df['datetime'] = df['datetime'].apply(lambda dt: " ".join([dt.split()[0], dt.split()[2]])
                                              if len(dt.split()) > 2 else dt)
        df['datetime']=df['datetime'].where(df['is_date'] == False)
        df['datetime']=pd.to_datetime(df['datetime'])#.where(df['is_date']==False)
        finaldf=df[['datetime','events','year']]
        finaldf.dropna(inplace=True)
        return finaldf
    def merge_sheets(self,sheets_dic):
        sheets_list=[]
        for key in (sheets_dic):
            years=[str(i) for i in range(2015,2025)]
            if key in (years):
                sheets_list.append(self.format_sheet(key,sheets_dic[key]))
                print(sheets_list)
        merged_sheet =pd.concat(sheets_list[::-1],ignore_index=True)
        return merged_sheet
    def save_sheet(self,sheet,name='combined.csv'):
        sheet.to_csv(name,index=False)
   
if __name__=='__main__':
    excelpath='all_events_no_tz.xlsx'
    combined_excel_path='all_events_combined_no_tz.csv'
    combined_excel_target_tz_path='all_events_combined_target_tz.csv'
    mydf=pd.read_excel(excelpath,sheet_name=None)
    myevents=Events(excelpath)
    combined_excel=myevents.combined_excel
    myevents.save_sheet(combined_excel,combined_excel_path)
    
    myeventsobject=Preprocessing(pd.read_csv(combined_excel_path))
    combined_excel_target_tz=myeventsobject.check_timezone(myeventsobject.dataframe,
                                   tz_col="datetime", default_tz = "Asia/Kolkata", target_tz = "US/Eastern")
    combined_excel_target_tz=combined_excel_target_tz[['datetime','events','year']]
    print(combined_excel_target_tz)
    myevents.save_sheet(combined_excel_target_tz,combined_excel_target_tz_path)