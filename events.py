import pandas as pd
from preprocessing import ManipulateTimezone
class Events:
    """Combines Events from Economic Events sheet and converts the timestamp to US/Eastern.
    """
    def __init__(self,excel,tier_dic={},flag_dic={}):
        self.excel=excel
        self.sheets_dic=pd.read_excel(self.excel,sheet_name=None)
        self.flag_dic=flag_dic
        if tier_dic=={}:
            tier_sheet='NA'
            for sheet in self.sheets_dic.keys():
                if 'tier' in str(sheet).strip().lower() or 'tiers' in str(sheet).strip().lower():
                    tier_sheet=sheet
                    break
            if tier_sheet=='NA':
                raise AttributeError('No Tier Data found. Please add a Tier Sheet with Events and Tier value.')
            temp_dic=(self.sheets_dic[tier_sheet].to_dict())
            events_tiers=list(temp_dic.values())[0:2]
            allevents=events_tiers[0]
            alltiers=events_tiers[1]
            for index1,index2 in zip(allevents,alltiers):
                if index1==index2:
                    tier_dic[allevents[index1]]=alltiers[index1]
            self.tier_dic=tier_dic
        else:
            self.tier_dic=tier_dic
        print(self.tier_dic)
        self.combined_excel=self.merge_sheets(self.sheets_dic,self.tier_dic,self.flag_dic)

    def __str__(self):
        return f'The selected events excel contains the following sheets: {list(self.sheets_dic.keys())}'
    

    def assign_tier_helper(self, row,tier_dic):
        myevent = str(row['events']).strip().lower()
        for event_key in tier_dic:
            if str(event_key).strip().lower() in myevent:
                return tier_dic[event_key]
        return 4  # Default value if no match found

    def assign_tier(self, finaldf,tier_dic):
        if 'tier' in finaldf.columns:
            return finaldf['tier']
        elif 'tiers' in finaldf.columns:
            return finaldf['tiers']
        else:
            finaldf['tier'] = finaldf.apply(lambda row: self.assign_tier_helper(row,tier_dic), axis=1)
            return finaldf['tier']
    

    def assign_flag_helper(self, row, flag_dic):
        myevent = str(row['events']).strip().lower()
        for flag_key, flag_condition in flag_dic.items():
            row[flag_key] = 0
            for event_key in flag_condition:
                if str(event_key).strip().lower() in myevent:
                    row[flag_key] = 1
                    break      
        if row['IND_Tier1']==0 and row['IND_Tier2']==0 and row['IND_Tier3']==0:
            row['IND_Tier4']=1
        else:
            row['IND_Tier4']=0        
        return row

    def assign_flag(self,finaldf, flag_dic):
        if not flag_dic:  # Check if dic is empty
            return finaldf
        # Apply the helper function row-wise
        finaldf = finaldf.apply(lambda row: self.assign_flag_helper(row, flag_dic), axis=1)
        return finaldf

        
    def format_sheet(self,key,df,tier_dic,flag_dic):
        df.columns = df.columns.str.strip().str.lower()
        df=df.dropna(how='all')
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
        df['year']=(df['datetime'].dt.year).astype('Int64')
        finaldf=df[['datetime','events','year']]
        finaldf['tier']=self.assign_tier(finaldf,tier_dic)
        finaldf=finaldf[['datetime','events','year','tier']]
        finaldf=self.assign_flag(finaldf,flag_dic)
        finaldf.dropna(inplace=True)
        return finaldf
    
    def merge_sheets(self,sheets_dic,tier_dic,flag_dic):
        sheets_list=[]
        for key in (sheets_dic):
            years=[str(i) for i in range(2015,3015)]
            if key in (years):
                sheets_list.append(self.format_sheet(key,sheets_dic[key],tier_dic,flag_dic))
        merged_sheet =pd.concat(sheets_list[::-1],ignore_index=True)
        return merged_sheet
    
    def save_sheet(self,sheet,name='combined.csv'):
        sheet.to_csv(name,index=False)

    