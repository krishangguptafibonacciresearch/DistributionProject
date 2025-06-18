import datetime
from datetime import timedelta
import pandas as pd
import yfinance as yf
from preprocessing import ManipulateTimezone
class Intraday:
    """
    Import data from yfinance. 
    """
    
    def __init__(self,tickers=[],interval="",start_intraday= -1,end_intraday= -1):
        self.dict_symbols = {
        "ZN=F":["ZN","10-Year T-Note Futures"],
        "ZB=F":["ZB","30-Year T-Bond Futures"],
        "ZF=F":["ZF","5-Year US T-Note Futures"],
        "ZT=F":["ZT","2-Year US T-Note Futures"],
        "DX-Y.NYB":["DXY","US Dollar Index"],
        "CL=F":["CL","Crude Oil futures"],
        "GC=F":["GC","Gold futures"],
        "NQ=F":["NQ","Nasdaq 100 futures"],
        "^DJI":["DJI","Dow Jones Industrial Average"],
        "^GSPC":["GSPC","S&P 500"]
        }
        if tickers!=[]:
            tempdic={}
            for ticker in tickers:
                if ticker in self.dict_symbols:
                    tempdic[ticker]=self.dict_symbols[ticker]
                else:
                    print(f'Data with respect to {ticker} not found in Yahoo Finance')
            self.dict_symbols=tempdic
        
        self.update_tickers_and_symbols()
        self.interval=interval
        self.start_intraday=start_intraday
        self.end_intraday=end_intraday

    def update_dict_symbols(self, new_dict_symbols):
        self.dict_symbols = new_dict_symbols
        self.update_tickers_and_symbols()

    def update_tickers_and_symbols(self):
        self.tickers = list(self.dict_symbols.keys())
        self.symbols = [i[0] for i in list(self.dict_symbols.values())]
        print('Your Ticker Dictionary:',self.dict_symbols)

    def fetch_data_yfinance(self,specific_tickers=[]): 
        """ Extracts Intraday data for specific tickers from Yahoo Finance.
        """
        today = datetime.datetime.now()
        data=pd.DataFrame()
        if self.start_intraday!=-1 and self.end_intraday!=-1: 
            # Both start and end date is specified
            end=(today-timedelta(days=self.end_intraday)).strftime("%Y-%m-%d")
            start = (today - timedelta(days=self.start_intraday)).strftime("%Y-%m-%d")
            data = yf.download(tickers=self.tickers, start=start, end=end, interval=self.interval)

        elif self.start_intraday==-1 and self.end_intraday==-1:
            # Neither start nor end date is specified
            data = yf.download(tickers=self.tickers, interval=self.interval)
        # Return data for specific tickers as a dictionary 
        try:        
            if specific_tickers!=[]:
                alltickerdata={}
                stackeddata=data.stack(level=0,future_stack=False)
                stackeddata.index.names=['Datetime','Price']
                for col in stackeddata.columns:
                    col_data=stackeddata[col].unstack()
                    col_data.columns.name = None
                    alltickerdata[col]=col_data
                return alltickerdata
            else:
                return data
        except Exception as e:
            print(e)
            return data
        
    @classmethod
    def data_acquisition(self,cleandata):
        mydata=cleandata.copy()
        mydata=mydata.dropna(axis=0)
        mydata.index.name=None
        mydata.columns.name=None
        mydata['timestamp']=mydata.index
        mydata.reset_index(drop=True,inplace=True)
        return mydata
    

