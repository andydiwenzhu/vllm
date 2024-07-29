import akshare as ak
import matplotlib.pyplot as plt
import mplfinance as mpf
import pandas as pd
import os
import shutil
import sys

class Updater:
    def __init__(self, symbol='399300', freq='1min', history_path='data/hs300-1m.csv'):
        self.symbol = symbol
        self.freq = freq
        self.hpath = history_path
    
    def fetch_update(self, date):
        base = ak.index_zh_a_hist_min_em(
                symbol=self.symbol, period=self.freq[:-3], 
                start_date=f"{date} 09:31:00", end_date=f"{date} 15:00:00")  
        if len(base) == 0:
            print(date, " is NOT a trading day")   
        assert len(base) == 240 // int(self.freq[:-3]), base
        base.columns = ['datetime', 'open', 'close', 'high', 'low', 'volume', 'money', 'price']
        if base['open'].mean() < 1000:
            base['open'] = base['close'].shift(1)
            base.loc[base.index[0], 'open'] = base.loc[base.index[0], 'close']
        base['volume'] *= 100
        base = base.set_index('datetime')
        base.to_csv(f'updates/{date}.csv')

    def fetch_update_range(self, start_date, end_date):
        dates = [d.strftime('%Y-%m-%d') for d in pd.date_range(start_date, end_date)]
        for date in dates:
            self.fetch_update(date)

    def backup(self):
        self.bpath = '/'.join(['backup'] + self.hpath.split('/')[1:])
        shutil.copyfile(self.hpath, self.bpath)

    def use_backup(self):
        self.bpath = '/'.join(['backup'] + self.hpath.split('/')[1:])
        shutil.copyfile(self.bpath, self.hpath)

    def last_date(self):
        df = pd.read_csv(self.hpath, index_col=0)
        last_date = pd.to_datetime(df.index[-1]).date().strftime('%Y-%m-%d')
        return last_date

    def merge(self):
        self.backup()
        df = pd.read_csv(self.hpath, index_col=0)
        last_date = pd.to_datetime(df.index[-1]).date().strftime('%Y-%m-%d')
        dfs = [df]
        for f in sorted(os.listdir('updates')):
            if f[:10] > last_date:
                df = pd.read_csv(f'updates/{f}', index_col='datetime')
                df = df.drop(columns='price')
                dfs.append(df)
        df = pd.concat(dfs)
        df.to_csv(self.hpath)
        

class Vola:
    def __init__(self, symbol='hs300', data_path='data/hs300-1m.csv'):
        self.symbol = symbol
        self.data = pd.read_csv(data_path, index_col=0)
        self.data.index = pd.to_datetime(self.data.index)

    def candle(self, df, name, bi, si, bp='low', sp='high', volume=False, folder='figs'):
        add_plot = []
        if len(bi):
            df.loc[bi, "in"] = df.loc[bi, bp]
            add_plot.append(mpf.make_addplot(df["in"], scatter=True, markersize=100, marker='^', color='r'))
        if len(si):
            df.loc[si, "out"] = df.loc[si, sp]
            add_plot.append(mpf.make_addplot(df["out"], scatter=True, markersize=100, marker='v', color='g'))
        if 'volas' in df.columns:
            add_plot.append(mpf.make_addplot(df["volas"], panel=1, color='orange'))

        mpf.plot(df, type='candle', volume=volume, addplot=add_plot, title={"title": name, "y": 1}, figratio=(12, 4), tight_layout=True, figscale=1.5, savefig=f"{folder}/{name}.png")


    def vola(self, date, th=0.003, plot=True):
        if isinstance(date, str):
            date = pd.to_datetime(date).date()
        df = self.data[self.data.index.date == date].copy()
        lows = []
        highs = []
        trend = None
        idxmin = None
        idxmax = None
        cummin = 1000000
        cummax = 0
        for idx, r in df.iterrows():
            open, high, low, close = r['open'], r['high'], r['low'], r['close']
            if trend is None or trend == 'down':
                if low < cummin:
                    idxmin = idx
                    cummin = low
                elif high / cummin > 1 + th:
                    trend = 'up'
                    lows.append(idxmin)
                    cummin = 1000000
                    cummax = high
                    idxmax = idx
            if trend is None or trend == 'up':
                if high > cummax:
                    idxmax = idx
                    cummax = high
                elif low / cummax < 1 - th:
                    trend = 'down'
                    highs.append(idxmax)
                    cummax = 0
                    cummin = low
                    idxmin = idx
        r = len(lows) + len(highs)
        if plot:
            self.candle(df, f'{self.symbol}_{date}_{th}_{r}', lows, highs)
        return r

    def daily_ohlc(self, date):
        if isinstance(date, str):
            date = pd.to_datetime(date).date()
        x = self.data[self.data.index.date == date].copy()
        if len(x) == 0:
            return None
        return {"datetime": date,
                "open": x.iloc[0]['open'], 
                "close": x.iloc[-1]['close'], 
                "high": x['high'].max(), 
                "low": x['low'].min(), 
                "volume": x['volume'].sum()}

    def is_trading(self, date):
        if isinstance(date, str):
            date = pd.to_datetime(date).date()
        x = self.data[self.data.index.date == date].copy()
        return len(x) > 0

    def vola_range(self, start_date, end_date, th=0.003):
        dates = [d.strftime('%Y-%m-%d') for d in pd.date_range(start_date, end_date)]
        dates = [d for d in dates if self.is_trading(d)]
        for date in dates:
            self.vola(date, th)

    def vola_daily(self, start_date, end_date, th=0.003):
        dates = [d.strftime('%Y-%m-%d') for d in pd.date_range(start_date, end_date)]
        dates = [d for d in dates if self.is_trading(d)]
        volas = []
        records = []
        for date in dates:
            ohlc = self.daily_ohlc(date)
            r = self.vola(date, th, plot=False)
            volas.append(r)
            ohlc['volas'] = r
            records.append(ohlc)
        x = pd.DataFrame(records)
        x = x.set_index('datetime')
        x.index = pd.to_datetime(x.index)
        bi = x['volas'].nsmallest(1, keep='all').index
        si = x['volas'].nlargest(1, keep='all').index
        self.candle(x, f'{self.symbol}-{start_date}~{end_date}', bi, si, volume=True, folder='figs_daily')


if __name__ == '__main__':
    # date = sys.argv[1] if len(sys.argv) > 1 else pd.Timestamp.today().strftime('%Y-%m-%d')
    # u = Updater()
    # u.fetch_update(date)
    # u.merge()
    v = Vola(symbol='zz1000', data_path='data/zz1000_1m.csv')
    #v.vola(date)
    v.vola_daily('2024-01-01', '2024-07-26')
        
