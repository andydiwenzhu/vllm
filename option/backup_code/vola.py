import akshare as ak
import pandas as pd
import mplfinance as mpf
import matplotlib.pyplot as plt
import numpy as np
import os

def candle(df, symbol, date, th, bi, si, baseline=None):
    add_plot = []
    if len(bi):
        df.loc[bi, "in"] = df.loc[bi, "low"]
        add_plot.append(mpf.make_addplot(df["in"], scatter=True, markersize=100, marker='^', color='r'))
    if len(si):
        df.loc[si, "out"] = df.loc[si, "high"]
        add_plot.append(mpf.make_addplot(df["out"], scatter=True, markersize=100, marker='v', color='g'))
    
    mpf.plot(df, type='candle', volume=True, addplot=add_plot, title={"title": f"{symbol} {date} {th}", "y": 1}, figratio=(12, 4), tight_layout=True, figscale=1.5, savefig=f"figs/{symbol}-{date}-{th:0.4f}.png")


def single_day(df, symbol, date, th = 0.002, plot=False):
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
    if plot:
        candle(df, symbol, date, th, lows, highs)
    return len(lows) + len(highs)
            

def vola(th=0.002, begin_date='2024-05-21'):
    df = pd.read_csv("hs3001m.csv", index_col=0)
    df.index = pd.to_datetime(df.index)
    df = df[df.index >= pd.to_datetime(begin_date)]
    dfs = [df]
    for f in os.listdir('updates'):
        x = pd.read_csv(f'updates/{f}', index_col='datetime')
        x = x.drop(columns='price')
        x.index = pd.to_datetime(x.index)
        dfs.append(x)
    df = pd.concat(dfs)

    volas = []
    records = []
    for date, x in df.groupby(df.index.date):
        r = single_day(x, 'hs300', date, th)
        volas.append(r)
        records.append({"datetime": pd.to_datetime(date), "volas": r, "open": x.iloc[0]['open'], "close": x.iloc[-1]['close'], "high": x['high'].max(), "low": x['low'].min(), "volume": x['volume'].sum()})
    #print(volas)
    #plt.hist(volas, bins=np.arange(min(volas), max(volas) + 2) - 0.5, edgecolor='black')
    #plt.show()
    x = pd.DataFrame(records)
    x = x.set_index('datetime')
    #y = x.loc[x['volume'].nlargest(len(x) // 3).index].copy()
    #y = y.sort_index()
    #print(y)
    y = x
    bi = y['volas'].nsmallest(4, keep='all').index
    si = y['volas'].nlargest(4, keep='all').index
    candle(x, 'hs300', '2024', th, si, bi)
    print(f'{th:0.4f} {len(bi)}: {list(x.loc[bi, "volas"].values)} {len(si)}: {list(x.loc[si, "volas"].values)}')
    # for b in bi:
    #     y = df[df.index.date == b]
    #     single_day(y, 'small', b.strftime("%Y-%m-%d"), th, True)
    # for s in si:
    #     y = df[df.index.date == s]
    #     single_day(y, 'large', s.strftime("%Y-%m-%d"), th, True)

def vola_adjust_th():
    df = pd.read_csv("hs3001m.csv", index_col=0)
    df.index = pd.to_datetime(df.index)
    volas = []
    records = []
    vols = []
    for date, x in df.groupby(df.index.date):
        records.append({"datetime": pd.to_datetime(date), "open": x.iloc[0]['open'], "close": x.iloc[-1]['close'], "high": x['high'].max(), "low": x['low'].min(), "volume": x['volume'].sum()})
    x = pd.DataFrame(records)
    x = x.set_index('datetime')
    x = x.sort_values('volume', ascending=False)
    x1 = x.iloc[:40]
    x2 = x.iloc[40:90]
    x3 = x.iloc[90:]
    volas = []
    for date in x1.index:
        volas.append(single_day(df[df.index.date==date], 'hs300', date, 0.003))
    x1['volas'] = volas
    bi1 = x1['volas'].nsmallest(5, keep='all').index
    si1 = x1['volas'].nlargest(5, keep='all').index
    print(f'{len(bi1)}: {list(x1.loc[bi1, "volas"].values)} {len(si1)}: {list(x1.loc[si1, "volas"].values)}')

    volas = []
    for date in x2.index:
        volas.append(single_day(df[df.index.date==date], 'hs300', date, 0.0025))
    x2['volas'] = volas
    bi2 = x2['volas'].nsmallest(5, keep='all').index
    si2 = x2['volas'].nlargest(5, keep='all').index
    print(f'{len(bi2)}: {list(x2.loc[bi2, "volas"].values)} {len(si2)}: {list(x2.loc[si2, "volas"].values)}')

    volas = []
    for date in x3.index:
        volas.append(single_day(df[df.index.date==date], 'hs300', date, 0.002))
    x3['volas'] = volas
    bi3 = x3['volas'].nsmallest(5, keep='all').index
    si3 = x3['volas'].nlargest(5, keep='all').index
    print(f'{len(bi3)}: {list(x3.loc[bi3, "volas"].values)} {len(si3)}: {list(x3.loc[si3, "volas"].values)}')

    bi = list(bi1) + list(bi2) + list(bi3)
    si = list(si1) + list(si2) + list(si3)
    print(bi)
    print(si)
    x = x.sort_index()
    candle(x, 'hs300', '2024', 0, si, bi)
    #x = pd.DataFrame(records)
    #plt.hist(vols, bins=np.arange(min(vols), max(vols) + 2) - 0.5, edgecolor='black')
    #plt.show()

def update(date='2024-07-19'):
    base = ak.index_zh_a_hist_min_em(symbol='399300', period="1", start_date=f"{date} 09:31:00", end_date=f"{date} 15:00:00")        
    print(base)
    base.columns = ['datetime', 'open', 'close', 'high', 'low', 'volume', 'money', 'price']
    if date == '2024-07-19':
        base['open'] = base['close'].shift(1)
        base.loc[base.index[0], 'open'] = 3500
    base['volume'] *= 100
    base = base.set_index('datetime')
    base.to_csv(f'updates/{date}.csv')


if __name__ == '__main__':
    #update('2024-07-26')
    #vola_adjust_th()
    vola(0.0025)
    #for th in range(1, 4):
    #    vola(th / 1000)