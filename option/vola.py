import pandas as pd
import mplfinance as mpf
import matplotlib.pyplot as plt
import numpy as np

def candle(df, symbol, date, bi, si, baseline=None):
    df.loc[bi, "in"] = df.loc[bi, "low"]
    df.loc[si, "out"] = df.loc[si, "high"]
    add_plot = [
        mpf.make_addplot(df["in"], scatter=True, markersize=100, marker='^', color='r'),
        mpf.make_addplot(df["out"], scatter=True, markersize=100, marker='v', color='g')
    ]
    
    mpf.plot(df, type='candle', addplot=add_plot, title={"title": f"{symbol} {date}", "y": 1}, figratio=(12, 4), tight_layout=True, figscale=1.5, savefig=f"figs/{symbol}-{date}.png")


def single_day(df, date, th = 0.002, plot=False):
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
        candle(df, 'hs300', date, lows, highs)
    return len(lows) + len(highs)
            

def vola(th=0.002):
    df = pd.read_csv("hs3001m.csv", index_col=0)
    df.index = pd.to_datetime(df.index)
    volas = []
    records = []
    for date, x in df.groupby(df.index.date):
        r = single_day(x, date, th)
        volas.append(r)
        records.append({"datetime": pd.to_datetime(date), "volas": r, "open": x.iloc[0]['open'], "close": x.iloc[-1]['close'], "high": x['high'].max(), "low": x['low'].min()})
    #print(volas)
    #plt.hist(volas, bins=np.arange(min(volas), max(volas) + 2) - 0.5, edgecolor='black')
    #plt.show()
    x = pd.DataFrame(records)
    x = x.set_index('datetime')
    bi = x['volas'].nsmallest(10).index
    si = x['volas'].nlargest(10).index
    candle(x, 'hs300', '2024', bi, si)


if __name__ == '__main__':
    vola(0.003)