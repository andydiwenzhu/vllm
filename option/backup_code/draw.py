import akshare as ak
import pandas as pd
import mplfinance as mpf

def candle(df, symbol, date, ret, bi, si, baseline=None):
    # df.loc[df['Open'] % 2 == 1, "in"] = df.loc[df['Open'] % 2 == 1, "Low"]
    df.loc[bi, "in"] = df.loc[bi, "open"]
    df.loc[si, "out"] = df.loc[si, "low"]
    add_plot = [
        mpf.make_addplot(df["in"], scatter=True, markersize=100, marker='^', color='r'),
        mpf.make_addplot(df["out"], scatter=True, markersize=100, marker='^', color='g')
    ]
    if 'index' in df.columns:
        add_plot.append(
            mpf.make_addplot(df["index"], color='orange')
        )

    # 绘制K线图
    mpf.plot(df, type='candle', addplot=add_plot, title={"title": f"{symbol} {date} {ret:.2%}", "y": 1}, figratio=(12, 4), tight_layout=True, figscale=1.5, savefig=f"figs/{symbol}-{date}.png")#, style='charles')


def single_day(symbol, date, strategy, plot=True, baseline=True):
    df = pd.read_csv(f'tq_data/{symbol}.csv')
    df = df[df['datetime'].str[:10]==date]
    df['datetime'] = pd.to_datetime(df['datetime'])
    df.set_index('datetime', inplace=True)
    df.index.name = 'Time'

    m = 0
    if "fix" in strategy:
        for i in range(5, 60):
            bi = df.index[i]
            x = df.iloc[i:].copy()
            open_price = x.loc[bi, 'open']
            x['close_signal'] = x['high'].cummax() / open_price - 1 > strategy['fix']
            idxs = x[x['close_signal']].index
            if len(idxs):
                idx = idxs[0]
            else:
                idx = x.index[-1]
            if x.loc[:idx, 'low'].min() < open_price * 0.9:
                continue
            if idx == x.index[-1]:
                close_price = x.loc[idx, 'close']
                if close_price / open_price > m:
                    m = close_price / open_price
                    mbi = bi
                    msi = idx
            else:
                close_price = open_price * (1 + strategy['fix'])
                m = 1 + strategy['fix']
                mbi = bi
                msi = idx
    elif "drop" in strategy:
        for i in range(len(df) // 2):
            bi = df.index[i]
            x = df.iloc[i:].copy()
            open_price = x.loc[bi, 'open']
            x['close_signal'] = x['high'].cummax() - x['low'] > x.loc[bi, 'low'] * strategy['drop']
            idxs = x[x['close_signal']].index
            if len(idxs):
                idx = idxs[0]
            else:
                idx = x.index[-1]
            close_price = x.loc[idx, 'low']
            if close_price / open_price > m:
                m = close_price / open_price
                mbi = bi
                msi = idx
    else:
        raise ValueError("Unsupported strategy %s", strategy.keys)

    if m > 0:
        if plot:
            base = None
            if baseline:
                mapping = {"CFFEX.IO": "399300"}
                base = ak.index_zh_a_hist_min_em(symbol=mapping[symbol[:8]], period="1", start_date=f"{date} 09:30:00", end_date=f"{date} 15:00:00")
                print(date, base)
                base.columns = ['datetime', 'open', 'close', 'high', 'low', 'volume', 'amount', 'price']
                base = base[['datetime', 'price']]
                df['index'] = base.iloc[:-1].set_index(df.index)['price']
            candle(df, symbol, date, m - 1, mbi, msi)
        return m - 1
    return -0.1
