import akshare as ak
import matplotlib.pyplot as plt
import mplfinance as mpf
import os
import pandas as pd
import zipfile

from collections import Counter
from datetime import datetime, date
from itertools import product
from pathlib import Path

current_month = 7

def get_data(api, symbol, interval=60, days=1):
    try:
        klines = api.get_kline_serial(symbol, interval, days*60*60*4 // interval)
    except Exception as e:
        print(e)
        return

    klines['datetime'] = pd.to_datetime(klines['datetime'], unit='ns').dt.tz_localize('UTC').dt.tz_convert('Asia/Shanghai')
    for c in ['id', 'volume', 'open_oi', 'close_oi']:
        klines[c] = klines[c].astype(int)
    return klines

def get_first_mode(lst):
    counter = Counter(lst)
    max_count = max(counter.values())
    for item, count in counter.items():
        if count == max_count:
            return item
    return None  # 如果列表为空


def run(interval=60):
    from tqsdk import TqApi, TqAuth
    api = TqApi(auth=TqAuth("anytimezdw", "123123123"))
    
    last_date_str = pd.read_csv('tq_data/main.csv').iloc[-1]['date']
    last_date = datetime.strptime(last_date_str, '%Y-%m-%d').date()
    days = (date.today() - last_date).days

    real_days = []
    for month in range(current_month, current_month+1):
        for dir in ['C', 'P']:
            for t in range(18):
                target = 3100 + t * 50
                symbol = f"CFFEX.IO240{month}-{dir}-{target}"
                print(symbol)
                df = get_data(api, symbol, interval=interval, days=days)
                df = df[df['datetime'].dt.date > last_date]
                real_days.append(len(df) // (60*60*4 // interval))
                file = Path(f"tq_data/{symbol}.csv")
                if file.exists():
                    x = pd.read_csv(file)
                    x = pd.concat([x, df])
                    x.to_csv(file, index=False)
                else:
                    df.to_csv(file, index=False)
    return get_first_mode(real_days)


def find_main():
    dfs = []
    for dir, month, t in product(['C', 'P'], range(1, current_month+1), range(22)):
        target = 3000 + t * 50
        file = Path(f"tq_data/CFFEX.IO240{month}-{dir}-{target}.csv")
        if file.exists():
            df = pd.read_csv(file)
            df['datetime'] = pd.to_datetime(df['datetime'])
            x = df[(df['datetime'].dt.hour==9)&(df['datetime'].dt.minute==30)&(df['open'] >= 60)&(df['open'] <= 120)].copy()
            x['date'] = x['datetime'].dt.date
            x = x[['date', 'open_oi', 'symbol', 'open']]
            x['dir'] = dir
            x['target'] = target
            dfs.append(x)
    df = pd.concat(dfs)
    idx = df.groupby(['dir', 'date'])['open_oi'].transform(lambda x: x == x.max())
    x = df.loc[idx]
    x = x.reset_index().sort_values('date')
    x = x.drop(columns=['index'])
    x.to_csv('tq_data/main.csv', index=False)
    return x

def draw(x, v1='target', v2='open_oi'):
    # 确保两个DataFrame的date列是datetime类型
    df1 = x[x['dir'] == 'C']
    df2 = x[x['dir'] == 'P']

    # 创建一个图形和两个子图（一个用于每个DataFrame）
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 8))  # 2行1列

    # 上部图表：df1的线图和柱状图
    ax1.set_xlabel('date')
    ax1.set_ylabel(v1, color='tab:red')
    ax1.plot(df1['date'], df1[v1], color='tab:red')
    ax1.tick_params(axis='y', labelcolor='tab:red')

    ax3 = ax1.twinx()
    ax3.set_ylabel(v2, color='tab:blue')
    ax3.bar(df1['date'], df1[v2], color='tab:blue', alpha=0.7)
    ax3.tick_params(axis='y', labelcolor='tab:blue')

    # 下部图表：df2的线图和柱状图
    ax2.set_xlabel('date')
    ax2.set_ylabel(v1, color='tab:red')
    ax2.plot(df2['date'], df2[v1], color='tab:red')
    ax2.tick_params(axis='y', labelcolor='tab:red')

    ax4 = ax2.twinx()
    ax4.set_ylabel(v2, color='tab:blue')
    ax4.bar(df2['date'], df2[v2], color='tab:blue', alpha=0.7)
    ax4.tick_params(axis='y', labelcolor='tab:blue')

    # 设置图表标题
    fig.suptitle(f'date vs {v1} (lines) & {v2} (bars) - top: call, bottom: put')

    # 调整子图间距
    plt.subplots_adjust(hspace=0.5)

    # 显示图表
    plt.show()

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


def single_day(symbol, date, drop=0.05, plot=True, baseline=True):
    df = pd.read_csv(f'tq_data/{symbol}.csv')
    df = df[df['datetime'].str[:10]==date]
    df['datetime'] = pd.to_datetime(df['datetime'])
    df.set_index('datetime', inplace=True)
    df.index.name = 'Time'

    m = 0
    for i in range(len(df) // 2):
        bi = df.index[i]
        x = df.iloc[i:].copy()
        open_price = x.loc[bi, 'open']
        #x['close_signal'] = x['low'] / x['high'].cummax() < 1 - drop
        x['close_signal'] = x['high'].cummax() - x['low'] > x.loc[bi, 'low'] * drop
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


def test_drop(drop=0.05, days=40, plot=False):
    df = pd.read_csv('tq_data/main.csv')
    df = df.iloc[-2*days:]
    df['ret'] = df.apply(lambda x: single_day(x['symbol'], x['date'], drop=drop, plot=plot), axis=1)
    max_idx = df.groupby('date')['ret'].idxmax()
    df = df.loc[max_idx].reset_index(drop=True)
    return df['ret'].mean()

def archive_folder(folder_path, output_filename):
    with zipfile.ZipFile(output_filename, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, dirs, files in os.walk(folder_path):
            for file in files:
                # 将文件添加到zip文件中
                # os.path.join用于正确处理不同操作系统中的路径分隔符
                file_path = os.path.join(root, file)
                # 写入zip文件时，需要指定文件夹路径，以保持文件结构
                # 因此，我们使用root作为zip文件中的路径，file作为文件名
                zipf.write(file_path, os.path.relpath(file_path, folder_path))

def update():
    #archive_folder("tq_data", f"backup_tq_data_{date.today().strftime('%Y-%m-%d')}.zip")
    #real_days = run()
    #find_main()
    for d in range(1, 25):
        r = test_drop(drop=d/100, days=100, plot=False)
        print(d/100, r, r/d)

if __name__ == '__main__':
    update()
