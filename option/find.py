import os
import pandas as pd

def find_main(prefix="IO2407"):
    dfs = []
    for f in os.listdir("tq_data"):
        if f.startswith(prefix):
            df = pd.read_csv(f"tq_data/{f}")
            df['datetime'] = pd.to_datetime(df['datetime'])
            x = df[(df['datetime'].dt.hour==9)&(df['datetime'].dt.minute==30)&(df['open'] >= 30)&(df['open'] <= 100)].copy()
            x['date'] = x['datetime'].dt.date
            x = x[['date', 'open_oi', 'symbol', 'open']]
            x['dir'] = f[7]
            x['target'] = f[2:6]
            dfs.append(x)
    df = pd.concat(dfs)
    idx = df.groupby(['dir', 'date'])['open_oi'].transform(lambda x: x == x.max())
    x = df.loc[idx]
    x = x.reset_index().sort_values('date')
    x = x.drop(columns=['index'])
    # x groupby date then sum open_oi, if > 6000, then such date is valid, now we need to filter x by valid dates
    y = x.groupby('date')['open_oi'].sum()
    y = y[y>6000]
    x = x[x['date'].isin(y.index)]
    assert (x.groupby('date').size()==2).all(), x.groupby('date').size()!=2
    x.to_csv(f'main/{prefix}_main.csv', index=False)
    return x

def find_target_main(prefix="IO24", dir="C"):
    dfs = []
    for f in os.listdir("tq_data"):
        if not f.startswith(prefix) or not f'-{dir}-' in f:
            continue
        df = pd.read_csv(f"tq_data/{f}")
        df['datetime'] = df['datetime'].str[:19]
        df['target'] = df['symbol'].str[-4:]
        df = df[['datetime', 'target', 'open', 'high', 'low', 'close', 'volume', 'open_oi', 'close_oi', 'symbol']]
        dfs.append(df)
    df = pd.concat(dfs)
    # df groupby datetime and target, then in each group, keep the row with the smallest open
    #df = df.groupby(['datetime', 'target']).apply(lambda x: x.loc[x['symbol'].idxmin()])
    return df

def draw():
    import matplotlib.pyplot as plt
    df = pd.read_csv('diff.csv')

    df.plot(x='datetime', y='diff', kind='line')

    # 显示图像
    plt.xlabel('Datetime')  # x轴标签
    plt.ylabel('Diff')  # y轴标签
    plt.title('Datetime vs Diff')  # 图像标题
    plt.grid(True)  # 添加网格线

    plt.show()    

def find_real_min():
    df = pd.read_csv('hs3001m.csv', index_col=0)
    df = df[['open']]
    df['target'] = (df['open']/50).astype(int) * 50
    df = df.reset_index()
    df.columns = ['datetime', 'index_open', 'target']
    df['datetime'] = (pd.to_datetime(df['datetime']) - pd.Timedelta(minutes=1)).dt.strftime('%Y-%m-%d %H:%M:%S')
    df['target'] = df['target'].astype(str)
    print(df)
    x = find_target_main()
    df = df.merge(x, on=['datetime', 'target'], how='left')
    print(df)
    #x = df.groupby(['datetime', 'target']).apply(lambda x: x.loc[x['symbol'].idxmin()])
    y = df.groupby(['datetime', 'target']).apply(lambda x: x.loc[x['open'].idxmin()]).reset_index(drop=True)
    y['diff'] = y['open'] - y['index_open'] + y['target'].astype(int)
    y.to_csv('diff.csv', index=False)

if __name__ == '__main__':
    #find_main()
    find_real_min()
    draw()