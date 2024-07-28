import pandas as pd
from draw import single_day

def test_drop(prefix="IO2407", strategy={"drop": 0.1}, days=40, plot=False, baseline=False, dir="CP"):
    df = pd.read_csv(f'main/{prefix}_main.csv')
    df = df.iloc[-2*days:]
    if len(dir) < 2:
        df = df[df['dir'] == dir]
    df['ret'] = df.apply(lambda x: single_day(x['symbol'][6:], x['date'], strategy=strategy, plot=plot, baseline=baseline), axis=1)
    max_idx = df.groupby('date')['ret'].idxmax()
    df = df.loc[max_idx].reset_index(drop=True)
    return df['ret'].mean(), f"win ratio: {len(df[df['ret'] > 0])/len(df):0.3f}"


if __name__ == '__main__':
    #print(test_drop(strategy={"fix": 0.75}, plot=True, dir="C"))
    for x in range(10, 80, 5):
        print(x/100, test_drop(strategy={"fix": x/100}, plot=False, dir='C'))