import akshare as ak
import pandas as pd

from tqsdk import TqApi, TqAuth
api = TqApi(auth=TqAuth("anytimezdw", "123123123"))
def get_data(api, symbol, interval=60, days=1):
    klines = api.get_kline_serial(symbol, interval, days*60*60*4 // interval)
    klines['datetime'] = pd.to_datetime(klines['datetime'], unit='ns').dt.tz_localize('UTC').dt.tz_convert('Asia/Shanghai')
    for c in ['id', 'volume', 'open_oi', 'close_oi']:
        klines[c] = klines[c].astype(int)
    return klines

def get_month(month="2406"):
    df = pd.read_csv('contract.csv')
    df = df[df['code'].str.contains(month)]
    print(df['code'])
    for ins in df['code']:
        ins = ins[:-5]
        df = get_data(api, f'CFFEX.{ins}', days=33)
        df.to_csv(f'tq_data/{ins}.csv', index=False)



if __name__ == '__main__':
    get_month("2407")
    api.close()
