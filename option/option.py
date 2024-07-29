import os
import mplfinance as mpf
import pandas as pd



class Updater:
    def __init__(self, contract_path='data/contract.csv'):
        self.cpath = contract_path
        from tqsdk import TqApi, TqAuth
        self.api = TqApi(auth=TqAuth("anytimezdw", "123123123"))

   
    def __del__(self):
        self.api.close()


    def get_data(self, symbol, interval=60, days=1):
        klines = self.api.get_kline_serial(symbol, interval, days*60*60*4 // interval)
        klines['datetime'] = pd.to_datetime(klines['datetime'], unit='ns').dt.tz_localize('UTC').dt.tz_convert('Asia/Shanghai')
        for c in ['id', 'volume', 'open_oi', 'close_oi']:
            klines[c] = klines[c].astype(int)
        return klines

    def get_month(self, month="2406"):
        df = pd.read_csv(self.cpath)
        df = df[df['code'].str.contains(month)]
        print(df['code'])
        for ins in df['code']:
            ins = ins[:-5]
            df = self.get_data(f'CFFEX.{ins}', days=33)
            df.to_csv(f'tq_data/{ins}.csv', index=False)

class Strategy:
    def __init__(self, prefix='IO24'):
        self.prefix = prefix

    def find_main(self, lower=30, upper=100, oi=2000):
        dfs = []
        for f in os.listdir("tq_data"):
            if f.startswith(self.prefix):
                df = pd.read_csv(f"tq_data/{f}")
                df['datetime'] = pd.to_datetime(df['datetime'])
                x = df[(df['datetime'].dt.hour==9)&(df['datetime'].dt.minute==30)&(df['open'] >= lower)&(df['open'] <= upper)].copy()
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
        y = x.groupby('date')['open_oi'].min()
        y = y[y>oi]
        old_len = len(x)
        x = x[x['date'].isin(y.index)]
        assert (x.groupby('date').size()==2).all(), x.groupby('date').size()!=2
        print(f'valid: {(len(x)/old_len):0.2%}')
        x.to_csv(f'data/main_{self.prefix}.csv', index=False)
        return x

    def candle(self, df, name, bi, si, bp='low', sp='high', folder='figs_option'):
        add_plot = []
        
        df.loc[bi, "in"] = df.loc[bi, bp]
        add_plot.append(mpf.make_addplot(df["in"], scatter=True, markersize=100, marker='^', color='r'))
        
        df.loc[si, "out"] = df.loc[si, sp]
        add_plot.append(mpf.make_addplot(df["out"], scatter=True, markersize=100, marker='v', color='g'))
        
        mpf.plot(df, type='candle', volume=True, addplot=add_plot, title={"title": name, "y": 1}, figratio=(12, 4), tight_layout=True, figscale=1.5, savefig=f"{folder}/{name}.png")



    def sd_fix_pnl(self, date, symbol, last_open_time='10:30:00', fix_loss=0.1, fix_profit=0.2, plot=False):
        df = pd.read_csv(f'tq_data/{symbol[len("CFFEX."):]}.csv')
        df = df[df['datetime'].str[:len(date)] == date]
        df = df.set_index('datetime')
        df.index = pd.to_datetime(df.index)
        lot = pd.to_datetime(f'{date} {last_open_time}').tz_localize('Asia/Shanghai')
        x = df[df.index < lot]
        op = x['low'].min()
        bp = x['low'].idxmin()
        y = df[df.index >= bp]
        loss = y[y['low'] < op * (1-fix_loss)]
        profit = y[y['high'] > op * (1+fix_profit)]
        if len(loss) == 0:
            if len(profit):
                r = fix_profit
                sp = profit.index[0]
            else:
                r = df['close'].iloc[-1] / op - 1
                sp = df.index[-1]
        elif len(profit) == 0 or loss.index[0] <= profit.index[0]:
            r = -fix_loss
            sp = loss.index[0]
        else:
            r = fix_profit
            sp = profit.index[0]
        if plot:
            self.candle(df, f'{date}_{symbol}_fix_{fix_loss:0.4f}_{fix_profit:0.4f}', bp, sp)
        return r

    def test_fix_pnl(self, start_date='2024-07-01', end_date='2024-07-26', fix_loss=0.1, fix_profit=0.2, plot=True):
        df = pd.read_csv(f'data/main_{self.prefix}.csv')
        df = df[(df['date'] >= start_date)&(df['date'] <= end_date)]
        records = []
        for i, r in df.iterrows():
            pnl = self.sd_fix_pnl(r['date'], r['symbol'], fix_loss=fix_loss, fix_profit=fix_profit, plot=plot)
            records.append({'date': r['date'], 'pnl': pnl})
        x = pd.DataFrame(records)
        good = x.groupby('date')['pnl'].max()
        bad = x.groupby('date')['pnl'].min()
        print(fix_profit, bad.mean(), x['pnl'].mean(), good.mean())


if __name__ == '__main__':
    # u = Updater(contract_path='data/mo_contract.csv')
    # for i in range(1, 8):
    #     u.get_month(f'240{i}')
    m = Strategy(prefix='MO24')
    #m.find_main()
    for p in [0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]:
        m.test_fix_pnl('2024-07-01', '2024-07-26', fix_profit=p, plot=False)
