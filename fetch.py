import requests
from itertools import combinations
import pandas as pd
import json
from time import sleep

#Main interface functions
def trades(platform, pools=None, coins=None, t_start=None, t_end=None, df=True, candles=False, reindex=True, trunc=True):
    """
    Pulls trade history for particular pools or coins
    
    platform: protocol to pull data for (e.g., 'curve', 'uni', 'sushi')
    pools: pool addresses (string or list of strings)
    coins: coin addresses (list of strings)
    
    Note: You can specify pools or coins alone. If both, returns intersection of the args

    Optional:
    t_start/t_end: Unix epochs (in seconds) for start/end of data. If None, returns entire history
    df: return data as list of price/volume DataFrames for each pair of coins
    candles: return data as list of OHLC+volume DataFrames. Input is candle duration (e.g., '5min', '4H', '1D'). Overrides df.
    
    Candle Options:
    reindex: same candle times across all coin pairs; candle included if any price changes
    trunc: truncate candles to first candle with data for all coin pairs
     
    """
    platform = aliascheck(Query.Trades, platform)
    
    if pools is None and coins is None:
        raise ValueError('Must specify pool(s) and/or coins')
    if coins is not None and len(coins) < 2:
        raise ValueError('Must input 2+ coins')
    
    #Fetch data
    args = [pools, coins, coins]
    query = Query.Trades[platform]()
    data = query.request(args, t_start = t_start, t_end = t_end)
    
    #Format as DataFrame
    if df or candles:
        if coins is None:
            coins = poolcoins(platform, pools)
        data = query.toDF(data, coins)
        
    if candles:
        data = query.tocandles(data, dur=candles, reindex=reindex, trunc=trunc)
        
    
    return data
    
def poolcoins(platform, pools):
    """
    Returns addresses for coins in the specified pool(s)
    
    platform: protocol to pull data for (e.g., 'curve', 'uni', 'sushi')
    pools: pool addresses (string or list of strings)
    """
    
    platform = aliascheck(Query.PoolCoins, platform)
    
    query = Query.PoolCoins[platform]()
    
    if type(pools) is str:
        coinaddresses = query.request(pools)
        
    elif type(pools) is list:
        coinaddresses = []
        for pool in pools:
            coinaddresses += query.request(pool)
            
    else:
        raise ValueError('Pools must be string or list')
    
    return coinaddresses

#General helper functions 
def aliascheck(query, alias):
    alias = alias.lower()
    if alias not in query:
        raise ValueError(
            'Platform alias not found. Available aliases (case-insensitive):\n'+
            str(list(query.keys()))
        )
    
    return alias

def getdictpath(dct, path):
    for p in path:
        dct = dct[p]
    return dct
    
def formatargs(filters, args):
    argstr = ''
    for i in range(len(filters)):
        filt = filters[i]
        arg = args[i]
        if arg is not None:
            if type(arg) == str:
                arg = [arg]
            s = json.dumps(arg).lower()
            argstr += '%s_in: %s, ' % (filt, s)
    
    return argstr
    
def savecandles(candles, coinnames):
    #Saves candles in format for Curve v2 simulator
    
    combos = list(combinations(coinnames,2))
    for i in range(len(combos)):
        coins = combos[i]
        path = coins[1]+coins[0]+'-1m.json'
        
        candles[i].index = candles[i].index.asi8//int(1e6) #timestamp format
        candles[i] = candles[i].astype(str)
        candles[i].reset_index(inplace=True)
        
        candles[i].to_json(path_or_buf=path, orient='values')


#Schemas for each type of query
class Schemas:
    pass        
        
#Schemas/functions for pulling trades   
class Trades(Schemas):
    def buildquery(self, args):
        obj = self.obj
        argstr = formatargs(self.filters, args)
        fields = ''
        for field in self.fields:
            fields+=field+' \n '
    
        query = f"""{{
          {obj}(first: %s, orderBy: timestamp, orderDirection: %s, 
          where: {{ {argstr} timestamp_gt: %s}}){{
            {fields}
          }}
        }}"""
    
        return query
    
    def request(self, args, t_start=None, t_end=None):
        query = self.buildquery(args)
        url = self.url
        objpath = self.obj
        
        print('Fetching %s data...' % type(self).__name__, end='\n')
        
        if t_start is None:
            t_prev = 0
        else:
            t_prev = int(t_start-1)
            
        if t_end is None:
            try:
                req = requests.post(url, json={"query": query % (1, 'desc', 0)}) #most recent trade
                t_end = getdictpath(req.json(), ['data', objpath, 0, 'timestamp']) #timestamp of most recent trade
            except:
                raise Exception('Data path missing from initial response. Check query parameters.')
                
        t_end = int(t_end)
        
        data = []
        while t_prev < t_end:
            retry = True
            while retry:
                req = requests.post(url, json={"query": query % (1000, 'asc', t_prev)})
                if req.status_code == 200:
                    try: 
                        t_prev = getdictpath(req.json(), ['data', objpath, -1, 'timestamp'])
                        t_prev = int(t_prev)
                        retry = False
                    except:
                        print('\nData missing from last chunk. Retrying...', end='\n')
                        sleep(.1)
                else:
                    print('Error %s: Retrying..' % str(req.status_code), end='\n')
                    sleep(.1)
 
            chunk = getdictpath(req.json(), ['data', objpath])
            data += self.formatresult(chunk, t_end=t_end)
            print('Retrieved timestamp: %s / %s' % (t_prev, t_end),  end='\r')
        
        print('\nDownload complete.')
    
        return data
        
    def toDF(self, data, coinorder):
        df = pd.DataFrame(data)
        df.set_index('timestamp', inplace=True)
        combos = list(combinations(coinorder,2))
        
        #Format as prices, volume
        prices = []
        for combo_i in combos:
            df1 = df[(df.tokenBought == combo_i[1]) & (df.tokenSold == combo_i[0])]
            df2 = df[(df.tokenBought == combo_i[0]) & (df.tokenSold == combo_i[1])]
        
            #Prices
            p1 = df1.amountSold/df1.amountBought
            p2 = df2.amountBought/df2.amountSold
        
            #Volume in base currency
            v1 = df1.amountBought
            v2 = df2.amountSold
        
            p = pd.concat([p1, p2]).sort_index()
            v = pd.concat([v1, v2]).sort_index()
            pv = pd.concat([p,v], axis=1)
            pv.columns = ['price', 'volume']
            pv.index = pd.to_datetime(pv.index, unit='s', utc=True)
            prices.append(pv)

        return prices
        
    def tocandles(self, prices, dur='1min', reindex=True, trunc=True):
        candles = []
        t0 = []
        for df in prices:
            #t, open, hi, lo, close, volume
            p = df.price.resample(dur).ohlc().dropna()
            p.open[1:] = p.close.shift()[1:] #fixing screwy pandas candles
            p.low = p[['open','low']].min(axis=1)
            p.high = p[['open','high']].max(axis=1)
            v = df.volume.resample(dur).sum()
            v = v[v != 0]
            pv = pd.concat([p,v], axis =1)
            candles.append(pv)
            t0.append(pv.index[0])
    
        #Reindex all candles to same times
        if reindex:
            t = pd.concat(candles).index.unique().sort_values()
            for i in range(len(candles)):
                c = candles[i].reindex(t)
                closes = c['close'].fillna(method='ffill')
                c.fillna({'open': closes, 'high': closes, 'low': closes, 'close': closes, 'volume': 0}, inplace=True)
                if trunc:
                    c = c[c.index >= max(t0)] #truncate to first candle with all pairs
                candles[i] = c
        
        return candles
            
class TradesCurve(Trades):
    def __init__(self):
        self.url = Subgraph.Curve.url

    obj = 'swapEvents'
    filters = [
        'pool', 
        'tokenBought', 
        'tokenSold',
    ]
    fields = [    
        'timestamp',
        'tokenSold',
        'tokenBought',
        'amountSold',
        'amountBought',
    ]
    
    def formatresult(self, chunk, t_end=None):
        data = []
        for d in chunk:
            if t_end is not None and int(d['timestamp']) > t_end:
                break
            d_out = d
            d_out['timestamp'] = int(d['timestamp'])
            d_out['amountBought'] = float(d_out['amountBought'])
            d_out['amountSold'] = float(d_out['amountSold'])
            data.append(d_out)
        return data
    
class TradesUni(Trades):
    def __init__(self):
        self.url = Subgraph.Uni.url
      
    obj = 'swaps'
    filters = [
        'pool', 
        'token0', 
        'token1',
    ]
    fields = [    
        'timestamp',
        'token0{id}',
        'token1{id}',
        'amount0',
        'amount1',
    ]
    
    def formatresult(self, chunk, t_end=None):
        data = []
        for d in chunk:
            if t_end is not None and int(d['timestamp']) > t_end:
                break
            d_out = {}
            d_out['timestamp'] = int(d['timestamp'])
            if float(d['amount0'])<0:
                d_out['tokenBought'] = d['token0']['id']
                d_out['tokenSold'] = d['token1']['id']  
                d_out['amountBought'] = -float(d['amount0'])
                d_out['amountSold'] = float(d['amount1'])
            elif float(d['amount1'])<0:
                d_out['tokenBought'] = d['token1']['id']
                d_out['tokenSold'] = d['token0']['id']
                d_out['amountBought'] = -float(d['amount1'])
                d_out['amountSold'] = float(d['amount0'])
            data.append(d_out)
        return data
    
class TradesSushi(Trades):
    def __init__(self):
        self.url = Subgraph.Sushi.url
    
    obj = 'swaps'
    filters = [
        'pool', 
        'tokenIn', 
        'tokenOut',
    ]
    fields = [    
        'timestamp',
        'tokenIn{id, decimals}',
        'tokenOut{id, decimals}',
        'amountIn',
        'amountOut',
    ]
    
    def formatresult(self, chunk, t_end=None):
        data = []
        for d in chunk:
            if t_end is not None and int(d['timestamp']) > t_end:
                break
            d_out = {}
            d_out['timestamp'] = int(d['timestamp'])
            d_out['tokenBought'] = d['tokenOut']['id']
            d_out['tokenSold'] = d['tokenIn']['id']  
            d_out['amountBought'] = int(d['amountOut'])/10**int(d['tokenOut']['decimals'])
            d_out['amountSold'] = int(d['amountIn'])/10**int(d['tokenIn']['decimals'])
            data.append(d_out)
        return data

#Schemas/functions for pulling the coins in a pool
class PoolCoins(Schemas):
    def request(self, pooladdress):
        url = self.url 
        query = """{%s(where: {%s: "%s"}){%s}}""" % (self.obj, self.filters, pooladdress.lower(), self.fields)
        req = requests.post(url, json={"query": query})
        resp = getdictpath(req.json(), ['data', self.obj, 0])
        coinaddresses = self.formatresult(resp)
        return coinaddresses
    
class PoolCoinsCurve(PoolCoins):
    def __init__(self):
        self.url = Subgraph.Curve.url
    
    obj = 'pools'
    filters = 'address'
    fields = 'coins'
    
    def formatresult(self, resp):
        coinaddresses = resp[self.fields]
        return coinaddresses
    
class PoolCoinsUni(PoolCoins):
    def __init__(self):
        self.url = Subgraph.Uni.url
    
    obj = 'pools'
    filters = 'id'
    fields = 'token0{id} token1{id}'
    
    def formatresult(self, resp):
        coinaddresses = [resp['token0']['id'], resp['token1']['id']]
        return coinaddresses
    
class PoolCoinsSushi(PoolCoins):
    def __init__(self):
        self.url = Subgraph.Sushi.url
    
    obj = 'liquidityPools'
    filters = 'id'
    fields = 'inputTokens{id}'
    
    def formatresult(self, resp):
        resp = resp['inputTokens']
        coinaddresses = []
        for coin in resp:
            coinaddresses.append(coin['id'])
        return coinaddresses
    
    
#Subgraph aliases/URLs
class Subgraph:
    class Curve:
        aliases = ['curve', 'c']
        url = 'https://api.thegraph.com/subgraphs/name/convex-community/volume-mainnet'
         
    class Uni:
        aliases = ['uni', 'univ3', 'uniswap', 'u']
        url = 'https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3'
        
    class Sushi:
        aliases = ['sushi', 'sushiswap', 's']
        url = 'https://api.thegraph.com/subgraphs/name/messari/sushiswap-ethereum'
    
#Queries by alias
class Query:
    Trades = {
        **dict.fromkeys(Subgraph.Curve.aliases, TradesCurve),
        **dict.fromkeys(Subgraph.Uni.aliases, TradesUni),
        **dict.fromkeys(Subgraph.Sushi.aliases, TradesSushi),
    }
    
    PoolCoins = {
        **dict.fromkeys(Subgraph.Curve.aliases, PoolCoinsCurve),
        **dict.fromkeys(Subgraph.Uni.aliases, PoolCoinsUni),
        **dict.fromkeys(Subgraph.Sushi.aliases, PoolCoinsSushi),
    }
