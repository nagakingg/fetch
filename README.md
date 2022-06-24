# fetch
Simple functions to pull price/pool data from Web3 subgraphs

# Dependencies
Python3, pandas

# Overview
Easily pull data from Web3 subgraphs. Currently supports: 

* fetching historical price data for coins/pools, and 
* identifying coins in pools 

for Curve, Uniswap3, and Sushiswap. 

Additional queries/providers can be easily added as subclasses.

# Examples
```python3
#Pull all trades for tricrypto2, return lists of price/volume for each coin pair
tricrypto2 = '0xD51a44d3FaE010294C616388b506AcdA1bfAAE46'
data = fetch.trades('curve', pools=tricrypto2)

#Pull tricrypto2 trades between April 1 and May 1, 2022
from datetime import datetime

t_start = datetime(2022,4,1).timestamp()
t_end =datetime(2022,5,1).timestamp()
data = fetch.trades('curve', pools=tricrypto2, t_start=t_start, t_end=t_end)

#Pull all trades for tricrypto2, return 6H candles for each coin pair
data = fetch.trades('curve', pools=tricrypto2, candles='6H')

#Pull all UniV3 trades between the coins in tricrypto2, return 6H candles for each pair
coinaddresses = fetch.poolcoins('curve', tricrypto2)
data = fetch.trades('uni', coins=coinaddresses, candles='6H')

#Pull all SushiSwap trades between the coins in tricrypto2, return 6H candles for each pair
data = fetch.trades('sushi', coins=coinaddresses, candles='6H')


```
