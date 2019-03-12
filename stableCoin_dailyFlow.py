import numpy as np
import pandas as pd
import json
from datetime import datetime
import datetime
import time
from pymongo import MongoClient
from pymongo import errors
import logging
import urllib.parse

logger = logging.getLogger('mylogger')
logger.setLevel(logging.DEBUG)

fh = logging.FileHandler('./stable_coin.log')
fh.setLevel(logging.DEBUG)

# 定义handler的输出格式
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)

# 给logger添加handler
logger.addHandler(fh)

def mongoClient(uri, db_name, collection_name):
    client = MongoClient(uri)
    db = client[db_name]
    collection = db[collection_name]
    return collection

def get_label(address):
    label_collection = mongoClient('mongodb://116.62.210.86:27017', 'eth_warning', 'eth_labels_1122')
    labels = list(label_collection.find({'address': address, 'label': 'Exchange'}))
    labels = [item for item in labels if 'name' in item]
    
    if len(labels) > 0:
        try:
            label = '|'.join(list(set([item['name'].split('_')[0].lstrip() for item in labels])))
        except:
            print(address)
    else:
        label = 'unknown'
    return label

def search_by_daily(stable_coin_name, blk_range, threshold, exchange_table):
    tmp = []
    transfer = []
    uri = 'mongodb://root:' + urllib.parse.quote('longhash123!@#QAZ') + '@47.96.228.84'
    stable_coin_collection = mongoClient(uri, 'parity', stable_coin_name)
   
    txs = stable_coin_collection.find({'blockNumber': {'$gt': blk_range[0], '$lte': blk_range[1]}})
    for tx in txs:
        tmp.append((tx['from'], tx['to'], tx['value']))
    df = pd.DataFrame(tmp, columns=['from', 'to', 'value'])
    grouped = df.groupby(['from','to'])
    
    for group in grouped:        
        transfer.append((group[0][0], group[0][1], round(group[1]['value'].sum(), 2)))
    tdf = pd.DataFrame(transfer, columns=['source', 'target', 'value'])
    tdf = tdf[tdf['value'] > threshold]
    tdf = tdf.reset_index(drop=True)
        
    # construct desired data structure
    node_info = list(set(tdf['source'].tolist()+tdf['target'].tolist()))
    nodes = [{"id": addr, "group": exchange_table.index(get_label(addr))} for addr in node_info]
    transfer_1 = [{"source":node_info.index(tdf['source'][i]), "target":node_info.index(tdf['target'][i]), "value":tdf['value'][i]} for i in tdf.index]
      
    return {"nodes": nodes, "links": transfer_1}


if __name__ == '__main__':
    
    # encode exchanges
    label_collection = mongoClient('mongodb://116.62.210.86:27017', 'eth_warning', 'eth_labels_1122')
    exchanges = list(label_collection.find({'label': 'Exchange'}))
    ex = [item['name'].split('_')[0] for item in exchanges]
    ex = [item.lstrip() for item in ex]
    ex = list(set(ex))
    ex.insert(0, 'unknown')

    stable_coins = ['GUSD', 'PAX', 'TrueUSD', 'USDC']

    dft = pd.read_csv('./eth_dailytime_table.csv')
    dft['datetime'] = pd.to_datetime(dft['datetime'])
    dft = dft.set_index('datetime')
    s = pd.Series(dft['blocknumber'], index=dft.index)

    #blk_range = [int(s['2018-12-30']), int(s['2018-12-31'])]
    blk_range = dft['blocknumber'].tolist()[-2:]
    #print(blk_range)

    uri = 'mongodb://root:' + urllib.parse.quote('longhash123!@#QAZ') + '@47.96.228.84'
    liveCharts_collection = mongoClient(uri, 'parity', 'stableCoin_dailyFlow')

    for name in stable_coins:
        logger.info(blk_range)
        net  = search_by_daily(name, blk_range, 1000, ex)
    #    print({'date': str(dft.index[-1]).split(' ')[0], 'name': name, 'data': net})

        try:
            liveCharts_collection.insert_one({'date': str(dft.index[-1]).split(' ')[0], 'name': name, 'data': net, 'blk':blk_range[-1]})
            logger.info(name)
        except Exception as e:
            logger.info(name)
            logger.error(e)
            