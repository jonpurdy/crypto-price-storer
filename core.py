#!/usr/local/bin/python3
# -*- coding: UTF-8 -*-


import json
import logging
import time
import datetime

import requests
import pymysql

import config_loader

logging.basicConfig(level="DEBUG", format='[%(asctime)s][%(levelname)s][%(name)s] %(message)s', filename="log.log")
logging.getLogger('urllib3').setLevel(logging.CRITICAL)

def main():

    # Set configuration and logging up first
    config_location = "~/.crypto-price-storer"
    config = config_loader.load_config(config_location)

    DB_IP = config.get('main', 'DB_IP')
    DB_USERNAME = config.get('main', 'DB_USERNAME')
    DB_PASSWORD = config.get('main', 'DB_PASSWORD')

    # First, currency conversion rates
    currFrom = "USD"
    currTo = ["KRW", "CAD", "MXN"]

    urlParamTo = currTo[0]
    if len(currTo) > 1:
        urlParamTo = ",".join(currTo)

    url = "http://api.fixer.io/latest?base=" + currFrom + "&symbols=" + urlParamTo
    r_fixer = requests.get(url)
    r_fixer_dict = json.loads(r_fixer.text)
    usd_krw_rate = r_fixer_dict["rates"]['KRW']
    usd_cad_rate = r_fixer_dict["rates"]['CAD']
    usd_mxn_rate = r_fixer_dict["rates"]['MXN']
    logging.debug("usd-krw %s, usd-cad %s, usd-mxn %s" % (usd_krw_rate, usd_cad_rate, usd_mxn_rate))


    # Next, let's get the prices from the exchanges
    korbit_prices_usd_dict = get_korbit_prices(usd_krw_rate)
    logging.debug("Korbit prices (USD): %s" % korbit_prices_usd_dict)

    # currently BTC, ETH, XRP
    kraken_prices_usd_dict = get_kraken_prices()
    logging.debug("Kraken prices (USD): %s" % kraken_prices_usd_dict)

    bitso_prices_usd_dict = get_bitso_prices(usd_mxn_rate)
    logging.debug("Bitso prices (USD): %s" % bitso_prices_usd_dict)

    # # korbit_prices_usd_dict = {'btc': float(15000), 'eth': float(500), 'xrp': float(0.25)}
    # # kraken_prices_usd_dict = {'btc': float(10000), 'eth': float(400), 'xrp': float(0.125)}
    # korbit_prices_usd_dict = {'name': 'korbit', 'btc': float(15000), 'eth': float(440), 'xrp': float(0.24)}
    # kraken_prices_usd_dict = {'name': 'kraken', 'btc': float(14000), 'eth': float(430), 'xrp': float(0.26)}
    # # korbit = Exchange(name='korbit')
    # # korbit.btc=float(15000)
    # # korbit.eth=float(440)df
    # # korbit.xrp=float(0.24)

    exchanges = [korbit_prices_usd_dict, kraken_prices_usd_dict, bitso_prices_usd_dict]

    
    # now, insert these prices into the mysql db

    try:
        db = pymysql.connect(DB_IP, DB_USERNAME, DB_PASSWORD, cursorclass=pymysql.cursors.DictCursor)
    except Exception as e:
        print(e)
        print("Couldn't connect to DB.")
        exit()

    for e in exchanges:
        exchange_name = e['name']
        for c in e:
            if c == 'name': pass
            else:
                query = ""
                query += "INSERT INTO crypto_historical.%s ( created, exchange, price_usd ) " % c
                query += "VALUES ( \'%s\', \'%s\', %s )" % (datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), exchange_name, e[c])
                print(query)

                cursor = db.cursor()
                cursor.execute(query)
                db.commit()

    db.close()


def get_korbit_prices(usd_krw_rate):
    # korbit
    korbit_prices_krw_dict = {}

    ## bitcoin
    try:
        r_korbit = requests.get('https://api.korbit.co.kr/v1/ticker?currency_pair=btc_krw')
        r_korbit_dict = json.loads(r_korbit.text)
        korbit_prices_krw_dict["btc"] = float(r_korbit_dict["last"])
    except:
        logging.error("Couldn't get or set Korbit ETH price.")

    time.sleep(1)

    ## ethereum
    try:
        r_korbit = requests.get('https://api.korbit.co.kr/v1/ticker?currency_pair=eth_krw')
        r_korbit_dict = json.loads(r_korbit.text)
        korbit_prices_krw_dict["eth"] = float(r_korbit_dict["last"])
        #logging.debug(r_korbit.text)
    except:
        logging.error("Couldn't get or set Korbit ETH price.")

    time.sleep(1)

    ## ripple
    try:
        r_korbit = requests.get('https://api.korbit.co.kr/v1/ticker?currency_pair=xrp_krw')
        r_korbit_dict = json.loads(r_korbit.text)
        korbit_prices_krw_dict["xrp"] = float(r_korbit_dict["last"])
    except:
        logging.error("Couldn't get or set Korbit XRP price.")

    logging.debug("Korbit prices (KRW): %s" % korbit_prices_krw_dict)

    korbit_prices_usd_dict = {}
    for cryptocurrency in korbit_prices_krw_dict:
        # price equals korbit_prices_krw_dict[item]
        #print(korbit_prices_krw_dict[cryptocurrency])
        korbit_prices_usd_dict[cryptocurrency] = float(korbit_prices_krw_dict[cryptocurrency])/float(usd_krw_rate)

    korbit_prices_usd_dict['name'] = "korbit"
    return korbit_prices_usd_dict


def get_kraken_prices():
    ''' Retrieves the prices of the currency pairs specified. Returns
        a dict.
    '''

    kraken_prices_usd_dict = {}
    try:
        r_kraken = requests.get('https://api.kraken.com/0/public/Ticker?pair=XBTUSD')
        r_kraken_dict = json.loads(r_kraken.text)
        #print(r_kraken_dict['result']['XETHZUSD']['c'][0])
        kraken_prices_usd_dict["btc"] = float(r_kraken_dict['result']['XXBTZUSD']['c'][0])
    except:
        logging.error("Couldn't get or set Kraken BTC price.")

    time.sleep(1)

    try:
        r_kraken = requests.get('https://api.kraken.com/0/public/Ticker?pair=ETHUSD')
        r_kraken_dict = json.loads(r_kraken.text)
        #print(r_kraken_dict['result']['XETHZUSD']['c'][0])
        kraken_prices_usd_dict['eth'] = float(r_kraken_dict['result']['XETHZUSD']['c'][0])
    except:
        logging.error("Couldn't get or set Kraken ETH price.")

    time.sleep(1)

    try:
        r_kraken = requests.get('https://api.kraken.com/0/public/Ticker?pair=XRPUSD')
        r_kraken_dict = json.loads(r_kraken.text)
        #print(r_kraken_dict['result']['XETHZUSD']['c'][0])
        kraken_prices_usd_dict["xrp"] = float(r_kraken_dict['result']['XXRPZUSD']['c'][0])
    except:
        logging.error("Couldn't get or set Kraken XRP price.")

    kraken_prices_usd_dict['name'] = "kraken"
    return kraken_prices_usd_dict

def get_bitso_prices(usd_mx_rate):
    # korbit
    bitso_prices_mx_dict = {}

    ## bitcoin
    try:
        r = requests.get('https://api.bitso.com/v3/ticker/?book=btc_mxn')
        r_dict = json.loads(r.text)
        bitso_prices_mx_dict["btc"] = float(r_dict["payload"]["last"])
    except:
        logging.error("Couldn't get or set Bitso BTC price.")

    time.sleep(1)

    ## eth
    try:
        r = requests.get('https://api.bitso.com/v3/ticker/?book=eth_mxn')
        r_dict = json.loads(r.text)
        bitso_prices_mx_dict["eth"] = float(r_dict["payload"]["last"])
    except:
        logging.error("Couldn't get or set Bitso ETH price.")

    time.sleep(1)


    logging.debug("Bitso prices (MXN): %s" % bitso_prices_mx_dict)

    biso_prices_usd_dict = {}
    for cryptocurrency in bitso_prices_mx_dict:
        # price equals korbit_prices_krw_dict[item]
        #print(korbit_prices_krw_dict[cryptocurrency])
        biso_prices_usd_dict[cryptocurrency] = float(bitso_prices_mx_dict[cryptocurrency])/float(usd_mx_rate)

    biso_prices_usd_dict['name'] = "bitso"
    return biso_prices_usd_dict

class Exchange(object):
    """ Exchange object.
    """

    def __init__(self, name):
        self.name = name


if __name__ == '__main__':
    main()