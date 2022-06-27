import time

from pybloom_live import ScalableBloomFilter
import requests
import pymongo
import json
import config


# 连接数据库
def db_connection():
    client = pymongo.MongoClient(config.host, config.port,
                                 serverSelectionTimeoutMS=600000, socketTimeoutMS=600000)
    return client


# 关闭数据库连接
def close_connection(mongo_client):
    mongo_client.close()


# 配置trs请求头
def get_headers():
    headers = {
        "accept": config.accept,
        "accept-encoding": config.accept_encoding,
        "accept-language": config.accept_language,
        "Cookie": config.cookie,
        "referer": config.referer,
        "User-Agent": config.user_agent
    }
    return headers


# 配置trs参数
def get_trs_params(i):  # 参数为从哪一页开始爬取
    params = {
        "symbol": "BTC",
        "start": i,  # 从哪一页开始
        "limit": config.limit,  # 一次爬出多少
    }
    return params


# 配置block参数
def get_block_params(i, h=None):  # 参数为从哪一页开始爬取
    params = {
        "symbol": "BTC",
        "start": i,  # 从哪一页开始
        "limit": config.limit,  # 一次爬出多少
        "block_height": h,
    }
    return params


# 配置address参数
def get_address_params(i, address):  # 参数为从哪一页开始爬取
    params = {
        "address": address,
        "symbol": "BTC",
        "start": i,  # 从哪一页开始
        "limit": config.limit,  # 一次爬出多少
    }
    return params


# 设置布隆过滤器
def get_trs_bloom_filter():
    # 设置可自动扩容的布隆过滤器
    bloom = ScalableBloomFilter(initial_capacity=200, error_rate=0.001)
    # 锻炼过滤器
    with open(config.b_trs_hash, "r") as b_hashid:
        line = b_hashid.readline()
        while line:
            # 开始锻炼
            bloom.add(line.strip())  # 为了防止有换行出现要用strip
            # print(line.strip())
            line = b_hashid.readline()
    return bloom


def get_blk_bloom_filter():
    # 设置可自动扩容的布隆过滤器
    bloom = ScalableBloomFilter(initial_capacity=200, error_rate=0.001)
    # 锻炼过滤器
    with open(config.b_blk_hash, "r") as b_hashid:
        line = b_hashid.readline()
        while line:
            # 开始锻炼
            bloom.add(line.strip())  # 为了防止有换行出现要用strip
            # print(line.strip())
            line = b_hashid.readline()
    return bloom


# 开爬
def reptile(proxies):
    # 设置请求
    url1 = "https://blockchain.coinmarketcap.com/api/transactions"
    url2 = "https://blockchain.coinmarketcap.com/api/address"
    url3 = "https://blockchain.coinmarketcap.com/api/block"
    headers = get_headers()
    # 设置布隆过滤器
    trs_bloom = get_trs_bloom_filter()
    blk_bloom = get_blk_bloom_filter()

    try:
        # 开始爬取
        i = 1
        while i < config.end:
            # 设置容器
            trs_result = []
            inputs_result = []
            outputs_result = []
            address_result = []
            blocks_result = []

            requests.packages.urllib3.disable_warnings()
            resp = requests.get(url1, headers=headers, params=get_trs_params(i),
                                timeout=180, proxies=proxies, verify=False)
            # 接受json数据
            msg = resp.json()
            # json存入文件
            '''
            data = json.dumps(msg, indent=1)
            with open(config.b_trs_json, "a", newline="\n") as f:
                f.write(data)
            '''
            # 开始解析
            if "data" in msg:
                for item in msg["data"]:
                    # 使用布隆过滤器
                    if trs_bloom.add(item["hash"]):
                        print("该条trs已经爬过")
                        continue
                    # 写入文件
                    with open(config.b_trs_hash, "a", encoding='utf-8') as f:
                        f.write(item['hash'] + '\n')

                    address_list = []
                    input_list = []
                    output_list = []
                    if "inputs" in item:
                        for _ in item["inputs"]:
                            input_list.append({"address_hash": _["address"]})
                            address_list.append(_["address"])
                    else:
                        print("input_list为空")
                    if "outputs" in item:
                        for _ in item["outputs"]:
                            input_list.append({"address_hash": _["address"]})
                            address_list.append(_["address"])
                    else:
                        print("output_list为空")

                    try:
                        # 开始解析transaction
                        one_trs = {
                            "trx_hash": item["hash"],
                            "trx_timestamp": item["timestamp"],
                            "trx_method": "",
                            "trx_in_block": {"block_hash": item["block_hash"], "block_height": item["block_height"]},
                            "input_count": len(item["inputs"]),
                            "output_count": len(item["outputs"]),
                            "lock_time_timestamp": "",
                            "input_value": item["input_value"],
                            "output_value": item["output_value"],
                            "fee": item["fee"],
                            "version": "",
                            "from_address": input_list,
                            "to_address": output_list,
                            "confirmations": item["confirmations"],
                            "is_pending": item["is_pending"],
                            "quote": item["quote"],
                            "inputs": item["inputs"],
                            "outputs": item["outputs"],
                            "size": item["size"]
                        }
                        trs_result.append(one_trs)
                        print("---------一条b_trs已解析---------")
                    except Exception as e:
                        print("---------一条b_trs解析失败---------")

                    # 开始解析address
                    for address in address_list:
                        # 此处可加布隆过滤器
                        time.sleep(3)
                        # 开始爬取
                        requests.packages.urllib3.disable_warnings()
                        resp2 = requests.get(url2, headers=headers, params=get_address_params(1, address),
                                             timeout=180, proxies=proxies, verify=False)
                        # 接受json数据
                        msg2 = resp2.json()
                        try:
                            # 开始解析
                            if msg2 is not None:
                                balance = ""
                                if "balance" in msg2:
                                    balance = msg2["balance"]

                                one_address = {
                                    "address_hash": address,
                                    "transactions_hash": "", "contract_hash": "", "token_coin": "",
                                    "balance": balance,
                                    "address_data_source": "", "address_tag": "", "ens": "",
                                    "social_media_url": "", "address_block": "", "address_data_product_by": "",
                                    "coin": msg2["coin"],
                                    "transaction_count": msg2["transaction_count"],
                                    "received_count": msg2["received_count"],
                                    "sent_count": msg2["sent_count"],
                                    "amount_received": msg2["amount_received"],
                                    "amount_sent": msg2["amount_sent"],
                                    "quote": msg2["quote"],
                                    "txs": msg2["txs"]
                                }
                                address_result.append(one_address)
                                print("---------一条b_address已解析---------")
                            else:
                                print("爬取address出错")
                        except Exception as e:
                            print("---------一条b_address解析出错---------")
                            continue

                    # 开始解析block
                    requests.packages.urllib3.disable_warnings()
                    resp3 = requests.get(url3, headers=headers,
                                         params=get_block_params(1, item["block_height"]),
                                         timeout=180, proxies=proxies, verify=False)
                    msg3 = resp3.json()
                    try:
                        if msg3 is not None and blk_bloom.add(msg3["hash"]):
                            print("该条block已经爬过,跳过")
                        else:
                            if msg3 is not None:
                                with open(config.b_blk_hash, "a", encoding='utf-8') as f:
                                    f.write(msg3["hash"] + "\n")

                                trx_hash_list = []
                                for t in msg3["txs"]:
                                    trx_hash_list.append({"trx_hash": t["hash"]})

                                one_block = {
                                    "block_hash": str(msg3["hash"]),
                                    "block_size": str(msg3["size"]),
                                    "stripped_size": "",
                                    "number": str(msg3["height"]),
                                    "version": "",
                                    "timestamp": msg3["timestamp"],
                                    "nonce": "", "bits": "",
                                    "trx_hash_list": trx_hash_list,
                                    "transaction_count": msg3["transaction_count"],
                                    "gasUsed": "",
                                    "miner_address": str(msg3["miner"]),
                                    "amount_transacted": msg3["amount_transacted"],
                                    "block_reward": msg3["block_reward"],
                                    "confirmations": msg3["confirmations"],
                                    "difficulty": msg3["difficulty"],
                                    "quote": msg3["quote"],
                                    "txs": msg3["txs"]
                                }
                                blocks_result.append(one_block)
                                print("---------一条b_block已解析---------")
                            else:
                                print("爬取block出错")
                    except Exception as e:
                        print("---------一条b_block解析出错---------")

                    try:
                        # 开始解析inputs
                        one_input = {
                            "trx_hash": item["hash"],
                            "block_hash": item["block_hash"],
                            "block_number": item["block_height"],
                            "block_timestamp": msg3["timestamp"],
                            "input_address": input_list,
                            "type_input_address": "",
                            "input_value": item["input_value"],
                            "required_signatures": ""
                        }
                        inputs_result.append(one_input)
                        print("---------一条b_input已解析---------")
                    except Exception as e:
                        print("---------一条b_input解析失败---------")

                    try:
                        # 开始解析outputs
                        one_output = {
                            "trx_hash": item["hash"],
                            "block_hash": item["block_hash"],
                            "block_number": item["block_height"],
                            "block_timestamp": msg3["timestamp"],
                            "output_address": output_list,
                            "type_output_address": "",
                            "output_value": item["output_value"],
                            "required_signatures": ""
                        }
                        outputs_result.append(one_output)
                        print("---------一条b_output已解析---------")
                    except Exception as e:
                        print("---------一条b_output解析失败---------")

                    print("一条完整bitcoin链接的解析完成！")
                    print("########################################")
            else:
                print(f"在爬取第{i}页中bitcoin信息出错")

            try:
                # 连接数据库
                client = db_connection()
                reptile_test = client[config.db_name]
                # 把trs存入数据库
                if len(trs_result) != 0:
                    b_trs = reptile_test[config.b_trs]
                    b_trs.insert_many(trs_result)
                    print("b_trs已经存入数据库")
                else:
                    print("没有获取到b_trs")
                # 把block存入数据库
                if len(blocks_result) != 0:
                    b_blocks = reptile_test[config.b_blocks]
                    b_blocks.insert_many(blocks_result)
                    print("b_bloks已经存入数据库")
                else:
                    print("没有获取到blocks")
                # 把address存入数据库
                if len(address_result) != 0:
                    b_address = reptile_test[config.b_address]
                    b_address.insert_many(address_result)
                    print("b_address已经存入数据库")
                else:
                    print("没有获取到address")
                # 把inputs存入数据库
                if len(inputs_result) != 0:
                    b_inputs = reptile_test[config.b_inputs]
                    b_inputs.insert_many(inputs_result)
                    print("b_inputs已经存入数据库")
                else:
                    print("没有获取到b_inputs")
                # 把outputs存入数据库
                if len(outputs_result) != 0:
                    b_outputs = reptile_test[config.b_outputs]
                    b_outputs.insert_many(outputs_result)
                    print("b_outputs已经存入数据库")
                else:
                    print("没有获取到b_outputs")
                # 关闭数据库
                close_connection(client)
                print(f"※第{i}页的10条bitcoin_trs解析完成！※")
                print("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
            except Exception as e:
                print(f"存入数据库出错：{e}")
            i = i + 1
    except Exception as e:
        print(f"爬取bitcoin出错：{e}")
