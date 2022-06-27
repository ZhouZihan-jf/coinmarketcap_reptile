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
        "symbol": "ETH",
        "start": i,  # 从哪一页开始
        "limit": config.limit,  # 一次爬出多少
    }
    return params


# 配置block参数
def get_block_params(i, h=None):  # 参数为从哪一页开始爬取
    params = {
        "symbol": "ETH",
        "start": i,  # 从哪一页开始
        "limit": config.limit,  # 一次爬出多少
        "block_height": h,
    }
    return params


# 配置address参数
def get_address_params(i, address):  # 参数为从哪一页开始爬取
    params = {
        "address": address,
        "symbol": "ETH",
        "start": i,  # 从哪一页开始
        "limit": config.limit,  # 一次爬出多少
    }
    return params


# 设置布隆过滤器
def get_trs_bloom_filter():
    # 设置可自动扩容的布隆过滤器
    bloom = ScalableBloomFilter(initial_capacity=200, error_rate=0.001)
    # 锻炼过滤器
    with open(config.e_trs_hash, "r") as b_hashid:
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
    with open(config.e_blk_hash, "r") as b_hashid:
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
        i = 1
        while i < config.end:
            # 设置容器
            trs_result = []
            inputs_result = []
            outputs_result = []
            address_result = []
            blocks_result = []
            # 开始爬取
            requests.packages.urllib3.disable_warnings()
            resp = requests.get(url1, headers=headers, params=get_trs_params(i),
                                timeout=180, proxies=proxies, verify=False)
            # 接受json数据
            msg = resp.json()
            # 存入json文件
            '''
            data = json.dumps(msg, indent=1)
            with open(config.e_trs_json, "a", newline="\n") as f:
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
                    with open(config.e_trs_hash, "a", encoding='utf-8') as f:
                        f.write(item['hash'] + '\n')

                    chain_id = None
                    if "chain_id" in item:
                        chain_id = item["chain_id"]

                    try:
                        # 开始解析transaction
                        one_trs = {
                            "trx_hash": item["hash"],
                            "trx_timestamp": item["timestamp"],
                            "trx_method": "",
                            "trx_in_block": {"block_hash": item["block_hash"]},
                            "input_count": "", "output_count": "", "lock_time_timestamp": "", "input_value": "",
                            "output_value": "", "fee": "", "version": "",
                            "from_address": item["from"],
                            "to_address": item["to"],
                            "block_height": item["block_height"],
                            "chain_id": chain_id,  # item["chain_id"]
                            "coin": item["coin"],
                            "confirmations": item["confirmations"],
                            "contract_created": item["contract_created"],
                            "gas": item["gas"],
                            "gas_price": item["gas_price"],
                            "gas_used": item["gas_used"],
                            "input": item["input"],
                            "is_contract": item["is_contract"],
                            "nonce": item["nonce"],
                            "quote": item["quote"],
                            "status": item["status"],
                            "transaction_index": item["transaction_index"],
                            "value": item["value"]
                        }
                        trs_result.append(one_trs)
                        print("---------一条e_trs已解析---------")
                    except Exception as e:
                        print("---------一条e_trs解析失败---------")

                    # 开始解析address
                    address_list = [item["from"], item["to"]]
                    for address in address_list:
                        # 此处可加布隆过滤器
                        time.sleep(3)
                        # 开始爬取
                        requests.packages.urllib3.disable_warnings()
                        resp2 = requests.get(url2, headers=headers,
                                             params=get_address_params(1, address),
                                             timeout=180, proxies=proxies, verify=False)
                        # 接受json数据
                        msg2 = resp2.json()
                        try:
                            # 开始解析
                            if msg2 is not None:
                                transactions_hash = []
                                if "txs" in msg2:
                                    for t in msg2["txs"]:
                                        transactions_hash.append(t["hash"])
                                balance = ""
                                if "balance" in msg2:
                                    balance = msg2["balance"]

                                one_address = {
                                    "address_hash": address,
                                    "transactions_hash": transactions_hash,
                                    "contract_hash": "", "token_coin": "",
                                    "balance": balance,
                                    "address_data_source": "", "address_tag": "", "ens": "", "social_media_url": "",
                                    "address_block": "", "address_data_product_by": "",
                                    "coin": item["coin"],  # 这里要是用msg2会出错
                                    "transaction_count": msg2["transaction_count"],
                                    "received_count": "",
                                    "contract_data": msg2["contract_data"],
                                    "is_contract": msg2["is_contract"],
                                    "is_erc20_contract": msg2["is_erc20_contract"],
                                    "quote": msg2["quote"]
                                }
                                address_result.append(one_address)
                                print("---------一条e_address已解析---------")
                            else:
                                print("爬取address出错")
                        except Exception as e:
                            print("---------一条e_address解析出错---------")
                            continue

                    # 开始解析block
                    requests.packages.urllib3.disable_warnings()
                    resp3 = requests.get(url3, headers=headers,
                                         params=get_block_params(1, item["block_height"]),
                                         timeout=180, proxies=proxies, verify=False)
                    msg3 = resp3.json()
                    try:
                        if msg3 is not None and blk_bloom.add(msg3["hash"]):
                            print("该条block已经爬过，跳过")
                        else:
                            if msg3 is not None:

                                with open(config.e_blk_hash, "a", encoding='utf-8') as f:
                                    f.write(msg3["hash"] + "\n")

                                trx_hash_list = []
                                if "txs" in msg3:
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
                                    "quote": msg3["quote"]
                                }
                                blocks_result.append(one_block)
                                print("---------一条e_block已解析---------")
                            else:
                                print("爬取block出错")
                    except Exception as e:
                        print("---------一条e_block解析出错---------")

                    try:
                        # 开始解析inputs
                        one_input = {
                            "trx_hash": item["hash"],
                            "block_hash": item["block_hash"],
                            "block_number": item["block_height"],
                            "block_timestamp": msg3["timestamp"],
                            "input_address": item["from"],
                            "type_input_address": "",
                            "input_value": "",
                            "required_signatures": ""
                        }
                        inputs_result.append(one_input)
                        print("---------一条e_input已解析---------")
                    except Exception as e:
                        print("---------一条e_input解析失败---------")

                    try:
                        # 开始解析outputs
                        one_output = {
                            "trx_hash": item["hash"],
                            "block_hash": item["block_hash"],
                            "block_number": item["block_height"],
                            "block_timestamp": msg3["timestamp"],
                            "output_address": item["to"],
                            "type_output_address": "",
                            "output_value": "",
                            "required_signatures": ""
                        }
                        outputs_result.append(one_output)
                        print("---------一条e_output已解析---------")
                    except Exception as e:
                        print("---------一条e_output解析失败---------")

                    print("一条完整ethereum链接的解析完成！")
                    print("########################################")
            else:
                print(f"在爬取第{i}页中ethereum信息出错")
            try:
                # 连接数据库
                client = db_connection()
                reptile_test = client[config.db_name]
                # 把trs存入数据库
                if len(trs_result) != 0:
                    e_trs = reptile_test[config.e_trs]
                    e_trs.insert_many(trs_result)
                    print("e_trs已经存入数据库")
                else:
                    print("没有获取到e_trs")
                # 把block存入数据库
                if len(blocks_result) != 0:
                    e_blocks = reptile_test[config.e_blocks]
                    e_blocks.insert_many(blocks_result)
                    print("e_blocks已经存入数据库")
                else:
                    print("没有获取到e_blocks")
                # 把address存入数据库
                if len(address_result) != 0:
                    e_address = reptile_test[config.e_address]
                    e_address.insert_many(address_result)
                    print("e_address已经存入数据库")
                else:
                    print("没有获取到e_address")
                # 把inputs存入数据库
                if len(inputs_result) != 0:
                    e_inputs = reptile_test[config.e_inputs]
                    e_inputs.insert_many(inputs_result)
                    print("e_inputs已经存入数据库")
                else:
                    print("没有获取到e_inputs")
                # 把outputs存入数据库
                if len(outputs_result) != 0:
                    e_outputs = reptile_test[config.e_outputs]
                    e_outputs.insert_many(outputs_result)
                    print("e_outputs已经存入数据库")
                else:
                    print("没有获取到e_outputs")
                # 关闭数据库
                close_connection(client)
                print(f"※第{i}页的10条ethereum_trs解析完成！※")
                print("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
            except Exception as e:
                print(f"存入数据库出错：{e}")
            i = i+1
    except Exception as e:
        print(f"爬取ethereum出错：{e}")


