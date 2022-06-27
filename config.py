import pymongo
# header相关
accept = "*/*"
accept_encoding = "gzip, deflate, br"
accept_language = "zh-CN,zh;q=0.9"
cookie = ""
referer = "https://blockchain.coinmarketcap.com/chain/bitcoin"
user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Safari/537.36"

# 参数相关
limit = 10
end = 4

# mongodb相关配置
host = '120.77.66.52'
port = 37016
db_name = 'reptile_test'
ip_proxy = 'ip_proxy'
b_blocks = "b_blocks"
b_address = "b_address"
b_inputs = "b_inputs"
b_outputs = "b_outputs"
b_trs = "b_trs"
e_blocks = "e_blocks"
e_address = "e_address"
e_inputs = "e_inputs"
e_outputs = "e_outputs"
e_trs = "e_trs"

# 文件相关
b_trs_hash = "files/b_trs_hash.txt"
b_blk_hash = "files/b_blk_hash.txt"
e_trs_hash = "files/e_trs_hash.txt"
e_blk_hash = "files/e_blk_hash.txt"
b_trs_json = "files/b_trs_json.json"
e_trs_json = "files/e_trs_json.json"


# 连接数据库
def db_connection():
    client = pymongo.MongoClient(host, port,
                                 serverSelectionTimeoutMS=5000, socketTimeoutMS=5000)
    return client


# 关闭数据库连接
def close_connection(mongo_client):
    mongo_client.close()


# 设置代理
def get_ip_proxy():
    proxies = {}
    try:
        # 连接数据库
        client = db_connection()
        reptile_test = client[db_name]
        ip_proxy_collection = reptile_test[ip_proxy]

        item = ip_proxy_collection.find_one()
        if item is not None:
            proxies = {
                'http': item['host'] + ':' + item['port'],
                'https': item['host'] + ':' + item['port']
            }
        else:
            print("数据库中没有代理ip")
    except Exception as e:
        print(f"代理ip获取失败:{e}")
    return proxies
