import threading

import config
from reptile import bitcoin_reptile
from reptile import ethereum_reptile


def bitcoin_crawl(s, proxies):
    s.acquire()
    try:
        bitcoin_reptile.reptile(proxies)
    except Exception as e:
        print(f"在爬取bitcoin信息过程中出错：{e}")
    s.release()
    # 循环定时执行
    global tb
    tb = threading.Timer(300, bitcoin_crawl, (s, proxies,))
    tb.start()


def ethereum_crawl(s, proxies):
    s.acquire()
    try:
        ethereum_reptile.reptile(proxies)
    except Exception as e:
        print(f"在爬取ethereum信息过程中出错：{e}")
    s.release()
    # 循环定时执行
    global te
    te = threading.Timer(120, ethereum_crawl, (s, proxies,))
    te.start()


if __name__ == "__main__":
    # 设定信号量
    sem = threading.Semaphore(1)
    # 设置代理
    proxies = {}
    # proxies = config.get_ip_proxy()
    # 开爬
    bitcoin_crawl(sem, proxies)
    ethereum_crawl(sem, proxies)


