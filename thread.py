# -*- coding: utf8 -*-
from queue import Queue
from threading import Thread, Lock
import urllib.parse
import socket
import re
import time

#用来记录已经解析过的url
done_urls = set()
#锁，用于进程的同步
lock = Lock()

#用于爬取数据,继承于线程类
class Fetcher(Thread):

    def __init__(self,task_urls):
        Thread.__init__(self)
        #待完成的url
        self.task_urls = task_urls
        #主线程结束时，所有子线程会强制结束
        self.daemon = True
        #创建后就开始运行
        self.start()

    def run(self):
        while True:
            url = self.task_urls.get()
            print(url)
            #建立与主机的链接
            sock = socket.socket()
            sock.connect(('localhost',80))
            #发出请求,要编码，转化为字节流
            get = 'GET {} HTTP/1.0\r\nHost:localhost\r\n\r\n'.format(url)
            sock.send(get.encode('ascii'))
            #获取返回的数据
            response = b''
            chunk = sock.recv(4096) #每次取4096b
            while chunk:
                response += chunk
                chunk = sock.recv(4096)
            #获取该网页下的所有链接
            links = self.parse_links(url,response)
            #进程同步
            lock.acquire()
            #links不属于done_urls的部分
            for link in links.difference(done_urls):
                self.task_urls.put(link)
            #这部分当是完成了，加入到done_urls中
            done_urls.update(links)
            #释放锁
            lock.release()
            #通知task_url队列这个任务完成了，可以进行下一个任务了(未完成任务会一直阻塞)
            self.task_urls.task_done()

    #根据fetch_url获取该页面上所有url
    def parse_links(self,fetch_url,response):
        #response为空
        if not response:
            print('errer:%s'%(fetch_url))
            return set()
        #response为非html格式也不行
        if not self._is_html(response):
            return set()
        #获取该网页下所有url
        urls = set(re.findall(r'''(?i)href=['"]?([^\s<>'"]+)''',self.body(response)))

        links = set()

        #用于除去非法的url
        for url in urls:
            #若url是相对路径，则与fetch_url合并，否则不变
            url = urllib.parse.urljoin(fetch_url,url)
            #将url的信息分段
            parts = urllib.parse.urlparse(url)
            #不是http格式
            if parts.scheme not in ('','http','https'):
                continue
            #获取主机号和端口
            host,port = urllib.parse.splitport(parts.netloc)
            #主机号不是localhost
            if host and host.lower() not in ('localhost'):
                continue
            #去掉frag部分
            defragmented,frag = urllib.parse.urldefrag(parts.path)
            links.add(defragmented)

        return links

    def _is_html(self,response):
        head,body = response.split(b'\r\n\r\n')
        headers = dict(h.split(': ') for h in head.decode().split('\r\n')[1:])
        return headers.get('Content-Type','').startswith('text/html')

    def body(self,response):
        #总之就是可以取得body部分
        body = response.split(b'\r\n\r\n',1)[1]
        return body.decode('utf-8')

class ThreahPool:
    def __init__(self,num_threads):
        self.task_urls = Queue()
        for i in range(num_threads):
            Fetcher(self.task_urls)

    def add_task(self,url):
        self.task_urls.put(url)

    def wait_completion(self):
        self.task_urls.join()

if __name__ == '__main__':
    start_time = time.time()

    pool = ThreahPool(4)

    pool.add_task('/python-doc/')
    done_urls.update(['/python-doc/'])
    pool.wait_completion()

    print('{} URLs fetched in {:.1f} seconds'.format(len(done_urls),time.time() - start_time))

'''
子线程的作用：
共享一个任务列表，每次取一个任务（url），取后任务列表阻塞，子线程开始解析，解析完毕后告诉任务列表任务完成，其他子线程可以继续取任务
解析完一个url后，得到一组url，去除已完成的部分后，分别同时加入到任务列表和已完成的列表中
'''

