# 数据爬取文件

import scrapy
import pymysql
import pymssql
from ..items import LvyoujingdianItem
import time
import re
import random
import platform
import json
import os
from urllib.parse import urlparse
import requests
import emoji

# 旅游景点
class LvyoujingdianSpider(scrapy.Spider):
    name = 'lvyoujingdianSpider'
    spiderUrl = 'https://m.ctrip.com/restapi/soa2/20591/getGsOnlineResult?_fxpcqlniredt=09031015416227214258&x-traceID=09031015416227214258-1676909638574-6356069'
    start_urls = spiderUrl.split(";")
    protocol = ''
    hostname = ''

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def start_requests(self):

        plat = platform.system().lower()
        if plat == 'linux' or plat == 'windows':
            connect = self.db_connect()
            cursor = connect.cursor()
            if self.table_exists(cursor, 'rv7qb_lvyoujingdian') == 1:
                cursor.close()
                connect.close()
                self.temp_data()
                return

        body = {
            "keyword": "宁夏",
            "pageIndex": 1,
            "pageSize": 12,
            "tab": "sight",
            "sourceFrom": "",
            "profile": False,
            "head": {
                "cid": "09031015416227214258",
                "ctok": "",
                "cver": "1.0",
                "lang": "01",
                "sid": "8888",
                "syscode": "09",
                "auth": "",
                "xsid": "",
                "extension": []
            }
        }

        for url in self.start_urls:
            yield scrapy.Request(
                method="POST",
                url=url,
                body=json.dumps(body),
                callback=self.parse,
                headers={
                    "Content-Type": "application/json"
                }
            )

    # 列表解析
    def parse(self, response):
        
        _url = urlparse(self.spiderUrl)
        self.protocol = _url.scheme
        self.hostname = _url.netloc
        plat = platform.system().lower()
        if plat == 'windows_bak':
            pass
        elif plat == 'linux' or plat == 'windows':
            connect = self.db_connect()
            cursor = connect.cursor()
            if self.table_exists(cursor, 'rv7qb_lvyoujingdian') == 1:
                cursor.close()
                connect.close()
                self.temp_data()
                return

        data = json.loads(response.body)
        list = data["items"]
        
        for item in list:

            fields = LvyoujingdianItem()
            fields["laiyuan"] = item["url"]
            fields["fengmian"] = item["imageUrl"]
            fields["biaoti"] = item["word"]
            fields["pingfen"] = item["commentScore"]
            fields["pinglunshu"] = item["commentCount"]

            detailUrlRule = item["url"]

            if detailUrlRule.startswith('http') or self.hostname in detailUrlRule:
                pass
            else:
                detailUrlRule = self.protocol + '://' + self.hostname + detailUrlRule
                fields["laiyuan"] = detailUrlRule

            yield scrapy.Request(url=detailUrlRule, meta={'fields': fields}, callback=self.detail_parse)

    # 详情解析
    def detail_parse(self, response):
        fields = response.meta['fields']

        try:
            fields["dizhi"] = self.remove_html(response.css('''div.baseInfoContent div.baseInfoItem:nth-child(1) p.baseInfoText::text''').extract_first())
        except:
            pass

        try:
            fields["kfsj"] = self.remove_html(response.css('''div.baseInfoContent div.baseInfoItem:nth-child(2) p[class="baseInfoText cursor openTimeText"]''').extract_first())
        except:
            pass

        try:
            fields["jieshao"] = self.remove_html(response.css('''div.detailModuleRef div[class="detailModule normalModule"] div.moduleContent:nth-child(1)''').extract_first())
        except:
            pass

        try:
            fields["detail"] = emoji.demojize(response.css('''div.detailModuleRef div[class="detailModule normalModule"] div:nth-child(6)''').extract_first())
        except:
            pass

        return fields

    # 去除多余html标签
    def remove_html(self, html):
        if html == None:
            return ''
        pattern = re.compile(r'<[^>]+>', re.S)
        return pattern.sub('', html).strip()

    # 数据库连接
    def db_connect(self):
        type = self.settings.get('TYPE', 'mysql')
        host = self.settings.get('HOST', 'localhost')
        port = int(self.settings.get('PORT', 3306))
        user = self.settings.get('USER', 'root')
        password = self.settings.get('PASSWORD', '123456')

        try:
            database = self.databaseName
        except:
            database = self.settings.get('DATABASE', '')

        if type == 'mysql':
            connect = pymysql.connect(host=host, port=port, db=database, user=user, passwd=password, charset='utf8')
        else:
            connect = pymssql.connect(host=host, user=user, password=password, database=database)

        return connect

    # 断表是否存在
    def table_exists(self, cursor, table_name):
        cursor.execute("show tables;")
        tables = [cursor.fetchall()]
        table_list = re.findall('(\'.*?\')',str(tables))
        table_list = [re.sub("'",'',each) for each in table_list]

        if table_name in table_list:
            return 1
        else:
            return 0

    # 数据缓存源
    def temp_data(self):

        connect = self.db_connect()
        cursor = connect.cursor()
        sql = '''
            insert into lvyoujingdian(
                laiyuan
                ,fengmian
                ,biaoti
                ,pingfen
                ,pinglunshu
                ,dizhi
                ,kfsj
                ,jieshao
                ,detail
            )
            select
                laiyuan
                ,fengmian
                ,biaoti
                ,pingfen
                ,pinglunshu
                ,dizhi
                ,kfsj
                ,jieshao
                ,detail
            from rv7qb_lvyoujingdian
            where(not exists (select
                laiyuan
                ,fengmian
                ,biaoti
                ,pingfen
                ,pinglunshu
                ,dizhi
                ,kfsj
                ,jieshao
                ,detail
            from lvyoujingdian where
             lvyoujingdian.laiyuan=rv7qb_lvyoujingdian.laiyuan
            and lvyoujingdian.fengmian=rv7qb_lvyoujingdian.fengmian
            and lvyoujingdian.biaoti=rv7qb_lvyoujingdian.biaoti
            and lvyoujingdian.pingfen=rv7qb_lvyoujingdian.pingfen
            and lvyoujingdian.pinglunshu=rv7qb_lvyoujingdian.pinglunshu
            and lvyoujingdian.dizhi=rv7qb_lvyoujingdian.dizhi
            and lvyoujingdian.kfsj=rv7qb_lvyoujingdian.kfsj
            and lvyoujingdian.jieshao=rv7qb_lvyoujingdian.jieshao
            and lvyoujingdian.detail=rv7qb_lvyoujingdian.detail
            ))
            limit {0}
        '''.format(random.randint(20,30))

        cursor.execute(sql)
        connect.commit()

        connect.close()
