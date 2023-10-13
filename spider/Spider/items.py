# 数据容器文件

import scrapy

class SpiderItem(scrapy.Item):
    pass

class LvyoujingdianItem(scrapy.Item):
    # 来源
    laiyuan = scrapy.Field()
    # 封面
    fengmian = scrapy.Field()
    # 标题
    biaoti = scrapy.Field()
    # 评分
    pingfen = scrapy.Field()
    # 评论数
    pinglunshu = scrapy.Field()
    # 地址
    dizhi = scrapy.Field()
    # 开放时间
    kfsj = scrapy.Field()
    # 介绍
    jieshao = scrapy.Field()
    # 优待政策
    detail = scrapy.Field()

