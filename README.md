# workit
My First Work<br>
# contents
## 项目一：更新汇率爬虫

过程：正则匹配解析并存储为json格式入库

优化：可以使用jenkins定时更新汇率

## 项目二：城市坐标匹配

过程：使用城市名,所在省,国家名,利用google map api得到城市坐标,与数据库中对应的坐标距离在1000m以内的为匹配,将城市信息和匹配结果保存到csv文件

优化：使用gevent协程处理request的阻塞

## 项目三：酒店列表爬虫

过程：抓取希尔顿和穷游酒店列表,详情

优化：使用lxml,xpath解析数据
