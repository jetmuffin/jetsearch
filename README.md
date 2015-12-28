# jetsearch
分布式爬虫及全文检索系统

## 预览
![preview](http://i4.tietuku.com/790d9671b3018989.png)

## 依赖
* redis
* zookeeper
* mongodb

## 安装

```
git clone git://github.com/JetMuffin/jetsearch.git
cd jetsearch
sudo pip install -r requirement.txt
```

## 爬虫使用

### master

配置`conf`文件：

```
vim jetsearch.conf
```

example:
```
[zookeeper]
zookeeper_url=127.0.0.1:2181

[redis]
redis_url=127.0.0.1:6379
spider_queue=task:spider
processor_queue=task:reprocessor
duplicate_set=set:duplicate

[mongodb]
mongodb_url=127.0.0.1:27017
storage_db=jetsearch03
page_table=tbl_page
term_table=tbl_term
```

启动`master`:

```
python ./master-start.py -c CONFIG_FILE_PATH
```

将`CONFIG_FILE_PATH`替换为你的config文件的具体路径

发布`job`:

```
python ./test-job.py
```

### slave

启动`slave`:

```
python ./slave-start.py -m MASTER_ADDR -t SLAVE_TYPE
```

将`MASTER_ADDR`替换为你的master地址，examle:`127.0.0.1:2181`

`SLAVE_TYPE`类型如下：
  * spider 爬虫slave
  * processor 处理器slave
  
## WEBUI

启动`webui`:

```
python ./webui-start.py
```

然后访问`http://MASTER_ADDR:8000`即可访问了

## 联系
初步完成基本功能，欢迎fork到自己仓库进行修改
我的邮箱：[564936642@qq.com](mailto:564936642@qq.com)

