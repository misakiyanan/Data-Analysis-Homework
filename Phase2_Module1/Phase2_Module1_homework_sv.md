

## 一. 建表

某公司地区业务有两张门店相关的表 (数据见:homework.xlsx)

<u>一张为门店信息表</u>
表名:area_table
字段内容 字段名 字段类型 长度
店铺id store_id varchar 10
店铺位置 area varchar 20
店长id leader_id int

<u>第二张为门店销售信息表</u> 
表名:store_table
字段内容 字段名 字段类型 长度 
店铺id store_id varchar 10 
订单id order_id varchar 12 
销量 sales_volume int 
销售日期 salesdate date

### 1.建表:(20分) 为以上两张表建立Mysql表格，并导入数据

```sql
-- create database
create database homework_20210707;
use homework_20210707;

-- create table [area_table]
create table area_table (
store_id varchar(10), 
area varchar(20),
leader_id int);

-- create table [store_table]
create table store_table (
store_id varchar(10),
order_id varchar(12),
sales_volume int,
salesdate date);

```

```sql
-- insert values [Method1: using mysql command]
-- [Assumption: worksheets in the provided .xlsx file are exported into .csv files]
-- ref. https://chartio.com/resources/tutorials/excel-to-mysql/
-- ref. https://stackoverflow.com/questions/3635166/how-do-i-import-csv-file-into-a-mysql-table

LOAD DATA LOCAL INFILE "/Users/xieyanan/Desktop/data/area.csv" INTO TABLE homework_20210707.area_table
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(store_id, area, leader_id);

```

```python
# insert values [Method2: using 'pymysql' package of python]
# ref.https://www.w3schools.com/python/python_mysql_insert.asp
# ref.https://blog.csdn.net/HAH_HAH/article/details/104042354

import pandas as pd
import pymysql

# area data
area = pd.read_csv('area.csv')
area_records = area.to_records(index=False).tolist()


# store data
store = pd.read_csv('store.csv')
def date_formatting(date_str):  # format date
    d, m, y = date_str.split('/')[0],  date_str.split('/')[1],  date_str.split('/')[2]
    if len(d) == 1:
        d = "0" + d
    if len(m) == 1:
        m = "0" + m
    formatted = '20'+y+'-'+m+'-'+d
    return formatted
store['salesdate'] = store['salesdate'].apply(date_formatting)  

store_records = store.to_records(index=False).tolist()
store_records  # with datetime format revised


# connect to mysql server
connect = pymysql.connect(host='localhost', 
                          user='root', 
                          passwd='Chouchou186', 
                          database='homework_20210707')
mycursor = connect.cursor()

# insert multiple rows
for row in area_records:
    sql = 'INSERT INTO area_table (store_id, area, leader_id) VALUES ("{0}", "{1}", {2})'.format(row[0], row[1], row[2])
    print(sql)
    mycursor.execute(sql)
connect.commit()

for row in store_records:
    sql = 'INSERT INTO store_table (store_id, order_id, sales_volume, salesdate) VALUES ("{0}", "{1}", {2}, "{3}")'.format(row[0], row[1], row[2], row[3])
    print(sql)
    mycursor.execute(sql)

connect.commit()

# close connection
mycursor.close()
connect.close()

```


check data:

```txt
mysql> select * from area_table;
+----------+-------+-----------+
| store_id | area  | leader_id |
+----------+-------+-----------+
| store1   | alpha |    139844 |
| store2   | alpha |     44311 |
| store3   | alpha |    951837 |
| store4   | alpha |    298987 |
| store5   | beta  |    774548 |
| store6   | beta  |    974663 |
| store7   | beta  |    550242 |
| store8   | beta  |    735906 |
| store9   | beta  |    813931 |
| store10  | beta  |    269966 |
| store11  | beta  |    442066 |
| store12  | gamma |     52404 |
| store13  | gamma |    250697 |
| store14  | gamma |    733530 |
| store15  | gamma |    356103 |
| store16  | gamma |    770287 |
+----------+-------+-----------+
16 rows in set (0.00 sec)


mysql> select * from store_table;
+----------+----------+--------------+------------+
| store_id | order_id | sales_volume | salesdate  |
+----------+----------+--------------+------------+
| store8   | 89948724 |          499 | 2021-06-25 |
| store1   | 89948725 |          617 | 2021-06-25 |
| store9   | 89948726 |          569 | 2021-06-25 |
| store3   | 89948727 |          579 | 2021-06-25 |
| store1   | 89948728 |           76 | 2021-06-25 |
| store7   | 89948729 |          228 | 2021-06-25 |
| store2   | 89948730 |          557 | 2021-06-26 |
| store6   | 89948731 |          177 | 2021-06-26 |
| store10  | 89948732 |           18 | 2021-06-26 |
| store7   | 89948733 |          309 | 2021-06-26 |
| store1   | 89948734 |          984 | 2021-06-26 |
| store1   | 89948735 |          999 | 2021-06-26 |
| store1   | 89948736 |          163 | 2021-06-26 |
| store1   | 89948737 |          206 | 2021-06-26 |
| store5   | 89948738 |          977 | 2021-06-26 |
| store6   | 89948739 |          884 | 2021-06-26 |
| store1   | 89948740 |          185 | 2021-06-26 |
| store2   | 89948741 |           37 | 2021-06-27 |
| store3   | 89948742 |          565 | 2021-06-27 |
| store3   | 89948743 |          295 | 2021-06-27 |
| store6   | 89948744 |          985 | 2021-06-27 |
| store8   | 89948745 |          874 | 2021-06-27 |
| store4   | 89948746 |          837 | 2021-06-27 |
| store2   | 89948747 |          754 | 2021-06-27 |
| store2   | 89948748 |          167 | 2021-06-28 |
| store9   | 89948749 |           81 | 2021-06-28 |
| store1   | 89948750 |          648 | 2021-06-28 |
| store4   | 89948751 |          651 | 2021-06-28 |
| store10  | 89948752 |          616 | 2021-06-28 |
| store7   | 89948753 |          643 | 2021-06-28 |
| store3   | 89948754 |          654 | 2021-06-28 |
| store10  | 89948755 |          185 | 2021-06-28 |
| store5   | 89948756 |            2 | 2021-06-28 |
| store5   | 89948757 |          448 | 2021-06-29 |
| store6   | 89948758 |          732 | 2021-06-29 |
| store4   | 89948759 |          112 | 2021-06-29 |
| store10  | 89948760 |          893 | 2021-06-29 |
| store10  | 89948761 |          665 | 2021-06-29 |
| store10  | 89948762 |          301 | 2021-06-29 |
| store5   | 89948763 |          161 | 2021-06-30 |
| store6   | 89948764 |          268 | 2021-06-30 |
| store3   | 89948765 |           84 | 2021-06-30 |
| store2   | 89948766 |          636 | 2021-06-30 |
| store5   | 89948767 |          789 | 2021-06-30 |
| store7   | 89948768 |          920 | 2021-06-30 |
| store10  | 89948769 |          930 | 2021-06-30 |
| store3   | 89948770 |          518 | 2021-06-30 |
| store5   | 89948771 |          970 | 2021-06-30 |
| store9   | 89948772 |          762 | 2021-07-01 |
| store10  | 89948773 |            9 | 2021-07-01 |
| store9   | 89948774 |          194 | 2021-07-01 |
| store9   | 89948775 |          253 | 2021-07-01 |
| store7   | 89948776 |          723 | 2021-07-01 |
| store7   | 89948777 |          911 | 2021-07-01 |
| store8   | 89948778 |          335 | 2021-07-01 |
| store1   | 89948779 |            7 | 2021-07-02 |
| store6   | 89948780 |          368 | 2021-07-02 |
| store1   | 89948781 |          948 | 2021-07-02 |
| store5   | 89948782 |          855 | 2021-07-02 |
| store6   | 89948783 |          870 | 2021-07-02 |
| store3   | 89948784 |          189 | 2021-07-02 |
| store9   | 89948785 |          132 | 2021-07-02 |
| store8   | 89948786 |          137 | 2021-07-02 |
| store1   | 89948787 |          728 | 2021-07-02 |
| store5   | 89948788 |          376 | 2021-07-03 |
| store8   | 89948789 |          882 | 2021-07-03 |
| store7   | 89948790 |          187 | 2021-07-03 |
| store2   | 89948791 |          489 | 2021-07-03 |
| store3   | 89948792 |          639 | 2021-07-03 |
| store1   | 89948793 |          558 | 2021-07-03 |
| store3   | 89948794 |          487 | 2021-07-03 |
| store2   | 89948795 |          719 | 2021-07-04 |
| store9   | 89948796 |          652 | 2021-07-04 |
| store9   | 89948797 |          703 | 2021-07-04 |
| store6   | 89948798 |          340 | 2021-07-04 |
| store7   | 89948799 |          510 | 2021-07-04 |
| store8   | 89948800 |          701 | 2021-07-04 |
| store1   | 89948801 |           52 | 2021-07-04 |
| store1   | 89948802 |           59 | 2021-07-05 |
| store3   | 89948803 |          325 | 2021-07-05 |
| store4   | 89948804 |          665 | 2021-07-05 |
| store1   | 89948805 |           45 | 2021-07-05 |
| store2   | 89948806 |          268 | 2021-07-05 |
| store6   | 89948807 |           42 | 2021-07-05 |
| store2   | 89948808 |          264 | 2021-07-05 |
| store10  | 89948809 |           17 | 2021-07-05 |
| store5   | 89948810 |           83 | 2021-07-05 |
| store9   | 89948811 |          199 | 2021-07-05 |
| store1   | 89948812 |           20 | 2021-07-05 |
| store8   | 89948813 |           26 | 2021-07-05 |
| store1   | 89948814 |          125 | 2021-07-06 |
| store2   | 89948815 |          317 | 2021-07-06 |
| store1   | 89948816 |           43 | 2021-07-06 |
| store4   | 89948817 |          720 | 2021-07-06 |
| store2   | 89948818 |          163 | 2021-07-06 |
| store8   | 89948819 |          663 | 2021-07-06 |
| store6   | 89948820 |           23 | 2021-07-06 |
+----------+----------+--------------+------------+
97 rows in set (0.01 sec)

```



### 2.统计(20分)

#### 2.1.统计每日每个店铺的销量(10分)

```sql
select salesdate, store_id, sum(sales_volume)
from store_table
group by salesdate, store_id;
```

query results:

```
+------------+----------+-------------------+
| salesdate  | store_id | sum(sales_volume) |
+------------+----------+-------------------+
| 2021-06-25 | store8   |               499 |
| 2021-06-25 | store1   |               693 |
| 2021-06-25 | store9   |               569 |
| 2021-06-25 | store3   |               579 |
| 2021-06-25 | store7   |               228 |
| 2021-06-26 | store2   |               557 |
| 2021-06-26 | store6   |              1061 |
| 2021-06-26 | store10  |                18 |
| 2021-06-26 | store7   |               309 |
| 2021-06-26 | store1   |              2537 |
| 2021-06-26 | store5   |               977 |
| 2021-06-27 | store2   |               791 |
| 2021-06-27 | store3   |               860 |
| 2021-06-27 | store6   |               985 |
| 2021-06-27 | store8   |               874 |
| 2021-06-27 | store4   |               837 |
| 2021-06-28 | store2   |               167 |
| 2021-06-28 | store9   |                81 |
| 2021-06-28 | store1   |               648 |
| 2021-06-28 | store4   |               651 |
| 2021-06-28 | store10  |               801 |
| 2021-06-28 | store7   |               643 |
| 2021-06-28 | store3   |               654 |
| 2021-06-28 | store5   |                 2 |
| 2021-06-29 | store5   |               448 |
| 2021-06-29 | store6   |               732 |
| 2021-06-29 | store4   |               112 |
| 2021-06-29 | store10  |              1859 |
| 2021-06-30 | store5   |              1920 |
| 2021-06-30 | store6   |               268 |
| 2021-06-30 | store3   |               602 |
| 2021-06-30 | store2   |               636 |
| 2021-06-30 | store7   |               920 |
| 2021-06-30 | store10  |               930 |
| 2021-07-01 | store9   |              1209 |
| 2021-07-01 | store10  |                 9 |
| 2021-07-01 | store7   |              1634 |
| 2021-07-01 | store8   |               335 |
| 2021-07-02 | store1   |              1683 |
| 2021-07-02 | store6   |              1238 |
| 2021-07-02 | store5   |               855 |
| 2021-07-02 | store3   |               189 |
| 2021-07-02 | store9   |               132 |
| 2021-07-02 | store8   |               137 |
| 2021-07-03 | store5   |               376 |
| 2021-07-03 | store8   |               882 |
| 2021-07-03 | store7   |               187 |
| 2021-07-03 | store2   |               489 |
| 2021-07-03 | store3   |              1126 |
| 2021-07-03 | store1   |               558 |
| 2021-07-04 | store2   |               719 |
| 2021-07-04 | store9   |              1355 |
| 2021-07-04 | store6   |               340 |
| 2021-07-04 | store7   |               510 |
| 2021-07-04 | store8   |               701 |
| 2021-07-04 | store1   |                52 |
| 2021-07-05 | store1   |               124 |
| 2021-07-05 | store3   |               325 |
| 2021-07-05 | store4   |               665 |
| 2021-07-05 | store2   |               532 |
| 2021-07-05 | store6   |                42 |
| 2021-07-05 | store10  |                17 |
| 2021-07-05 | store5   |                83 |
| 2021-07-05 | store9   |               199 |
| 2021-07-05 | store8   |                26 |
| 2021-07-06 | store1   |               168 |
| 2021-07-06 | store2   |               480 |
| 2021-07-06 | store4   |               720 |
| 2021-07-06 | store8   |               663 |
| 2021-07-06 | store6   |                23 |
+------------+----------+-------------------+
70 rows in set (0.00 sec)

```

#### 2.2.统计每日每个区域的销量(10分)

```sql
select s.salesdate, a.area, sum(s.sales_volume)
from store_table s 
left join area_table a
on s.store_id = a.store_id
group by s.salesdate, a.area;
```

query results:

```
+------------+-------+---------------------+
| salesdate  | area  | sum(s.sales_volume) |
+------------+-------+---------------------+
| 2021-06-25 | beta  |                1296 |
| 2021-06-25 | alpha |                1272 |
| 2021-06-26 | alpha |                3094 |
| 2021-06-26 | beta  |                2365 |
| 2021-06-27 | alpha |                2488 |
| 2021-06-27 | beta  |                1859 |
| 2021-06-28 | alpha |                2120 |
| 2021-06-28 | beta  |                1527 |
| 2021-06-29 | beta  |                3039 |
| 2021-06-29 | alpha |                 112 |
| 2021-06-30 | beta  |                4038 |
| 2021-06-30 | alpha |                1238 |
| 2021-07-01 | beta  |                3187 |
| 2021-07-02 | alpha |                1872 |
| 2021-07-02 | beta  |                2362 |
| 2021-07-03 | beta  |                1445 |
| 2021-07-03 | alpha |                2173 |
| 2021-07-04 | alpha |                 771 |
| 2021-07-04 | beta  |                2906 |
| 2021-07-05 | alpha |                1646 |
| 2021-07-05 | beta  |                 367 |
| 2021-07-06 | alpha |                1368 |
| 2021-07-06 | beta  |                 686 |
+------------+-------+---------------------+
23 rows in set (0.00 sec)

```



### 3.程序异常(20分)

本公司有报表需要展示区域“alpha”的前一日所有店铺销量，示例如下: 

 ```txt
店铺     昨日销量
store1   124
store2   532
store3   325
store4   665
 ```

某天由于“store3”停业，当日“store3”在store_table表中没有数据。

当第二天需要出昨日报表数据时。 报表原本预想展示的数据如下: 

```txt
店铺     昨日销量
store1  168
store2  480 
store3  0 
store4  720
```

后台代码为:

```sql
select a.store_id, IFNULL(sum(sales_volume),0) as sales_volumes
from area_table a
left join
store_table b
on a.store_id=b.store_id
where a.area='alpha'
and b.salesdate=DATE_SUB(curdate(),INTERVAL 1 DAY) 
GROUP BY a.store_id
order by a.store_id;

-- 注: DATE_SUB(curdate(),INTERVAL 1 DAY) 该函数意为:求出昨天的日期; 
-- 实现过程:1.使用curdate()求出当天日期;
-- 2.使用DATE_SUB(当天日期,INTERVAL 1 DAY)实现当天日期减一天的日期，即昨日日期;
```

#### 3.1.请分析代码为什么无法执行出期望结果?(10分)

```
经过groupby和sum聚合后，store_id这一列，没有‘store3’这一个值；
而源代码中IFNULL的操作的设定的前提是store_id这一列有‘store3‘这个值，并且对应的sales_volumnes值为NULL。
```

#### 3.2.请修改代码，产出报表的设想结果。(10分)

```sql
select stores.store_id, IFNULL(sales_stats.sales_volumes, 0) sales_volumes
from 
(select distinct a.store_id
from area_table a
where a.area = 'alpha') stores

left join
(select a.store_id, sum(sales_volume) as sales_volumes
from area_table a
left join store_table s
on a.store_id = s.store_id
where a.area = 'alpha'
and s.salesdate = DATE_SUB(curdate(), INTERVAL 1 DAY) 
GROUP BY a.store_id
order by a.store_id) sales_stats

on stores.store_id = sales_stats.store_id;
```

query results:

```
mysql> select stores.store_id, IFNULL(sales_stats.sales_volumes, 0) sales_volumes
    -> from 
    -> (select distinct a.store_id
    -> from area_table a
    -> where a.area = 'alpha') stores
    -> 
    -> left join
    -> (select a.store_id, sum(sales_volume) as sales_volumes
    -> from area_table a
    -> left join store_table s
    -> on a.store_id = s.store_id
    -> where a.area = 'alpha'
    -> and s.salesdate = DATE_SUB(curdate(), INTERVAL 1 DAY) 
    -> GROUP BY a.store_id
    -> order by a.store_id) sales_stats
    -> 
    -> on stores.store_id = sales_stats.store_id;
+----------+---------------+
| store_id | sales_volumes |
+----------+---------------+
| store1   |           168 |
| store2   |           480 |
| store3   |             0 |
| store4   |           720 |
+----------+---------------+
4 rows in set (0.00 sec)

```

 

## 二、SQL语句阅读能力(40分)

能够读懂业务SQL是一个数据分析师的基本技能。
掌握SQL的执行顺序，才能正确地读懂SQL语句。 

**执行顺序
<u>With as --></u> 
<u>FROM(JOIN 部分一般先左后右) --></u> 
<u>WHERE --></u> 
<u>GROUP BY --></u> 
<u>HAVING --></u> 
<u>SELECT --></u> 
<u>ORDER BY</u>**

每个部分中，先执行子查询内部，再将子查询看做一个整体，按普通顺序执行。
多个子查询嵌套，最先执行最内部的子查询。

### 1. 请按如下方式写出SQL语句的执行顺序

```sql
-- 例子:
select uclass,sum(money) as uclass_money 
from user_table a
join (select uid, money
from user_earn_table where money is not null )b
on a.uid = b.uid
group by uclass
order by uclass_money desc limit 100;

-- 拆解示意:
-- 1.from user_earn_table 
-- 2.where money is not null 
-- 3.select uid,money
-- 4.from user_table a
-- 5.join (select uid,money from user_earn_table where money is not null )b on a.uid = b.uid
-- 6.group by uclass
-- 7.select uclass,sum(money) as uclass_money 
-- 8.order by uclass_money desc
-- 9.limit 100;
```

请拆解如下语句:
本语句为某视频网站公司对于k-pop业务线的各渠道用户、新增用户数据抽取语句

```sql
with temp_active_hour_table_kps as 
(select a0.dt, product_id, mkey, substr(FROM_UNIXTIME(st_time),12,2) as hour, a0.device_id
 from (select dt, product_id, st_time, device_id
       from kps_dwd.kps_dwd_dd_view_user_active 
       where dt='${dt_1}') a0
 left join (select dt, mkey, device_id
            from kps_dwd.kps_dwd_dd_user_channels 
            where dt='${dt_1}') a1
 on a0.device_id = a1.device_id)
 
 select dt, product, product_id, a1.mkey, name_cn, hour, status, dau, new
 from (select dt, 'K-pop' as product, product_id, mkey, hour, status, 
       count(distinct a.device_id) as dau, 
       count(distinct if(b.device_id is not null, a.device_id, null)) as new
       from (select dt, product_id, mkey, hour, device_id, 'active' as status
             from temp_active_hour_table_kps
             group by dt, mkey, product_id, device_id,hour
             
             union all
             
             select dt, product_id, mkey, min(hour) as hour, device_id, 'first' as status
             from temp_active_hour_table_kps
             group by dt,mkey,product_id,device_id) a
       left join (select dt, device_id
                  from kps_dwd.kps_dwd_dd_fact_view_new_user
                  where dt='${dt_1}'
                  group by dt,device_id) b 
       on a.dt=b.dt and a.device_id = b.device_id
       group by dt, product_id, mkey, hour, status) a1 
left join asian_channel.dict_lcmas_channel b1
on a1.mkey = b1.mkey;


--notes:
-- line1 - 9: with ***** as 
-- line11 - 31: select **** from a1 left join b1
```

-- 拆解步骤如下，请将下列横线处拆解内容补充完整 
-- 以下题4个空，每空10分

```sql
-- 1.
from kps_dwd.kps_dwd_dd_view_user_active 
```

```sql
-- 2.
where dt='${dt_1}'
```

```sql
-- 3.
select dt, product_id, st_time, device_id
```

```sql
-- 4.
from (......) a0
```

```sql
-- 5.
left join (select dt, mkey, device_id
           from kps_dwd.kps_dwd_dd_user_channels
           where dt='${dt_1}') a1
on a0.device_id = a1.device_id
```

```sql
-- 填空

-- 6.
with temp_active_hour_table_kps as 
(select a0.dt, product_id, mkey, substr(FROM_UNIXTIME(st_time),12,2) as hour, a0.device_id
 ......)
```

```sql
-- 7.
select dt, product_id, mkey, hour, device_id, 'active' as status
from temp_active_hour_table_kps
group by dt, mkey, product_id, device_id, hour
```

```sql
-- 填空

-- 8. 
select dt, product_id, mkey, min(hour) as hour, device_id, 'first' as status
from temp_active_hour_table_kps
group by dt,mkey,product_id,device_id) a
```

```sql
-- 9.
from
(......) a
```

```sql
-- 填空

-- 10.
left join (select dt, device_id
           from kps_dwd.kps_dwd_dd_fact_view_new_user
           where dt='${dt_1}'
           group by dt,device_id) b 
on a.dt=b.dt and a.device_id = b.device_id
```

```sql
-- 11.
from (select dt, 'K-pop' as product, product_id, mkey, hour, status,
      count(distinct a.device_id) as dau,
      count(distinct if(b.device_id is not null,a.device_id,null)) as new
      ......
      group by dt, product_id, mkey, hour, status) a1
```

```sql
-- 填空

-- 12. 
left join asian_channel.dict_lcmas_channel b1
on a1.mkey = b1.mkey
```

```sql
-- 13.
select dt, product, product_id, a1.mkey, name_cn, hour, status, dau, new
```

![IMG_0328](/Users/xieyanan/Desktop/IMG_0328.jpg)



