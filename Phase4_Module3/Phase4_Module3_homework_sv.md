### 抖音介绍 

短视频从泛娱乐中突围，反超长视频，活跃用户规模高达6.4亿，现已进入存量厮杀战。短视频商业变 现模式成熟，广告和直播打赏收入占比接近90%，营收可观。   
而受到用户兴趣转移、商家普及、疫情催化等因素影响，2020年短视频行业与直播电商行业均迎来了势 头强劲的增长，在行业关键数据上的变化多次超出市场分析机构的预测，行业格局也迎来了重新洗牌。     
抖音的电商业务依托于各自的短视频平台的巨量流量溢出和用户的电商需求而诞生，其发展必然会受到 短视频行业发展的一定限制。电商业务细分下来又分为短视频电商和直播电商，其中直播电商是业务核 心。      
短视频平台本质上是内容平台，使用市场对内容平台来说极其重要的，有了足够的市场才能够投放更多 的广告，才能探索更多元化的业务，DAU对平台极为重要。 

### 业务需求 

从20年3月开始，企业进行大力运营拉新，使得日活（新用户+老用户）快速上升， 但是最近一周（4月 12-4月18） 日活极具下降， 拉新运营活动停止对其影响的贡献率为45.6%，另一主要原因是新用户回访的比例只有10%，现需求是 提升拉新运营活动所拉来的新 用户的留存(提高日活) ，使其日活（DAU） 达到35000。 

### 数据介绍 

| **字段名**    | **注释**           |
| ------------- | ------------------ |
| uid           | 用户id             |
| user_city     | 用户所在的城市     |
| item_id       | 作品id             |
| author_id     | 作者id             |
| item_city     | 作品城市           |
| channel       | 观看到该作品的来源 |
| finish        | 是否浏览完作品     |
| like          | 是否对作品点赞     |
| music_id      | 音乐id             |
| device        | 设备id             |
| duration_time | 作品时长           |
| public_time   | 作品发布时间       |
| show_time     | 观看时间           |



### 作业要求 

#### 统计现状

```sql
create database homework_module3;
use homework_module3;

create table tiktok(
    uid int,
    user_city int,
    item_id int,
    author_id int,
    item_city int,
    channel int,
    finish int,
    like_ int,
    music_id int,
    device int,
    duration_time int,
    public_time timestamp,
    show_time timestamp
)
row format delimited fields terminated by ','
tblproperties("skip.header.line.count"="1");

load data local inpath '/home/hadoop/datas/final_track2_test.csv' overwrite into
table tiktok;
```



1. 统计以每日、小时 、周几为维度发布的视频数 

   * <u>每日视频数</u>

   ```sql
   select 
   to_date(public_time) as public_date,
   count(distinct item_id)
   from tiktok 
   group by to_date(public_time)
   order by public_date
   limit 1000;
   ```

   ```
   截取部分查询结果
   
   public_date	_c1
   2018-02-08	8
   2018-02-12	2
   2018-04-24	1
   ...................
   2019-01-01	33
   2019-01-02	34
   2019-01-03	28
   ...................
   2020-01-01	764
   2020-01-02	779
   2020-01-03	922
   ...................
   2020-04-01	29551
   2020-04-02	30538
   2020-04-03	32293
   2020-04-04	32891
   2020-04-05	36165
   2020-04-06	38219
   2020-04-07	41704
   2020-04-08	47369
   2020-04-09	75262
   2020-04-10	91209
   2020-04-11	95462
   2020-04-12	94326
   2020-04-13	88710
   2020-04-14	87748
   2020-04-15	87095
   2020-04-16	77104
   2020-04-17	70230
   2020-04-18	21689
   Time taken: 102.173 seconds, Fetched: 681 row(s)
   
   ```
   
   * 每小时视频数
   
   ```sql
   select 
   date(public_time) as public_date,
   hour(public_time) as public_hour,
   count(distinct item_id) as num_of_items
   from tiktok
   group by date(public_time), hour(public_time)
   order by date(public_time), hour(public_time)
   limit 10000000;
   ```
   
   ```
   (截取部分查询结果)
   2020-04-17	0	3958
   2020-04-17	1	4375
   2020-04-17	2	4693
   2020-04-17	3	4827
   2020-04-17	4	4166
   2020-04-17	5	2981
   2020-04-17	6	2799
   2020-04-17	7	1872
   2020-04-17	8	1277
   2020-04-17	9	1093
   2020-04-17	10	562
   2020-04-17	11	462
   2020-04-17	12	502
   2020-04-17	13	770
   2020-04-17	14	1700
   2020-04-17	15	2978
   2020-04-17	16	3779
   2020-04-17	17	4182
   2020-04-17	18	4158
   2020-04-17	19	4167
   2020-04-17	20	4192
   2020-04-17	21	4002
   2020-04-17	22	3485
   2020-04-17	23	3250
   2020-04-18	0	3145
   2020-04-18	1	3474
   2020-04-18	2	3765
   2020-04-18	3	3516
   2020-04-18	4	2993
   2020-04-18	5	2316
   2020-04-18	6	1539
   2020-04-18	7	748
   2020-04-18	8	193
   Time taken: 91.981 seconds, Fetched: 11678 row(s)
   
   ```
   
   * <u>按周几为维度统计</u>
   
   ```sql
   select 
       date_format(public_time, 'u') as dow,
       case date_format(public_time, 'u')
           when 1 then "Mon"
           when 2 then "Tue"
           when 3 then "Wed"
           when 4 then "Thu"
           when 5 then "Fri"
           when 6 then "Sat"
           when 7 then "Sun"
           end as dayofweek,
       count(distinct item_id) as num_of_items
   from tiktok
   group by date_format(public_time, 'u')
   order by dow
   limit 7;
   ```
   
   ```
   dow	dayofweek	num_of_items
   1	Mon	        218888
   2	Tue	        226783
   3	Wed	        239288
   4	Thu	        262971
   5	Fri	        277095
   6	Sat	        237168
   7	Sun	        221178
   Time taken: 90.856 seconds, Fetched: 7 row(s)
   
   ```
   
   
   
2. 统计每日的在线人数

   ```sql
   select date(show_time) as view_date,
   count(distinct uid) dau
   from tiktok
   group by date(show_time)
   order by view_date
   limit 1000;
   ```

   ```
   (截取部分查询结果)
   
   view_date	dau
   2018-02-11	7
   2018-02-16	9
   2018-04-27	1
   2018-05-07	1
   2018-05-14	2
   2018-05-17	1
   2018-05-28	2
   2018-05-29	1
   2018-05-30	1
   ..................
   2020-04-13	47186
   2020-04-14	46111
   2020-04-15	45095
   2020-04-16	43328
   2020-04-17	42370
   2020-04-18	39293
   2020-04-19	36179
   2020-04-20	31568
   2020-04-21	19601
   Time taken: 82.407 seconds, Fetched: 682 row(s)
   
   ```

   

3. 统计各视频的点赞率、观看完成率（点赞人数/ 观看人数）

   ```sql
   -- 由于视频数量大（1，688，371条视频），
   -- 这里只统计观看量超过1000的视频的点赞率和观看完成率
   -- 以观察结果分布
   select 
       item_id,
       count(*) as view_count,
       concat(cast(sum(like_) / count(uid) as decimal(5, 4)) * 100, "%") as like_rate,
       concat(cast(sum(finish) /count(uid) as decimal(5, 4)) * 100, "%") as finish_rate
   from tiktok
   group by item_id
   having count(*) >= 1000
   order by view_count desc
   limit 1000;
   ```

   ```
   item_id	view_count	like_rate	finish_rate
   6404	2100	0.38%	43.62%
   2541	1959	0.2%	47.93%
   87	1934	0.88%	47.41%
   867	1869	0.37%	47.51%
   3966	1852	0.22%	59.72%
   634336	1835	0.98%	65.99%
   12177	1667	0.6%	67.07%
   5862	1655	0.73%	49.85%
   1942	1585	0.69%	35.52%
   10605	1544	0.52%	47.02%
   4020	1534	0.13%	47.72%
   259	1532	0.26%	40.6%
   4296	1526	0.33%	47.84%
   218	1526	1.05%	37.29%
   109	1523	0.92%	38.41%
   122	1497	0.4%	53.71%
   900	1449	0.48%	27.33%
   7596	1445	0.62%	35.43%
   672	1365	0.15%	61.17%
   7428	1359	1.47%	39.66%
   633060	1352	1.33%	61.32%
   1	1327	0.38%	23.51%
   4388	1327	0.9%	48.68%
   13956	1324	0.15%	41.99%
   6353	1313	0.99%	52.78%
   100	1297	1.23%	54.2%
   5818	1279	0.94%	39.48%
   674	1267	0.63%	40.17%
   2907020	1236	3.32%	30.42%
   4076	1234	0.49%	46.43%
   1996	1231	1.38%	37.53%
   1499	1223	0.33%	25.59%
   739	1196	1.42%	51.92%
   776	1190	0.25%	24.79%
   11	1189	2.1%	50.71%
   8971	1175	2.3%	21.45%
   1876	1166	1.63%	21.61%
   2141	1164	0.6%	38.06%
   2432	1160	0.69%	41.98%
   1565	1144	0.79%	24.83%
   8155	1140	2.54%	41.4%
   1035	1135	0.53%	47.75%
   1519	1131	0.44%	56.68%
   4341	1129	0.44%	62.09%
   1297	1122	0.8%	56.24%
   633370	1118	0.45%	48.21%
   7715	1116	0.09%	45.61%
   1338	1110	0.18%	37.93%
   1688	1109	0.72%	54.37%
   3799	1107	2.08%	33.79%
   1381	1087	1.93%	35.23%
   17972	1085	1.75%	43.41%
   1851	1083	0.55%	30.29%
   159019	1082	3.42%	22.18%
   2195	1081	0.56%	50.05%
   885	1079	1.2%	43.19%
   10468	1077	0.37%	45.68%
   3986	1067	0.28%	29.24%
   8566	1057	1.04%	49.76%
   636589	1055	0.38%	42.18%
   16576	1054	0.85%	31.12%
   2283	1047	3.15%	45.65%
   11400	1046	1.43%	33.65%
   2194	1045	0.19%	53.97%
   1229	1040	0.38%	37.21%
   6971	1038	1.16%	41.23%
   10578	1032	0.97%	53.78%
   3697	1022	0.49%	28.77%
   10739	1021	0.29%	43.29%
   4547	1019	0.29%	34.74%
   633534	1015	0.3%	71.43%
   76828	1010	2.38%	21.49%
   1804	1009	1.19%	39.35%
   6624	1006	1.79%	22.17%
   4362	1004	0.7%	54.68%
   4213	1004	1.2%	47.91%
   Time taken: 76.168 seconds, Fetched: 76 row(s)
   
   ```

   

4. 统计各城市用户数分布 

   ````sql
   select 
       user_city,
       count(distinct uid) as user_count
   from tiktok
   group by user_city
   order by user_count desc
   limit 1000;
   ````

   ```
   截取部分查询结果
   user_city	user_count
   -1	37266
   99	2419
   6	1588
   129	1314
   109	1232
   31	1181
   70	1029
   21	1018
   73	990
   114	982
   57	970
   14	925
   140	826
   47	820
   134	798
   ..............
   382	1
   395	1
   372	1
   379	1
   387	1
   Time taken: 64.814 seconds, Fetched: 393 row(s)
   
   ```

   

##### 业务实现

1. 对业务需求进行拆解以脑图的形式进行展现（其中包含为用户推荐好的视频）

   ![](https://tva1.sinaimg.cn/large/008i3skNgy1gsjt44yg4jj317q0og76c.jpg)

2. HQL实现为用户推荐感兴趣的视频（以内容黏住用户）

   ```sql
   -- 1. 计算用户对视频的喜好程度
   -- 定义评分规则：	
       -- a.初始分数 +1（存在一条播放记录即默认有播放行为）
       -- b.看完视频 +3
       -- c.点赞     +6
   -- (满分10，可能的评分计算结果集合{1， 4， 7， 10})：
   
   create table user_video_preference as
       select 
           uid,
           item_id,
           1 + finish*3 + like_*6 as pre
       from tiktok
       -- 受算力限制，筛选样本
       where uid in (0, 100, 200)
       and item_id between 5000 and 10000;
   ```

   ```sql
   -- 2.基于商品的 (item-based) 协同过滤
   -- 2.1 计算视频相似度矩阵
   create table video_corr as
   with temp as (
       select 
           t1.item_id as item1, 
           t2.item_id as item2, 
           sum(t1.pre * t2.pre) as pre 
       from user_video_preference as t1
       inner join user_video_preference as t2
       on t1.uid = t2.uid
       group by t1.item_id, t2.item_id
   )
   select 
       t1.item1,
       t1.item2,
       t1.pre/(sqrt(t2.pre) * sqrt(t3.pre)) as correl
   from temp
   as  t1
   inner join temp as t2 on t1.item1=t2.item1 and t2.item1=t2.item2
   inner join temp as t3 on t1.item2=t3.item1 and t3.item1=t3.item2;
   
   
   -- 2.2 预测用户喜好程度，推荐视频
   create table user_video_rec as 
   with temp as 
   (select
        uid, 
        item2, 
        sum(pre * correl) as pre 
    from user_video_preference
    inner join video_corr
    on item_id = item1
    group by uid, item2
   )
   
   select *
   from (
       select *, 
              rank() over(partition by uid order by pre desc) as row_rank
       from temp
       where concat(uid, item2) not in (
           select concat(uid, item_id) 
           from user_video_preference)
   ) sub
   where row_rank = 1;
   
   
   
   -- 查询推荐结果
   select * from user_video_rec;
   
   /* -- 查询结果
   user_video_rec.uid	user_video_rec.item_id	user_video_rec.row_rank
   0   9980     1
   100 6964     1
   200 9405     1
   Time taken: 0.233 seconds, Fetched: 3 row(s)
   */
   
   ```

   