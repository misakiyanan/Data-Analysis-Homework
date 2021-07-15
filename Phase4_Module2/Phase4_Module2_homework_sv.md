### ⼀、背景 

某电商平台为了合理的投⼊⼈⼒物⼒创造更⼤的销售利润，现对已有的销售数据进⾏⽤户分析，提 出合理的促销计划。 围绕产品和⽤户两⼤⽅⾯展开为电商平台制定策略提供分析及建议。 

### ⼆、需求 

* ⽤户分析：从性别、年龄、 职业、城市、居住年限，婚姻状况等维度找到⾼质量⽤户，并查看⾼质 量⽤户⼈群的占⽐，为其提供⾼价值消费品 (定位⾼价值消费品以销售⾦额评估）。针对其他的⽤户，主要引导⽤户进⾏购买，多推荐⼀些热销的商品(定位热销产品) 
* 产品分析：从销量、销售额都⾼的产品并以⼆⼋法则找到⾼贡献的⼀级产品类⽬ 

### 三、数据介绍

**假定每条记录为一单**

| **字段名称**               | **字段描述**     |
| -------------------------- | ---------------- |
| User_ID                    | 顾客ID           |
| Product_ID                 | 商品ID           |
| Gender                     | 顾客性别         |
| Age                        | 顾客年龄         |
| Occupation                 | 顾客从事职业ID   |
| City_Category              | 城市类别         |
| Stay_In_Current_City_Years | 在现城市呆的年数 |
| Marital_Status             | 婚姻状况         |
| Product_Category_1         | 商品类别1        |
| Product_Category_2         | 商品类别2        |
| Product_Category_3         | 商品类别3        |
| Purchase                   | 消费金额         |



```sql
-- 建表

-- create databse
create database homework_module2;
use homework_module2;

-- create table
create table module2_datas 
(User_ID int,
 Product_ID string,
 Gender string,
 Age string,
 Occupation int,
 City_Category string,
 Stay_In_Current_City_Years string,
 Marital_Status int,
 Product_Category_1 int,
 Product_Category_2 int,
 Product_Category_3 int,
 Purchase double
)
row format delimited fields terminated by ','
tblproperties(
"skip.header.line.count"="1"
);


load data local inpath '/home/hadoop/datas/model2_datas.csv' overwrite into
table model2_datas;

# ref. 
# https://blog.csdn.net/weixin_55759540/article/details/117661026
```



### 四、 需求实现 

1. 查询订单整体的消费情况（包括：总销售额、⼈均消费、平均每单消费）（10分）

   ```sql
   select 
   sum(Purchase) as TotalAmount,
   sum(Purchase) / count(distinct User_ID) as UserAverage,
   sum(Purchase) / count(*) as OrderAmount
   from module2_datas;
   ```

   query result:

   ```
   totalamount 	useraverage     	orderamount
   5.017668378E9	851751.5494822611	9333.859852635065
   ```

   

2. ⽤户分析（找到⾼质量⼈群） （30分） 

   * 统计各性别消费情况(字段包含性别、⼈数、⼈数占⽐、⼈均消费、消费⾦额、消费占⽐) 并以消费占⽐降序 

     ```sql
     select 
     a.gender as gender,
     a.user_count as user_count,
     concat(round(user_count / sum(user_count) over(), 4) * 100, "%") as user_count_ratio,
     round(a.purchase_amount / a.user_count, 2) as average_purchase,
     a.purchase_amount as purchase_amount,
     concat(round(purchase_amount / sum(purchase_amount) over(), 4) * 100, "%") as purchase_amount_ratio
     from
     (select 
     gender,
     count(distinct user_id) as user_count,
     cast(sum(purchase) as bigint) as purchase_amount
     from module2_datas
     group by gender) as a
     order by purchase_amount / sum(purchase_amount) over() desc
     limit 2
     ;
     ```

     query result:

     ```
     gender	user_count	user_count_ratio  average_purchase	purchase_amount	purchase_amount_ratio
     M	    4225	    71.72%	          911963.16         3853044357	    76.79%
     F	    1666	    28.28%	          699054.03	        1164624021	    23.21%
     Time taken: 71.306 seconds, Fetched: 2 row(s)
     ```

     

   * 统计各年龄段消费情况(字段包含年龄段 、 ⼈数、 ⼈数占⽐、消费⾦额、 ⼈均消费、消费占⽐)并以消费占⽐降序 

     ```sql
     select
     a.age as age,
     a.user_count as user_count,
     concat(round(a.user_count / sum(a.user_count) over(), 4) * 100, "%") as user_count_ratio,
     a.purchase_amount as purchase_amount,
     round(a.purchase_amount / a.user_count, 2) as average_amount,
     concat(round(a.purchase_amount / sum(a.purchase_amount) over(), 4) * 100, "%") as amount_ratio
     from
     (select age,
     count(distinct user_id) as user_count,
     cast(sum(purchase) as bigint) as purchase_amount
     from module2_datas
     group by age) as a
     order by a.purchase_amount / sum(a.purchase_amount) over() desc
     limit 7;
     ```

     query result:

     <font color='grey'>(问题: 1. 小数位数， 2. limit变量）</font>

     ```
     age	user_count	user_count_ratio	purchase_amount	average_amount	amount_ratio
     26-35	2053	34.849999999999994%	1999749106	    974061.91	    39.85%
     36-45	1167	19.81%	            1010649565	    866023.62	    20.14%
     18-25	1069	18.15%	            901669280	    843469.86	    17.97%
     46-50	531 	9.01%	            413418223	    778565.39	    8.24%
     51-55	481	    8.16%	            361908356	    752408.22	    7.21%
     55+  	372	    6.3100000000000005%	 197614842	     531222.69	     3.94%
     0-17	218	    3.6999999999999997%	 132659006	     608527.55	     2.64%
     Time taken: 69.208 seconds, Fetched: 7 row(s)
     ```

     

   * 统计各职业消费情况(字段包含职业 、⼈数、⼈数占⽐、消费⾦额、⼈均消费、消费占⽐) 并以消费占⽐降序

     ```sql
     select
     a.occupation as occupation,
     a.user_count as user_count,
     concat(round(a.user_count / sum(a.user_count) over(), 4) * 100, "%") as user_count_ratio,
     a.purchase_amount as purchase_amount,
     round(a.purchase_amount / a.user_count, 2) as average_amount,
     concat(round(a.purchase_amount / sum(a.purchase_amount) over(), 4) * 100, "%") as amount_ratio
     from
     (select occupation,
     count(distinct user_id) as user_count,
     cast(sum(purchase) as bigint) as purchase_amount
     from module2_datas
     group by occupation) as a
     order by a.purchase_amount / sum(a.purchase_amount) over() desc
     limit 21;
     ```

     query result:

     ```
     occupation	user_count	user_count_ratio	purchase_amount	average_amount	amount_ratio
     4	740	12.559999999999999%	657530393	888554.59	13.100000000000001%
     0	688	11.68%	            625814811	909614.55	12.47%
     7	669	11.360000000000001%	549282744	821050.44	10.95%
     1	517	8.780000000000001%	414552829	801843.0	8.260000000000002%
     17	491	8.33%	            387240355	788676.89	7.720000000000001%
     12	376	6.38%	            300672105	799659.85	5.99%
     20	273	4.63%	            292276985	1070611.67	5.82%
     14	294	4.99%	            255594745	869369.88	5.09%
     16	235	3.9899999999999998%	234442330	997626.94	4.67%
     2	256	4.35%	            233275393	911232.0	4.65%
     6	228	3.8699999999999997%	185065697	811691.65	3.6900000000000004%
     3	170	2.8899999999999997%	160428450	943696.76	3.2%
     15	140	2.3800000000000003%	116540026	832428.76	2.32%
     10	192	3.26%	            114273954	595176.84	2.2800000000000002%
     5	111	1.8800000000000001%	112525355	1013741.94	2.2399999999999998%
     11	128	2.17%	            105437359	823729.37	2.1%
     19	71	1.21%	            73115489	1029795.62	1.46%
     13	140	2.3800000000000003%	71135744	508112.46	1.4200000000000002%
     18	67	1.1400000000000001%	60249706	899249.34	1.2%
     9	88	1.49%	            53619309	609310.33	1.0699999999999998%
     8	17	0.29%	            14594599	858505.82	0.29%
     Time taken: 73.497 seconds, Fetched: 21 row(s)
     
     ```

     

   *  统计各婚姻状况消费情况(字段包含婚姻状况 、⼈数、⼈数占⽐、消费⾦额、⼈均消费、消费占⽐) 并以消费占⽐降序

     ```sql
     select
     a.marital_status as m_status,
     a.user_count as user_count,
     concat(round(a.user_count / sum(a.user_count) over(), 4) * 100, "%") as user_count_ratio,
     a.purchase_amount as purchase_amount,
     round(a.purchase_amount / a.user_count, 2) as average_amount,
     concat(round(a.purchase_amount / sum(a.purchase_amount) over(), 4) * 100, "%") as amount_ratio
     from
     (select marital_status,
     count(distinct user_id) as user_count,
     cast(sum(purchase) as bigint) as purchase_amount
     from module2_datas
     group by marital_status) as a
     order by a.purchase_amount / sum(a.purchase_amount) over() desc
     limit 2;
     ```

     query result:

     ```
     m_status	user_count	user_count_ratio	purchase_amountaverage_amount	amount_ratio
     0	3417	57.99999999999999%	2966289500	868097.6                        59.12%
     1	2474	42.0%	            2051378878	829174.97	                    40.88%
     Time taken: 73.162 seconds, Fetched: 2 row(s)
     
     ```

     

   * 依据以上查询结果找到⾼质量⼈群（ 如：性别为...，年龄段为...，职业为...，婚姻状况为...) 

     ```
     高质量人群：
     性别M，年龄26-35，职业为4，婚姻状况为0
     ```

     

3. 产品分析 （30分） 

   * 查询出订单量TOP10的产品 (包含字段排名编号、商品ID、销量、销量占⽐ )按销量降序显示 

     ```sql
     select
     rank() over(order by a.sales desc) as rank_,
     a.product_id as prod_id,
     a.sales as sales,
     concat(round(a.sales / sum(a.sales) over(), 4) * 100, "%") as sales_ratio
     from
     (select
     product_id,
     count(*) as sales
     from module2_datas
     group by product_id) as a
     order by rank_
     limit 10;
     
     ```

     query result:

     ```
     rank_	prod_id	sales	sales_ratio
     1	P00265242	1858	0.35000000000000003%
     2	P00110742	1591	0.3%
     3	P00025442	1586	0.3%
     4	P00112142	1539	0.29%
     5	P00057642	1430	0.27%
     6	P00184942	1424	0.26%
     7	P00046742	1417	0.26%
     8	P00058042	1396	0.26%
     9	P00145042	1384	0.26%
     9	P00059442	1384	0.26%
     Time taken: 98.576 seconds, Fetched: 10 row(s)
     
     ```

     

   * 查询出销售额TOP10的产品 (包含字段 排名编号、商品ID、销售额、销售额占⽐ ) 

     ```sql
     select
     rank() over(order by a.purchase_amt desc) as rank_,
     a.product_id as prod_id,
     a.purchase_amt as purchase_amt,
     concat(round(a.purchase_amt / sum(a.purchase_amt) over(), 4) * 100, "%") as purchase_amt_ratio
     from
     (select
     product_id,
     cast(sum(purchase) as bigint) as purchase_amt
     from module2_datas
     group by product_id) as a
     order by rank_
     limit 10;
     ```

     query result:

     ```
     rank_	prod_id	purchase_amt	purchase_amt_ratio
     1	P00025442	27532426	0.5499999999999999%
     2	P00110742	26382569	0.53%
     3	P00255842	24652442	0.49%
     4	P00184942	24060871	0.48%
     5	P00059442	23948299	0.48%
     6	P00112142	23882624	0.48%
     7	P00110942	23232538	0.45999999999999996%
     8	P00237542	23096487	0.45999999999999996%
     9	P00057642	22493690	0.44999999999999996%
     10	P00010742	21865042	0.44%
     Time taken: 97.998 seconds, Fetched: 10 row(s)
     
     ```

     

   * 统计各⼀级产品类⽬的订单量、销售额、订单量占⽐、销售额占⽐、累计销售额占⽐ （以销售额占⽐降序），并根据查询结果找到累计销售额达到20%的⼏个⼀级产品类⽬

     **1级 category_1**

     ```sql
     select
     rank() over(order by a.purchase_amt desc) as rank_,
     a.product_category_1 as prod_cate_1,
     a.sales as sales,
     a.purchase_amt as purchase_amt,
     concat(round(a.sales / sum(a.sales) over(), 4) * 100, "%") as sales_ratio,
     concat(round(a.purchase_amt / sum(a.purchase_amt) over(), 4) * 100, "%") as purchase_amt_ratio,
     concat(round(sum(a.purchase_amt) over(order by purchase_amt desc rows between unbounded preceding and current row) / sum(a.purchase_amt) over(), 4) * 100, "%") as acc_pur_ratio
     from
     (select
     product_category_1, 
     count(*) as sales,
     cast(sum(purchase) as bigint) as purchase_amt
     from module2_datas
     group by product_category_1) as a
     order by rank_
     limit 18;
     ```

     ```
     rank_	prod_cate_1	sales	purchase_amt	sales_ratio	purchase_amt_ratio	acc_pur_ratio
     1	1	138353	1882666325	25.740000000000002%	37.519999999999996%	 37.519999999999996%
     2	5	148592	926917497	27.639999999999997%	18.47%               55.989999999999995%
     3	8	112132	840693394	20.86%	            16.75%	             72.75%
     4	6	20164	319355286	3.75%	            6.36%	             79.11%
     5	2	23499	264497242	4.37%	            5.27%	             84.38%
     6	3	19849	200412211	3.6900000000000004%	3.9899999999999998%	 88.38000000000001%
     7	16	9697	143168035	1.7999999999999998%	2.85%                91.23%
     8	11	23960	112203088	4.46%	            2.2399999999999998%  93.47%
     9	10	5032	99029631	0.9400000000000001%	1.97%                95.44%
     10	15	6203	91658147	1.15%	            1.83%	             97.27%
     11	7	3668	60059209	0.6799999999999999%	1.2%                 98.47%
     12	4	11567	26937957	2.15%	            0.54%	             99.0%
     13	14	1500	19718178	0.27999999999999997%	0.38999999999999996%	99.4%
     14	18	3075	9149071 	0.5700000000000001%	    0.18%	                 99.58%
     15	9	404	    6277472  	0.08%	                0.13%	                 99.7%
     16	17	567	    5758702	    0.11%	                 0.11%	                  99.82%
     17	12	3875	5235883	    0.72%	                 0.1%	                 99.92%
     18	13	5440	3931050	    1.01%	                 0.08%	                  100.0%
     Time taken: 103.335 seconds, Fetched: 18 row(s)
     
     ```

     ```
     累计销售额达到20%的⼏个⼀级产品类⽬:
     rank_5 ~ rank_18的商品
     ```

     <font color='grey'>问题：所有组合？</font>

     

     **2级 product_category_2**

     ```sql
     select
     rank() over(order by a.purchase_amt desc) as rank_,
     a.product_category_2 as prod_cate_2,
     a.sales as sales,
     a.purchase_amt as purchase_amt,
     concat(round(a.sales / sum(a.sales) over(), 4) * 100, "%") as sales_ratio,
     concat(round(a.purchase_amt / sum(a.purchase_amt) over(), 4) * 100, "%") as purchase_amt_ratio,
     concat(round(sum(a.purchase_amt) over(order by purchase_amt desc rows between unbounded preceding and current row) / sum(a.purchase_amt) over(), 4) * 100, "%") as acc_pur_ratio
     from
     (select
     product_category_2, 
     count(*) as sales,
     cast(sum(purchase) as bigint) as purchase_amt
     from module2_datas
     where product_category_2 is not null
     group by product_category_2) as a
     order by rank_
     limit 17;
     ```

     ```
     rank_	prod_cate_2	sales	purchase_amt	sales_ratio	purchase_amt_ratio	acc_pur_ratio
     1	2	48481	660395610	13.08%	17.69%	17.69%
     2	8	63058	648112417	17.02%	17.36%	35.06%
     3	16	42602	438744196	11.5%	11.75%	46.81%
     4	15	37317	386556477	10.07%	10.36%	57.17%
     5	14	54158	384866069	14.610000000000001%	10.31% 67.47999999999999%
     6	4	25225	257757097	6.81%	6.909999999999999%     74.38%
     7	5	25874	233747130	6.98%	6.260000000000001%     80.65%
     8	6	16251	186896021	4.390000000000001%	5.01%  85.65%
     9	11	13945	124608092	3.7600000000000002%	3.34%  88.99000000000001%
     10	17	13130	123639094	3.54%	3.3099999999999996%    92.30000000000001%
     11	13	10369	100291709	2.8000000000000003%	2.69%  94.99%
     12	10	2991	46827140	0.8099999999999999%	1.25%  96.25%
     13	9	5591	40716981	1.51%	1.09%	97.34%
     14	12	5419	37763181	1.46%	1.01%	98.35000000000001%
     15	3	2835	31835725	0.76%	0.8500000000000001%    99.2%
     16	18	2730	25582006	0.74%	0.69%	99.89%
     17	7	615	4229499	0.16999999999999998%	0.11%	100.0%
     Time taken: 98.591 seconds, Fetched: 17 row(s)
     
     ```

     **3级 product_category_3**

     ```sql
     select
     rank() over(order by a.purchase_amt desc) as rank_,
     a.product_category_3 as prod_cate_3,
     a.sales as sales,
     a.purchase_amt as purchase_amt,
     concat(round(a.sales / sum(a.sales) over(), 4) * 100, "%") as sales_ratio,
     concat(round(a.purchase_amt / sum(a.purchase_amt) over(), 4) * 100, "%") as purchase_amt_ratio,
     concat(round(sum(a.purchase_amt) over(order by purchase_amt desc rows between unbounded preceding and current row) / sum(a.purchase_amt) over(), 4) * 100, "%") as acc_pur_ratio
     from
     (select
     product_category_3, 
     count(*) as sales,
     cast(sum(purchase) as bigint) as purchase_amt
     from module2_datas
     where product_category_3 is not null
     group by product_category_3) as a
     order by rank_
     limit 15;
     ```

     ```
     rank_	prod_cate_3	sales	purchase_amt	sales_ratio	purchase_amt_ratio	acc_pur_ratio
     1	16	32148	385213413	19.57%	20.11%	20.11%
     2	15	27611	340670945	16.81%	17.78%	37.89%
     3	5	16380	198662402	9.969999999999999%	10.37% 48.26%
     4	17	16449	193760503	10.01%	10.11%	58.379999999999995%
     5	14	18121	182187903	11.03%	9.51%	67.89%
     6	8	12384	161357998	7.539999999999999%	8.42%  76.31%
     7	9	11414	119043392	6.950000000000001%	6.21%  82.53%
     8	12	9094	79288332	5.54%	4.14%	86.66%
     9	13	5385	70990467	3.2800000000000002%	3.71%  90.36999999999999%
     10	6	4818	63548518	2.93%	3.32%	93.69%
     11	18	4563	50118090	2.78%	2.62%	96.3%
     12	10	1698	22962030	1.03%	1.2%	97.5%
     13	11	1773	21475687	1.08%	1.1199999999999999%    98.61999999999999%
     14	4	1840	17992055	1.1199999999999999%	0.9400000000000001%	99.56%
     15	3	600	8374300	0.37%	0.44%	100.0%
     Time taken: 106.514 seconds, Fetched: 15 row(s)
     ```

     

     

4. 细化分析 （30分） 

   * 查询出各性别销售额TOP10 产品 （ 字段包含商品ID、订单量、销售额、销售额占⽐、类别1、类 别2、类别3）

     ```sql
     select * from(
     select
     rank() over(partition by a.gender order by a.purchase_amt desc) as rank_,
     a.gender as gender,
     a.product_id as prod_id,
     a.sales as sales,
     a.purchase_amt as purchase_amt,
     concat(round(a.purchase_amt / sum(a.purchase_amt) over(), 4) * 100, "%") as purchase_amt_ratio,
     b.pcate1 as pcate1,
     b.pcate2 as pcate2,
     b.pcate3 as pcate3
     from
     (select
     gender,
     product_id,
     count(*) as sales,
     cast(sum(purchase) as bigint) as purchase_amt
     from module2_datas
     group by gender, product_id) as a
     left join
     (select
     distinct product_id,
     product_category_1 as pcate1,
     product_category_2 as pcate2,
     product_category_3 as pcate3
     from module2_datas) as b
     on a.product_id = b.product_id) as c
     where c.rank_ <= 10
     distribute by c.gender
     sort by c.rank_;
     ```

     ```
     c.rank_	c.gender	c.prod_id	c.sales	c.purchase_amt	c.purchase_amt_ratio	c.pcate1	c.pcate2	c.pcate3
     1	F	P00255842	366	6690088	0.13%	16	NULL	NULL
     1	M	P00025442	1245	21768902	0.43%	1	2	9
     2	F	P00059442	350	6007826	0.12%	6	8	16
     2	M	P00110742	1234	20750212	0.41000000000000003%	1	2	8
     3	F	P00110842	351	5933348	0.12%	1	2	5
     3	M	P00184942	1131	19337647	0.38999999999999996%	1	8	17
     4	F	P00025442	341	5763524	0.11%	1	2	9
     4	M	P00112142	1207	18981577	0.38%	1	2	14
     5	F	P00110742	357	5632357	0.11%	1	2	8
     5	M	P00057642	1174	18720360	0.37%	1	15	16
     6	F	P00110942	305	5066142	0.1%	1	2	NULL
     6	M	P00237542	1092	18562039	0.37%	1	15	16
     7	F	P00148642	300	5049905	0.1%	6	10	13
     7	M	P00110942	1031	18166396	0.36%	1	2	NULL
     8	M	P00255842	988	17962354	0.36%	16	NULL	NULL
     8	F	P00112142	332	4901047	0.1%	1	2	14
     9	F	P00028842	289	4867128	0.1%	6	8	NULL
     9	M	P00059442	1034	17940473	0.36%	6	8	16
     10	F	P00184942	293	4723224	0.09%	1	8	17
     10	M	P00010742	1056	17517618	0.35000000000000003%	1	8	17
     Time taken: 155.2 seconds, Fetched: 20 row(s)
     
     ```

     

   * 查询⾼质量⽤户群年龄段的订单量TOP10产品（ 字段包含商品ID、订单量、销售额、订单量占⽐、 销售额占⽐、类别1、类别2、类别3）

     ```sql
     select * from
     (select
     rank() over(order by a.purchase_amt desc) as rank_,
     a.product_id as prod_id,
     a.sales as sales,
     a.purchase_amt as purchase_amt,
     concat(round(a.purchase_amt / sum(a.purchase_amt) over(), 4) * 100, "%") as purchase_amt_ratio,
     b.pcate1 as pcate1,
     b.pcate2 as pcate2,
     b.pcate3 as pcate3
     from
     (select
     product_id,
     count(*) as sales,
     cast(sum(purchase) as bigint) as purchase_amt
     from module2_datas
     where age="26-35"
     group by product_id) as a
     left join
     (select
     distinct product_id,
     product_category_1 as pcate1,
     product_category_2 as pcate2,
     product_category_3 as pcate3
     from module2_datas) as b
     on a.product_id = b.product_id) as c
     where c.rank_ <= 10;
     ```

     ```
     c.rank_	c.prod_id	c.sales	c.purchase_amt	c.purchase_amt_ratio	c.pcate1	c.pcate2	c.pcate3
     10	P00057642	581	9110947	0.45999999999999996%	1	15	16
     9	P00059442	535	9211235	0.45999999999999996%	6	8	16
     8	P00110942	519	9218356	0.45999999999999996%	1	2	NULL
     7	P00112142	597	9258356	0.45999999999999996%	1	2	14
     6	P00028842	516	9286868	0.45999999999999996%	6	8	NULL
     5	P00184942	570	9493975	0.47000000000000003%	1	8	17
     4	P00237542	568	9697110	0.48%	1	15	16
     3	P00255842	537	9860878	0.49%	16	NULL	NULL
     2	P00025442	595	10594786	0.53%	1	2	9
     1	P00110742	627	10605442	0.53%	1	2	8
     Time taken: 137.401 seconds, Fetched: 10 row(s)
     
     ```

     