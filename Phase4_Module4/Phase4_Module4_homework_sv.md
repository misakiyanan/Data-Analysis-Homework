## 作业要求

### 1. 数据存入Hive中，要求分层处理

* ODS层：元数据存储

```sql
create database homework_module4_ods;
use homework_module4_ods;

create table game(
    user_id int,
    register_time timestamp,
    wood_add_value double,
    wood_reduce_value double,
    stone_add_value double,
    stone_reduce_value double,
    ivory_add_value double,
    ivory_reduce_value double,
    meat_add_value double,
    meat_reduce_value double,
    magic_add_value double,
    magic_reduce_value double,
    infantry_add_value double,
    infantry_reduce_value double,
    cavalry_add_value double,
    cavalry_reduce_value double,
    shaman_add_value double,
    shaman_reduce_value double,
    wound_infantry_add_value double,
    wound_infantry_reduce_value double,
    wound_cavalry_add_value double,
    wound_cavalry_reduce_value double,
    wound_shaman_add_value double,
    wound_shaman_reduce_value double,
    general_acceleration_add_value double,
    general_acceleration_reduce_value double,
    building_acceleration_add_value double,
    building_acceleration_reduce_value double,
    reaserch_acceleration_add_value double,
    reaserch_acceleration_reduce_value double,
    training_acceleration_add_value double,
    training_acceleration_reduce_value double,
    treatment_acceleraion_add_value double,
    treatment_acceleration_reduce_value double,
    bd_training_hut_level double,
    bd_healing_lodge_level double,
    bd_stronghold_level double,
    bd_outpost_portal_level double,
    bd_barrack_level double,
    bd_healing_spring_level double,
    bd_dolmen_level double,
    bd_guest_cavern_level double,
    bd_warehouse_level double,
    bd_watchtower_level double,
    bd_magic_coin_tree_level double,
    bd_hall_of_war_level double,
    bd_market_level double,
    bd_hero_gacha_level double,
    bd_hero_strengthen_level double,
    bd_hero_pve_level double,
    sr_scout_level double,
    sr_training_speed_level double,
    sr_infantry_tier_2_level double,
    sr_cavalry_tier_2_level double,
    sr_shaman_tier_2_level double,
    sr_infantry_atk_level double,
    sr_cavalry_atk_level double,
    sr_shaman_atk_level double,
    sr_infantry_tier_3_level double,
    sr_cavalry_tier_3_level double,
    sr_shaman_tier_3_level double,
    sr_troop_defense_level double,
    sr_infantry_def_level double,
    sr_cavalry_def_level double,
    sr_shaman_def_level double,
    sr_infantry_hp_level double,
    sr_cavalry_hp_level double,
    sr_shaman_hp_level double,
    sr_infantry_tier_4_level double,
    sr_cavalry_tier_4_level double,
    sr_shaman_tier_4_level double,
    sr_troop_attack_level double,
    sr_construction_speed_level double,
    sr_hide_storage_level double,
    sr_troop_consumption_level double,
    sr_rss_a_prod_levell double,
    sr_rss_b_prod_level double,
    sr_rss_c_prod_level double,
    sr_rss_d_prod_level double,
    sr_rss_a_gather_level double,
    sr_rss_b_gather_level double,
    sr_rss_c_gather_level double,
    sr_rss_d_gather_level double,
    sr_troop_load_level double,
    sr_rss_e_gather_level double,
    sr_rss_e_prod_level double,
    sr_outpost_durability_level double,
    sr_outpost_tier_2_level double,
    sr_healing_space_level double,
    sr_gathering_hunter_buff_level double,
    sr_healing_speed_level double,
    sr_outpost_tier_3_level double,
    sr_alliance_march_speed_level double,
    sr_pvp_march_speed_level double,
    sr_gathering_march_speed_level double,
    sr_outpost_tier_4_level double,
    sr_guest_troop_capacity_level double,
    sr_march_size_level double,
    sr_rss_help_bonus_level double,
    pvp_battle_count double,
    pvp_lanch_count double,
    pvp_win_count double,
    pve_battle_count double,
    pve_lanch_count double,
    pve_win_count double,
    avg_online_minutes double,
    pay_price double,
    pay_count double,
    prediction_pay_price double
  ) 
  row format delimited fields terminated by ','
  tblproperties("skip.header.line.count"="1");
  
  load data local inpath '/home/hadoop/datas/tap_fun_train.csv' overwrite into table game;
```

* DWD层：数据清洗

```sql
create database homework_module4_dwd;
use homework_module4_dwd;

create table game_dwd as
(select 
     user_id,
     register_time,
     wood_reduce_value,
     meat_reduce_value,
     general_acceleration_add_value as ga_add,
     general_acceleration_reduce_value as ga_reduce,
     building_acceleration_add_value as ba_add,
     building_acceleration_reduce_value as ba_reduce,
     reaserch_acceleration_add_value as ra_add,
     reaserch_acceleration_reduce_value as ra_reduce,
     training_acceleration_add_value as ta_add,
     training_acceleration_reduce_value as ta_reduce,
     treatment_acceleraion_add_value as tma_add,
     treatment_acceleration_reduce_value as tma_reduce,
     avg_online_minutes,
     pay_count,
     pay_price,
     case 
         when pay_price=0 then '零消费'
         when pay_price <= 68 then '低消费'
         when pay_price <= 202 then '中消费'
         when pay_price > 202 then '高消费'
         end
     as purchase_level
 from homework_module4_ods.game 
);
```

```
查看数据
select * from game_dwd limit 5;

game_dwd.user_id	game_dwd.register_time	game_dwd.wood_reduce_value	game_dwd.meat_reduce_value	game_dwd.ga_add	game_dwd.ga_reduce	game_dwd.ba_add	game_dwd.ba_reduce	game_dwd.ra_add	game_dwd.ra_reduce	game_dwd.ta_add	game_dwd.ta_reduce	game_dwd.tma_addgame_dwd.tma_reduce	game_dwd.avg_online_minutes	game_dwd.pay_count	game_dwd.pay_price	game_dwd.purchase_level
1	2018-02-02 19:47:15	3700.0	2000.0	0.0	0.0	0.0	0.0	50.0	0.0	50.0	0.0	0.0	0.0	0.333333	0.0	0.0	零消费
1593	2018-01-26 00:01:05	0.0	0.0	0.0	0.0	0.0	0.0	0.0	0.0	0.0	0.0	0.0	0.0	0.333333	0.0	0.0	零消费
1594	2018-01-26 00:01:58	0.0	0.0	0.0	0.0	0.0	0.0	0.0	0.0	0.0	0.0	0.0	0.0	1.166667	0.0	0.0	零消费
1595	2018-01-26 00:02:13	0.0	0.0	0.0	0.0	0.0	0.0	0.0	0.0	0.0	0.0	0.0	0.0	3.166667	0.0	0.0	零消费
1596	2018-01-26 00:02:46	0.0	0.0	0.0	0.0	0.0	0.0	0.0	0.0	0.0	0.0	0.0	0.0	2.333333	0.0	0.0	零消费
Time taken: 0.275 seconds, Fetched: 5 row(s)
```

* DWS层轻度汇总

```sql
create database homework_module4_dws;
use homework_module4_dws;
```

	* a 统计每日新用户

```sql
-- 统计每日新用户
create table daily_new_users as 
(select 
     to_date(register_time) as reg_date,
     count(distinct user_id) as num_of_reg_users
 from homework_module4_dwd.game_dwd
 group by to_date(register_time)
 order by reg_date asc
 limit 1000
);
```

```
-- 查看
select * from daily_new_users;

daily_new_users.reg_date	daily_new_users.num_of_reg_users
2018-01-26	70250
2018-01-27	70417
2018-01-28	79227
2018-01-29	63803
2018-01-30	50201
2018-01-31	56522
2018-02-01	83245
2018-02-02	60173
2018-02-03	51659
2018-02-04	60421
2018-02-05	60998
2018-02-06	57203
2018-02-07	71576
2018-02-08	72402
2018-02-09	50143
2018-02-10	53521
2018-02-11	54014
2018-02-12	52231
2018-02-13	50638
2018-02-14	54419
2018-02-15	78707
2018-02-16	56355
2018-02-17	44477
2018-02-18	59447
2018-02-19	117311
2018-02-20	92860
2018-02-21	43720
2018-02-22	42110
2018-02-23	44635
2018-02-24	45648
2018-02-25	49835
2018-02-26	42647
2018-02-27	39140
2018-02-28	42928
2018-03-01	36226
2018-03-02	42775
2018-03-03	48970
2018-03-04	50989
2018-03-05	44726
2018-03-06	41438
Time taken: 0.132 seconds, Fetched: 40 row(s)
```

* b 统计流失用户

```sql
-- 统计流失用户
create table loss_users as 
(select
    user_id,
    wood_reduce_value,
    meat_reduce_value,
    avg_online_minutes,
    pay_count
from homework_module4_dwd.game_dwd
where 
 (wood_reduce_value=0 and meat_reduce_value=0 and avg_online_minutes<=2) 
 or pay_count=0
);

```

```
查看数据
select * from loss_users limit 10;

loss_users.user_id	loss_users.wood_reduce_value	loss_users.meat_reduce_value	loss_users.avg_online_minutes	loss_users.pay_count
1	3700.0	2000.0	0.333333	0.0
1593	0.0	0.0	0.333333	0.0
1594	0.0	0.0	1.166667	0.0
1595	0.0	0.0	3.166667	0.0
1596	0.0	0.0	2.333333	0.0
1597	0.0	0.0	0.166667	0.0
1598	0.0	0.0	4.0	0.0
1599	0.0	0.0	0.666667	0.0
1600	0.0	0.0	1.833333	0.0
1601	0.0	0.0	0.5	0.0
Time taken: 0.109 seconds, Fetched: 10 row(s)

```

* APP层

```sql
create database homework_module4_app;
use homework_module4_app;
```

* a 量： a1. 每日新用户

  ```sql
  select * 
  from homework_module4_dws.daily_new_users;
  ```

  ```
  daily_new_users.reg_date	daily_new_users.num_of_reg_users
  2018-01-26	70250
  2018-01-27	70417
  2018-01-28	79227
  2018-01-29	63803
  2018-01-30	50201
  2018-01-31	56522
  2018-02-01	83245
  2018-02-02	60173
  2018-02-03	51659
  2018-02-04	60421
  2018-02-05	60998
  2018-02-06	57203
  2018-02-07	71576
  2018-02-08	72402
  2018-02-09	50143
  2018-02-10	53521
  2018-02-11	54014
  2018-02-12	52231
  2018-02-13	50638
  2018-02-14	54419
  2018-02-15	78707
  2018-02-16	56355
  2018-02-17	44477
  2018-02-18	59447
  2018-02-19	117311
  2018-02-20	92860
  2018-02-21	43720
  2018-02-22	42110
  2018-02-23	44635
  2018-02-24	45648
  2018-02-25	49835
  2018-02-26	42647
  2018-02-27	39140
  2018-02-28	42928
  2018-03-01	36226
  2018-03-02	42775
  2018-03-03	48970
  2018-03-04	50989
  2018-03-05	44726
  2018-03-06	41438
  Time taken: 0.39 seconds, Fetched: 40 row(s)
  ```

  

* b 质： b1. 新用户付费率

  ```sql
  select 
  sum(a.is_payuser) / count(distinct user_id) as pay_rate
  from
  (select 
  user_id,
  if(pay_count>0, 1, 0) as is_payuser
  from homework_module4_dwd.game_dwd)a;
  ```

  ```
  pay_rate
  0.018111395638212645
  Time taken: 30.062 seconds, Fetched: 1 row(s)
  ```

  

* b 质： b2. ARPU -- 用户的平均收入

  ```sql
  select 
  sum(pay_price) / count(distinct user_id) as arpu
  from homework_module4_dwd.game_dwd;
  ```

  ```
  arpu
  0.5346691072184288
  Time taken: 29.223 seconds, Fetched: 1 row(s)
  ```

  

* b 质： b3. ARPPU -- 付费用户平均收益

  ```sql
  select 
  b.total_price / b.pay_user as arppu
  from
  (select 
  sum(pay_price) as total_price,
  count(distinct user_id) as pay_user
  from homework_module4_dwd.game_dwd
  where pay_count > 0) b;
  ```

  ```
  arppu
  29.521143367347563
  Time taken: 23.119 seconds, Fetched: 1 row(s)
  ```

  

* c 用户消费程度： c1. 不同消费程度的平均在线时长

  ```sql
  select 
  purchase_level,
  round(avg(avg_online_minutes), 2) as avg_online_time
  from homework_module4_dwd.game_dwd
  group by purchase_level; 
  ```

  ```
  purchase_level	avg_online_time
  中消费	286.46
  低消费	127.41
  零消费	7.81
  高消费	377.02
  Time taken: 22.865 seconds, Fetched: 4 row(s)
  ```

  

* c 用户消费程度： c2. 不同消费程度的数量分布

  ```sql
  select 
  purchase_level,
  count(distinct user_id) as num_of_users
  from homework_module4_dwd.game_dwd
  group by purchase_level; 
  ```

  ```
  purchase_level	num_of_users
  中消费	1684
  低消费	38707
  零消费	2246568
  高消费	1048
  Time taken: 31.584 seconds, Fetched: 4 row(s)
  
  ```

  

* d 加速卷获取使用情况：d1. 不同群体的加速卷获取情况对比

  ```sql
  -- 统计不同消费群体各类加速卷的平均获取情况
  select 
  purchase_level,
  round(avg(ga_add), 2) as avg_ga_add,
  round(avg(ba_add), 2) as avg_ba_add,
  round(avg(ra_add), 2) as avg_ra_add,
  round(avg(ta_add), 2) as avg_ta_add,
  round(avg(tma_add), 2) as avg_tma_add
  
  from homework_module4_dwd.game_dwd
  group by purchase_level;
  ```

  ```
  purchase_level	avg_ga_add	avg_ba_add	avg_ra_add	avg_ta_add	avg_tma_add
  中消费	27809.56	13525.6	13960.16	17181.48	390.6
  低消费	4167.81	3427.6	3386.74	2208.85	150.5
  零消费	150.63	122.82	46.91	137.49	7.41
  高消费	95332.35	37167.1	41563.06	56312.49	669.88
  
  ```

  

* d 加速卷获取使用情况：d2. 不同群体的加速卷使用情况对比

  ```sql
  -- 统计不同消费群体各类加速卷的平均使用情况
  select 
  purchase_level,
  round(avg(ga_reduce), 2) as avg_ga_reduce,
  round(avg(ba_reduce), 2) as avg_ba_reduce,
  round(avg(ra_reduce), 2) as avg_ra_reduce,
  round(avg(ta_reduce), 2) as avg_ta_reduce,
  round(avg(tma_reduce), 2) as avg_tma_reduce
  
  from homework_module4_dwd.game_dwd
  group by purchase_level;
  ```

  ```
  purchase_level	avg_ga_reduce	avg_ba_reduce	avg_ra_reduce	avg_ta_reduce	avg_tma_reduce
  中消费	22164.23	12183.57	10809.24	10847.86	68.47
  低消费	2901.37	2665.84	2120.21	1207.27	11.85
  零消费	90.84	74.55	13.19	16.78	0.07
  高消费	81731.52	33859.81	36856.31	43964.98	115.62
  Time taken: 23.912 seconds, Fetched: 4 row(s)
  
  ```

  

### 2. 使用Tableau对业务需求监控的指标进行可视化
(在原始数据中采样1/10)

![tableau](https://github.com/misakiyanan/Data-Analysis-Homework/blob/main/Phase4_Module4/tableau.jpg)

