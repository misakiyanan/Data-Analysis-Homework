## 一、数据源合并练习

### 1. 把同一文件夹下相同表头的Excel表合并到一起;(请自建数据源完成)(10分)

### 2. 把【市场渠道每日激活量和留存率】下三张sheet表数据关联到一张表中(10分)

![join](/Users/xieyanan/Desktop/Online Courses/lagou/big_data_analysis/阶段3模块1作业/images/join.png)

## 二、字段处理练习

### 1. 新增度量字段:ROI，公式为:ROI = 渠道LTV / 人均消耗(10分)

![roi](/Users/xieyanan/Desktop/Online Courses/lagou/big_data_analysis/阶段3模块1作业/images/roi.png)

### 2. 新增维度字段:投放结果

公式为:如果ROI>=2 则“放量投放”;如果0.8<RO1<2 则”观望“;如果ROI<=0.8 则“缩量投放”(10分)

![result](/Users/xieyanan/Desktop/Online Courses/lagou/big_data_analysis/阶段3模块1作业/images/result.png)

### 3. 新增参数字段:投放结果(参数)

参数为0-1之间的浮点数，公式为:如果ROI>=2则“放量投放”;如果[参数]<RO1<2 则”观望“;如果ROI<=[参数] 则“缩量投放”(10分)

![parameter](/Users/xieyanan/Desktop/Online Courses/lagou/big_data_analysis/阶段3模块1作业/images/parameter.png)

![result_param](/Users/xieyanan/Desktop/Online Courses/lagou/big_data_analysis/阶段3模块1作业/images/result_param.png)

## 三、可视化练习

### 1. 制作ROI 和留存率的散点图，以渠道为粒度，以投放结果(参数)作为颜色图例，并将渠道区分为 四象限，分象限进行描述(15分)

![clusters](/Users/xieyanan/Desktop/Online Courses/lagou/big_data_analysis/阶段3模块1作业/images/clusters.png)

### 2. 在上一题的基础上对渠道进行聚类，并对类别进行简单描述(15分)

![clusters](/Users/xieyanan/Desktop/Online Courses/lagou/big_data_analysis/阶段3模块1作业/images/clusters.png)

### 3. 制作渠道质量管理数据看板

包括但不限于以下内容:每日消耗、每日激活量、分渠道激活占比、渠道激活量排名、渠道投放结果判断(参数)、渠道四象限分类图、渠道ROI跌破1高亮预警(20 分)



![board](/Users/xieyanan/Desktop/Online Courses/lagou/big_data_analysis/阶段3模块1作业/images/board.png)

