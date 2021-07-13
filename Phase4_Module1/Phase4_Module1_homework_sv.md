### 简述题

### 1. Hadoop分布式集群，一般有三个重要部分组成?

分布式存储 - HDFS [Hadoop Distributed File System]       
分布式计算 - MapReduce      
数据仓库 - Hive

### 2. 什么是元数据，以及元数据的作用?

元数据是**关于数据的数据 data about data**，它是有关数据的描述和语境。 它有助于组织、查找和理解数据。

作用：    
分类 - 元数据在分类或组织内容中起着关键作用。   
保留 - 元数据可用于跟踪与文档的相关记录计划相关联的日期等内容。  
信息安全 - 元数据可用于标记安全设置、验证访问和编辑权限，从而控制分发。   
客户体验 - 元数据还可以用作捕获用户对内容的评分的一种方式，例如，表明内容是“有价值的”或“无用的”甚至是“过时的”。  
信息“可查找性”——元数据作为一种搜索和检索增强机制非常有价值，它使用户能够针对某个领域（如作者、主题、日期等）进行查询。      

ref. https://dataedo.com/kb/data-glossary/what-is-metadata

<font size=2>Metadata is simply **data about data**. It means it is a description and context of the data. It helps to organize, find and understand data.</font>

<font size=2>ref. https://info.aiim.org/aiim-blog/what-is-metadata-definition-and-value.</font>

<font size=2>The *ISO Records Management Standard 15489* definition: "Data describing content, content, and structure of records and their management through time."</font>

<font size=2>The *US Department of Defense DoD 5015.2 standard* definition: "Data describing stored data: that is, data describing the structure, data elements, interrelationships, and other characteristics of electronic records."</font>

<font size=2>The *US National Information Standards Organization (NISO)* definition: "Structured information that describes, explains, locates, or otherwise makes it easier to retrieve, use, or manage an information resource."</font>

ref. https://info.aiim.org/aiim-blog/what-is-metadata-definition-and-value

### 3. 数据可以分为哪些层次结构?

数据运营层ODS - 存放接入的原始数据     
数据仓库DW - 存放按主题建立的各种数据模型    
数据应用层APP - 面向业务定制的应用数据      
维表层DIM

### 4. 简述HDFS的读写过程(读数据以及写数据)

HDFS 遵循“一次写入多次读取”模型。我们无法编辑已经存储在 HDFS 中的文件，但可以通过重新打开文件来追加数据。首先在读写操作客户端(client)中，与NameNode进行交互，NameNode 提供特权，然后客户端可以从各自的数据节点中读取和写入数据块。

**写文件：**要在 HDFS 中写入文件，客户端需要与 master 交互，即 namenode（master）。namenode为客户端提供了地址，作为客户端开始写入数据的节点，之后在datanode上写入数据，datanode会创建数据写入管道(data write pipeline)。第一个数据节点将块(block)复制到另一个数据节点，另一个数据节点将其复制到第三个数据节点。一旦它创建了块的副本(replica)，它就会发回确认(acknowlegement)。

**读文件：**要从 HDFS 读取文件，客户端需要与 namenode（master）交互，因为 namenode 是 Hadoop 集群的核心（它存储所有元数据，即有关数据的数据）。namenode 检查所需的权限，如果客户端具有足够的权限，则 namenode 提供存储文件的从站地址，客户端将直接与相应的数据节点交互以读取数据块。

ref. https://data-flair.training/blogs/hadoop-hdfs-data-read-and-write-operations/



#### 5. 简述MapReduce的工作流程

##### Map

输入数据首先被分成更小的块。 然后将每个块(block)分配给一个映射器(mapper)进行处理。  
例如，如果一个文件有 100 条记录要处理，则 100 个映射器可以一起运行，每个映射器处理一条记录。 或者也许 50 个映射器可以一起运行，每个映射器处理两条记录。 Hadoop 框架根据要处理的数据大小和每个映射器服务器上可用的内存块来决定使用多少个映射器。

##### Reduce

在所有映射器完成处理后，框架会在将结果传递给化简器(reducer)之前对结果进行混洗(shuffle)和排序(sorting)。 当映射器(mapper)仍在进行中时，减速器(reducer)无法启动。 具有相同键(key)的所有映射输出值都分配给单个减速器(reducer)，然后聚合(aggregate)该键的值。

ref. https://www.talend.com/resources/what-is-mapreduce/

##### Map

<font size=2>The input data is first split into smaller blocks. Each block is then assigned to a mapper for processing.    
For example, if a file has 100 records to be processed, 100 mappers can run together to process one record each. Or maybe 50 mappers can run together to process two records each. The Hadoop framework decides how many mappers to use, based on the size of the data to be processed and the memory block available on each mapper server.</font>

##### Reduce

<font size=2>After all the mappers complete processing, the framework shuffles and sorts the results before passing them on to the reducers. A reducer cannot start while a mapper is still in progress. All the map output values that have the same key are assigned to a single reducer, which then aggregates the values for that key.</font>

##### Combine and Partition

<font size=2>There are two intermediate steps between Map and Reduce.</font>

<font size=2>**Combine** is an optional process. The combiner is a reducer that runs individually on each mapper server. It reduces the data on each mapper further to a simplified form before passing it downstream.</font>

<font size=2>This makes shuffling and sorting easier as there is less data to work with. Often, the combiner class is set to the reducer class itself, due to the cumulative and associative functions in the reduce function. However, if needed, the combiner can be a separate class as well.</font>

<font size=2>**Partition** is the process that translates the <key, value> pairs resulting from mappers to another set of <key, value> pairs to feed into the reducer. It decides how the data has to be presented to the reducer and also assigns it to a particular reducer.</font>

<font size=2>The default partitioner determines the hash value for the key, resulting from the mapper, and assigns a partition based on this hash value. There are as many partitions as there are reducers. So, once the partitioning is complete, the data from each partition is sent to a specific reducer.</font>

