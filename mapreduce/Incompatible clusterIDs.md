**时间**

20170309

**错误描述**

```shell
FATAL org.apache.hadoop.hdfs.server.datanode.DataNode: 
Initialization failed for Block pool <registering> (Datanode Uuid unassigned) 
service to master/192.168.16.60:9000. Exiting. 
java.io.IOException: Incompatible clusterIDs in /opt/hadoop-2.6.0/data:
namenode clusterID = CID-f434ad24-f2fd-4844-a88c-84ce903fb7c2; 
datanode clusterID = CID-dcbe1dd3-085f-4f30-8acb-3e89f4cb1a4f
```

**原因**

datanode的clusterID 和 namenode的clusterID 不匹配。

**解决办法**
根据日志中的路径，cd /home/hadoop/tmp/dfs，能看到 data和name两个文件夹，将name/current下的VERSION中的clusterID复制到data/current下的VERSION中，覆盖掉原来的clusterID，让两个保持一致，然后重启，启动后执行jps，查看进程。

```shell
20131 SecondaryNameNode
20449 NodeManager
19776 NameNode
21123 Jps
19918 DataNode
20305 ResourceManager
```

**总结**

出现该问题的原因

在第一次格式化dfs后，启动并使用了hadoop，后来又重新执行了格式化命令（hdfs namenode -format)，这时namenode的clusterID会重新生成，而datanode的clusterID 保持不变。datanode没有启动，将hadoop配置文件修改后，重新格式化了hadoop集群，即hadoop name -format，但是此时发现slave节点没有启动datanode
解决方法如下：

1. 先执行stop-all.sh暂停所有服务
2. 将所有Salve节点上的tmp(即 hdfs-site.xml 中指定的 dfs.data.dir 文件夹，DataNode存放数据块的位置)、 logs 文件夹删除 ， 然后重新建立tmp , logs 文件夹
3. 将所有Salve节点上的/usr/hadoop/conf下的core-site.xml删除，将master节点的core-site.xml文件拷贝过来，到各个Salve节点
scp /usr/hadoop/conf/core-site.xml hadoop@slave1:/usr/hadoop/conf/
4. 重新格式化: hadoop namenode -format
5. 启动：start-all.sh

hadoop中启动namenode等出现的一些问题
1、先运行stop-all.sh
2、格式化namdenode，不过在这之前要先删除原目录，即core-site.xml下配置的<name>hadoop.tmp.dir</name>所指向的目录，删除后切记要重新建立配置的空目录，然后运行hadoop namenode -format
3、运行start-all.sh

总而言之，如果要刷新，那么先将tmp name logs data 文件删除，再进行format，重启
