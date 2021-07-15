# mapreduce

[ReadFileNotbyMapandReduce](src/main/java/com/example/storageformat/fileread/ReadFileNotbyMapandReduce.java)
在非map reduce过程中读取文件
[MapReduce读文件](https://blog.csdn.net/d2457638978/article/details/81484289)

[InputpathSet](src/main/java/com/example/inputformat/InputpathSet.java)
正则路径读取,setinputformat和addinputformat

[CombineTextInputFormatbyFileName](src/main/java/com/example/inputformat/CombineTextInputFormatbyFileName.java)
类似CombineTextIntputFormat，getsplits是按文件名和文件大小获取

[parquet](src/main/java/com/example/storageformat/parquet/ReadParquet.java)
[hadoop简单读写parquet](https://blog.csdn.net/d2457638978/article/details/83015693) 

MR相关测试方式
1. junit
2. mrunit
3. 在hadoop源码测试的基础上进行
[MapReduce学习写测试](https://blog.csdn.net/d2457638978/article/details/80943886)

[hadoop configuration代码](src/main/java/com/example/configuration/ConfigurationExample.java)
[hadoop configuration](https://blog.csdn.net/d2457638978/article/details/88890609)

