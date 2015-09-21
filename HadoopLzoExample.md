TryingLzoWithSpark
==================

Running a example of reading a Lzo file from Spark using Intellij Idea. 



###Pre-conditions

- Have Hadoop hdfs up and running locally.
- Have hadoop (https://github.com/twitter/hadoop-lzo) builded. And generate native libs:
(/path/to/hadoop-lzo/target/native/Linux-amd64-64/lib)
Anyway in this project you can find this libraries in: src/main/resources/hadoop-lzo/native/Linux-amd64-64



###Hdfs configs

hadoop-env.sh should contain this at the end of file:
	export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:/path/to/hadoop-lzo-0.4.19.jar
	export JAVA_LIBRARY_PATH=/path/to/hadoop-lzo/target/native/Linux-amd64-64/lib:/path/to/hadoop-2.6.0/lib/native


core-site.xml should contain:

  <property>
    <name>io.compression.codecs</name>
    <value>com.hadoop.compression.lzo.LzoCodec,com.hadoop.compression.lzo.LzopCodec</value>
  </property>
  <property>
    <name>io.compression.codec.lzo.class</name>
    <value>com.hadoop.compression.lzo.LzoCodec</value>
  </property>



### Putting a example file in hdfs
	hadoop dfs -mkdir /input
	hadoop dfs -put customers.txt.lzo /input


### Generate index for file using hadoop-lzo
	hadoop jar /path/to/hadoop-lzo-0.4.19.jar com.hadoop.compression.lzo.LzoIndexer /input/customers.txt.lzo

###Checking:
	hdfs dfs -ls /input

Must See something like this:
``` -rw-r--r--   1 marcos supergroup  171236594 2015-09-21 18:47 /input/customers.txt.lzo
    -rw-r--r--   1 marcos supergroup      10768 2015-09-21 18:50 /input/customers.txt.lzo.index
```




### Running example from Intellij Idea:

Edit configurations for the example:

VM Options:
	-Djava.library.path=src/main/resources/hadoop-lzo/native/Linux-amd64-64



