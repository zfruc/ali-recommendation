# 文档
## flink-ai-extends
- 下载源代码，编译[参考](https://github.com/alibaba/flink-ai-extended)
### Setup

**Requirements**
1. python: 2.7 future support python 3
2. pip
3. cmake >= 3.6
4. java 1.8
5. maven >= 3.3.0
6. hbase 1.4.9
7. kafka 2.11
8. zookeeper 3.5.5

**Install python2**

macOS
```shell
/usr/bin/ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"
export PATH="/usr/local/bin:/usr/local/sbin:$PATH"
brew install python@2 
```
Ubuntu
```shell
sudo apt install python-dev
```

**Install pip**

```shell 
curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py
python get-pip.py
```

Ubuntu you can install with command:
```shell
sudo apt install python-pip
```

**Install pip dependencies**
Install the pip package dependencies (if using a virtual environment, omit the --user argument):
```shell
pip install -U --user pip six numpy wheel mock grpcio grpcio-tools pandas
```
**Install cmake**

cmake version must >= 3.6

[cmake download page](https://cmake.org/download/) 

**Install java 8**

[java download page](http://www.oracle.com/technetwork/java/javase/downloads/index.html)

**Install maven**

maven version >= 3.3.0

[download maven page](http://maven.apache.org/download.cgi)

```shell
tar -xvf apache-maven-3.6.1-bin.tar.gz
mv -rf apache-maven-3.6.1 /usr/local/
```
configuration environment variables
```shell
MAVEN_HOME=/usr/local/apache-maven-3.6.1
export MAVEN_HOME
export PATH=${PATH}:${MAVEN_HOME}/bin
```

**Install hbase**

[hbase_download_page](https://hbase.apache.org/downloads.html)

**Install kafka**

[kafka_download_page](http://kafka.apache.org/downloads)

**Install zookeeper**

[zookeeper_download_page](http://mirror.bit.edu.cn/apache/zookeeper/zookeeper-3.5.5/)

macOS
```shell
brew install zookeeper
```


### Build From Source
**Compiling source code depends on tensorflow 1.11.0. Compiling commands will automatically install tensorflow 1.11.0**

```shell 
mvn -DskipTests=true clean install
```

## 集群运行

###修改com.alibaba.streamcompute.tools.Constants配置为集群配置：

- KAFKA_BOOTSTRAP_VALUE = "[ip]:[port]"

- ZOOKEEPER_QUORUM_VALUE = "[ip]"

### 造数据
#### kafka 
- create topic:
```bash
bin/kafka-topics.sh --create --bootstrap-server [ip:9092] --replication-factor 1 --partitions 1 --topic userclick
``` 

- mvn clean packge
- flink run -c com.alibaba.streamcompute.data.WriteToKafka recommendation-1.0-SNAPSHOT.jar


#### hbase
- create table

```bash
bin/hbase shell
create 'user', 'cf'
create 'item', 'cf'
create 'click', 'cf'
create 'i2i', 'cf'
``` 

- flink run -c com.alibaba.streamcompute.data.CreateUserData recommendation-1.0-SNAPSHOT.jar
- flink run -c com.alibaba.streamcompute.data.CreateItemData recommendation-1.0-SNAPSHOT.jar
- flink run -c com.alibaba.streamcompute.data.WriteClickRecordToHbase recommendation-1.0-SNAPSHOT.jar
- flink run -c com.alibaba.streamcompute.i2i.UpdateI2i recommendation-1.0-SNAPSHOT.jar



### 集群运行 Recommendation.java
#### build tfenv.zip
- [参考](https://github.com/alibaba/flink-ai-extended)
- 运行 flink-ai-extended/docker/flink/create_venv.sh 产生 tfenv.zip
    - 注：14 行结尾 添加 pandas
- 将 tfenv.zip 上传到 hdfs 的 /user/root/tfenv.zip
#### build tf python script
- zip -r code.zip code
- 将 code.zip 上传到 hdfs /user/root/
#### 提交作业
```bash
./bin/flink run -m yarn-cluster -yjm 2048 -ytm 2048 -ys 4 -yn 1 -c com.alibaba.streamcompute.Recommendation   [recommendation-1.0-SNAPSHOT.jar]  --train [prediction.py] --envpath [hdfs://emr-header-1.cluster-133106:9000/user/root/tfenv.zip] --code-path [hdfs://emr-header-1.cluster-133106:9000/user/root/code.zip] --zk-conn-str [emr-worker-2,emr-header-1,emr-worker-1]
``` 


## 本地运行

###修改com.alibaba.streamcompute.tools.Constants配置为本地配置：

- KAFKA_BOOTSTRAP_VALUE = "127.0.0.1:[port]"

- ZOOKEEPER_QUORUM_VALUE = "127.0.0.1"

### 造数据
#### kafka 
- create topic:
```bash
bin/kafka-topics.sh --create --bootstrap-server [localhost:9092] --replication-factor 1 --partitions 1 --topic userclick
``` 
-运行com.alibaba.streamcompute.data.WriteToKafka.java

#### hbase
- create table

```bash
bin/hbase shell
create 'user', 'cf'
create 'item', 'cf'
create 'click', 'cf'
create 'i2i', 'cf'
``` 

- 运行com.alibaba.streamcompute.data.createUserData.java
- 运行com.alibaba.streamcompute.data.createItemData.java 
- 运行com.alibaba.streamcompute.data.WriteClickRecordToHbase.java
- 运行com.alibaba.streamcompute.i2i.UpdateI2i.java


### 本地运行 RecommendationLocal.java

- 运行com.alibaba.streamcompute.RecommendationLocal.java
