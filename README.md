
## Working with Pyspark
Apache Spark is an open source framework that allows to perform large scale real-time data processing at good speed. PySpark is an Python API of Apache Spark  which acts as an interface to work on Spark using Python.It was mostly written in Scala programming language. It runs 100x faster than the Hadoop-Mapreduce with advent of "In-memory processing" and supports real-time computation.It provides robust, fault tolerant data objects named as RDD's that are useful to work on Machine learning and big data based applications.

#### Spark Architecture
The major features of spark that support high speed real-time data processing are the
parallel processing of distributed data, in-memory computation abiltity and lazy evaluations(when a set of 
transformations are applied on data instead of execution, a Directed Acyclic graph[DAG] is generated
in spark and execution is done only when an action is triggered.).
The Spark Architecture follows a Master-Slave architecture where each cluster has
 a Master Node(Driver), multiple worker nodes(slaves) and a cluster manager.
- The Master node has a driver program which schedules the jobs, negotiates with
 cluster manager for allocation of resources.
- After the Spark initalization a Spark context is created by the driver prgram that handles all 
spark functionalities.
- The driver program splits the input data into partitions and distributes it across worker nodes to
process on it.
- A job is split into multiple taks by the SparkContext(in driver program) and tasks are 

#### PySpark Using Colab
To work with PySpark on local machine needs Java, Scala, Py4j library etc and several other software to be installed so instead PySpark on Google Colab is better alternative when the data used is mounted on drive.
#### RDD - Resilient Distributed Datasets
