Apach Flink Kafka and RollingSink example
=========================================

This is a small Apache Flink streaming job that writes messages from Kafka to local file system (or potentially) HDFS using RollingSink. 
The job runs in IDE and Flink local cluster setup. YARN deployment is not tested. 

Versions
-------------
This setup was intended to run with:

* Scala 2.10
* Apache Kafka 0.9.0.0
* Apache Flink 0.10.1

TODO
-------------

* Check Scala 2.10/11 version conflict
* Packaging: is it better to include the complete compile path or just the necessary libraries using trail and error?

