Apach Flink Kafka and RollingSink example
=========================================

This is a small Apache Flink streaming job that writes messages from Kafka to local file system (or potentially) HDFS using RollingSink. 
The job runs in IDE and Flink local cluster setup. YARN deployment is not tested. 

The main hurdle was getting the right Kafka connector in Flink and getting the Maven packaging right.

Versions
-------------
This setup was intended to run with:

* Scala 2.10
* Apache Kafka 0.9.0.0
* Apache Flink 0.10.1

TODO
-------------

* Check Scala conflict version

