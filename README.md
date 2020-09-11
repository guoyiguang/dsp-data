===


目录
===

* [elasticsearch_to_mysql](campaign_num.py)
>主要功能为将elasticsearch 中的数据按照多个字段进行聚合后，重新插入到mysql中。
* [logstash_conf](logstash/sbs_imp_log_s3.conf)
>主要功能是logstash处理日志，然后将日志存储到s3上。结果分为正常和异常不同目录
* [add_partition](logstash/add_partitions.py)
> 当longstash 以'\t'格式存储到s3的时候 在athena上建立的外部表 需要手动加载分区。
* [athena](athena_to_mysql.py)
>主要功能为查询athena中的数据 将不同的表进行聚合后将数据存储到mysql中