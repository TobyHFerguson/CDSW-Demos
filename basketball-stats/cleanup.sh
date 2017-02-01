#drop old tables
beeline -u "jdbc:hive2://<host:>10000/default" -e "DROP Table if exists players"
beeline -u "jdbc:hive2://<host>:10000/default" -e "DROP Table if exists age"
beeline -u "jdbc:hive2://<host>:10000/default" -e "DROP Table if exists experience"
beeline -u "jdbc:hive2://<host>:10000/default" -e "DROP Table if exists similar"

#delete old data from hdfs
hadoop fs -rm -r -f /tmp/BasketballStatsWithYear/
hadoop fs -rm -r -f /tmp/BasketballStats/
hadoop fs -rm  -r -f /user/hive/warehouse/players/
hadoop fs -rm  -r -f /user/hive/warehouse/age/
hadoop fs -rm  -r -f /user/hive/warehouse/experience/
hadoop fs -rm  -r -f /user/hive/warehouse/similar/