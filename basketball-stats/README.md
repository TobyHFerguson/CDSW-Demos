# BasketballStats
Basketball Statistics Demo
Created by Jordan Volz (jordan.volz@cloudera.com)

<b>Status</b>: Demo Ready
<b>Use Case</b>: End-to-end spark workflow: data processing, ad-hoc analytics, and predictive analytics

<b>Steps</b>:
1. Open a terminal and run setup.sh
2. Create a Scala Session and run data-processing.scala
3. Create a Python Session and run analysis.py
4. Return to the Scala Session and run machine-learning.scala
5. When finished, open a terminal and run cleanup.sh

<b>Recommended Session Sizes</b>: 4 CPU, 8 GB RAM
Raw stats are in /data. 

<b>Notes</b>: 
1. Raw stats are in /data
2. data-processing.scala --> data transformations + table creations
3. analysis.py --> ad-hoc analysis with pandas
4. machine-learning.scala --> Regression analysis with spark mllib
5. Your user will need write access into Hive. 

<b>Demo Script</b>
http://github.mtv.cloudera.com/foe/BasketballStats/blob/master/BasketballStatsDemoScript.docx

<b>Related Content</b>:
http://blog.cloudera.com/blog/2016/06/how-to-analyze-fantasy-sports-using-apache-spark-and-sql/ 
http://blog.cloudera.com/blog/2016/06/how-to-analyze-fantasy-sports-with-apache-spark-and-sql-part-2-data-exploration/
