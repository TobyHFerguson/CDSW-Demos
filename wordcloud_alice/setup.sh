#!/bin/bash

wget http://www.umich.edu/~umfandsf/other/ebooks/alice30.txt -P /tmp
hdfs dfs -put /tmp/alice30.txt /tmp/
mkdir -p wordcloud_alice/resources
wget "http://www.stencilry.org/stencils/movies/alice%20in%20wonderland/255fk.jpg?p=*full-image" -O wordcloud_alice/resources/alice-mask.jpg

pip install -r wordcloud_alice/requirements.txt
