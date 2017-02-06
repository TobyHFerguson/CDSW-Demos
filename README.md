# CDSW-Demos

Add new demos as directories in this repository. Recommended structure of directories is as follows: 

1) readme.md - markup that provides an overview of the demo, explains the order to run scripts, recommnedations for building jobs, etc. 

2) /data - stores any data needed for the demo. Should be small/medium sized data sets. 

3) setup.sh - script that runs any setup commands prior to running main CDSW scripts. This is stuff like pulling down large data sets, loading data into HDFS, etc. 

4) <scripts>.<py/r/scala> - main CDSW scripts. This is where a bulk of the demo lives. Good commenting is encouraged to help people in the field demo. 

5) cleanup.sh - script that cleans out the system so <scripts>.<py/r/scala> can be run again w/o error. 

6) spark-defaults.conf - recommend spark settings to be used by CDSW
