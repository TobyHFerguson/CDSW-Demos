from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.types import *


conf = SparkConf().setAppName("basketball-analysis")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

# #set up dataframes

dfPlayers = sqlContext.sql("SELECT * from basketball.players")
pdPlayers= dfPlayers.toPandas()

dfAge = sqlContext.sql("SELECT * from basketball.age")
pdAge = dfAge.toPandas()

dfExperience = sqlContext.sql("SELECT * from basketball.experience")
pdExperience = dfExperience.toPandas()

import matplotlib.pyplot as plt
import seaborn as sb

##Let's Look at the distribution of age over the data set
pdAge[["age","valueZ_count"]].plot(kind='bar',x="age",y="valueZ_count")

##player value in years 2016 + 2015
pdPlayers[pdPlayers["year"]==2016][['name','zTOT']].sort_values(by='zTOT',ascending=0)[:25]
pdPlayers[pdPlayers["year"]==2015][['name','zTOT']].sort_values(by='zTOT',ascending=0)[:25]
pdPlayers[pdPlayers["year"]==2016][['name','nTOT']].sort_values(by='nTOT',ascending=0)[:25]
pdPlayers[pdPlayers["year"]==2015][['name','nTOT']].sort_values(by='nTOT',ascending=0)[:25]

##Some statistics on 3 point shooting
pdPlayers[pdPlayers["name"]=='Stephen Curry'][pdPlayers["year"]==2016][['name','zFG','zFT','z3P','zTRB','zAST','zSTL','zBLK','zTOV','zTOT']]
pdPlayers[pdPlayers["year"]==2016][['name','3P','z3P']].sort_values(by="z3P",ascending=0)[:20]
pdPlayers[['name','year','3P','z3P']].sort_values(by="3P",ascending=0)[:20]
pdPlayers[['name','year','3P','z3P']].sort_values(by="z3P",ascending=0)[:20]
pdPlayers[pdPlayers["name"]=='Joe Hassett'][pdPlayers["year"]==1981][['name','zFG','zFT','z3P','zTRB','zAST','zSTL','zBLK','zTOV','zTOT']]
pdPlayers[['year','3PA']].groupby('year').mean().plot(kind='bar')

##Player value by Age
pdAge[["age","valueZ_mean"]].plot(kind='bar',x="age",y="valueZ_mean")
pdAge[["age","valueN_mean"]].plot(kind='bar',x="age",y="valueN_mean")
pdAge[["age","deltaZ_mean"]].plot(kind='bar',x="age",y="deltaZ_mean")
pdAge[["age","deltaN_mean"]].plot(kind='bar',x="age",y="deltaN_mean")

##Player value by Experience
pdExperience[["Experience","valueZ_mean"]].plot(kind='bar',x="Experience",y="valueZ_mean")
pdExperience[["Experience","valueN_mean"]].plot(kind='bar',x="Experience",y="valueN_mean")
pdExperience[["Experience","deltaZ_mean"]].plot(kind='bar',x="Experience",y="deltaZ_mean")
pdExperience[["Experience","deltaN_mean"]].plot(kind='bar',x="Experience",y="deltaN_mean")

##Let's look at some player examples
#Players who fit the general pattern
pdPlayers[pdPlayers["name"] == 'Michael Jordan'][["age","nTOT"]].plot(kind='bar',x='age',y='nTOT')
pdPlayers[pdPlayers["name"] == 'Shaquille O\'Neal'][["age","nTOT"]].plot(kind='bar',x='age',y='nTOT')
pdPlayers[pdPlayers["name"] == 'Allen Iverson'][["age","nTOT"]].plot(kind='bar',x='age',y='nTOT')

#players who don't fit the average pattern
pdPlayers[pdPlayers["name"] == 'Kyle Korver'][["age","nTOT"]].plot(kind='bar',x='age',y='nTOT')
pdPlayers[pdPlayers["name"] == 'Tyreke Evans'][["age","nTOT"]].plot(kind='bar',x='age',y='nTOT')
pdPlayers[pdPlayers["name"] == 'Stephen Curry'][["age","nTOT"]].plot(kind='bar',x='age',y='nTOT')

##The Best Seasons of All time???
pdPlayers[['name','year','age','zTOT']].sort_values(by='zTOT',ascending=0)[:50]
pdPlayers[['name','year','age','nTOT']].sort_values(by='nTOT',ascending=0)[:50]
