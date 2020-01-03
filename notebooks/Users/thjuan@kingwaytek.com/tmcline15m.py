# Databricks notebook source
#讀資料
import pyspark.sql.functions as func
from pyspark.sql.window import Window
from pyspark.sql.functions import udf, col,abs,expr, when
from pyspark.sql.types import *
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import Row


roaddata=sqlContext.sql("""
select * from tbroaddata where kind<8
""")

roaddata.registerTempTable("dfroad")

spark.conf.set(
 "fs.azure.account.key.storage0tmcdata.blob.core.windows.net",
 "YEUc30NKB2mEkwrTbj93kufyfKJJ7HM3eTMLP1nocDzyOE3/8zX+eseR52asoxmtvkP/V2kVC7a9DxyzlOU8tw==")


dfSchema =StructType([ \
StructField("hwkey", StringType(), True),StructField("secdiff", IntegerType(), True) \
,StructField("speed", IntegerType(), True),StructField("vtype", StringType(), True) \
,StructField("roadid", StringType(), True),StructField("next_roadid", StringType(), True) \
, StructField("epoch", IntegerType(), True), StructField("weekday", IntegerType(), True) \
, StructField("fiveminute_count_of_day", IntegerType(), True), StructField("tmcspeed", IntegerType(), True) ])


dfline = sqlContext.read \
    .format('com.databricks.spark.csv') \
    .options(header='true') \
    .load('wasbs://tmcline@storage0tmcdata.blob.core.windows.net/2019/{01, 02, 03, 04, 05, 06, 07, 08, 09, 10, 11}/*', schema=dfSchema)

dfline.registerTempTable("tbline")



# COMMAND ----------

#移除連續假期期間，建立15m欄位

dffilter=sqlContext.sql("""
select df.hwkey, df.secdiff, df.speed, df.vtype, df.roadid, df.next_roadid, df.epoch, case when df.weekday=0 then 7 else df.weekday end as  weekday,
df.fiveminute_count_of_day, ceiling(df.fiveminute_count_of_day/3) as quarter_count_of_day, df.tmcspeed 
from tbline df
join dfroad as r 
on r.RoadID = abs(df.roadid)
where df.roadid<>'' and df.hwkey<>'' and df.speed>0 and r.kind<8 and ( (df.epoch>1546358399 AND df.epoch<1549036800) OR (df.epoch>1549814399 AND df.epoch<1551283200) OR (df.epoch>1551628799 AND df.epoch<1554307200) OR (df.epoch>1554652799 AND df.epoch<1556640000) OR (df.epoch>1556726399 AND df.epoch<1559836800 ) OR (df.epoch>1560095999 AND df.epoch<1568304000) OR (df.epoch>1568563199 AND df.epoch<1570636800) OR df.epoch>1570982399 )
""")


#調整錯誤秒數→將大車隊回傳的ms調成s

#dffilter=dffilter.withColumn('next_epoch',func.lead(col('epoch')).over(Window.partitionBy("hwkey").orderBy("epoch")))\
#.withColumn('epoch_diff',col('next_epoch')-col('epoch'))\
#.withColumn('lag_epochdiff', func.lag(col('epoch_diff')).over(Window.partitionBy("hwkey").orderBy("epoch")))\
#.withColumn('lag_secdiff', func.lag(col('secdiff')).over(Window.partitionBy("hwkey").orderBy("epoch")))\

dffilter.registerTempTable("tbfilter")

#dftimeadj=sqlContext.sql("""
#SELECT hwkey, secdiff, speed, vtype, roadid, next_roadid, epoch, weekday, fiveminute_count_of_day, epoch_diff,
#quarter_count_of_day, tmcspeed, 
#CASE WHEN epoch_diff IS NOT NULL THEN 
#  CASE WHEN secdiff!=epoch_diff THEN round(secdiff/1000,0) ELSE secdiff END
#ELSE 
#  CASE WHEN lag_secdiff!=lag_epochdiff THEN round(secdiff/1000,0) ELSE secdiff END
#END as secadj
#FROM tbfilter
#""")

#dftimeadj.registerTempTable("tbtimeadj")




# COMMAND ----------


tmclineRaw = dffilter.withColumn("epoch_8", (col("epoch") + 28800).cast('timestamp'))

tmclineRaw = tmclineRaw.withColumn("epoch8_hour", func.hour(col("epoch_8")))
tmclineRaw = tmclineRaw.withColumn("epoch8_minute", func.minute(col("epoch_8")))
tmclineRaw = tmclineRaw.withColumn('epoch8_weekday', (func.date_format('epoch_8', 'u')).cast("integer") )

tmclineRaw.registerTempTable("tblineraw")

# COMMAND ----------

dffilter.write.parquet("tmcline_2019020304.parquet")

# COMMAND ----------

#讀資料
import pyspark.sql.functions as func
from pyspark.sql.window import Window
from pyspark.sql.functions import udf, col,abs,expr, when
from pyspark.sql.types import *
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import Row
from datetime import datetime




# COMMAND ----------


tmclineRaw = sqlContext.read.parquet("/tmcline_2019020304.parquet/*")


tmclineRaw = tmclineRaw.withColumn("epoch_8", (col("epoch") + 28800).cast('timestamp'))

tmclineRaw = tmclineRaw.withColumn("epoch8_hour", func.hour(col("epoch_8")))
tmclineRaw = tmclineRaw.withColumn("epoch8_minute", func.minute(col("epoch_8")))
tmclineRaw = tmclineRaw.withColumn('epoch8_weekday', (func.date_format('epoch_8', 'u')).cast("integer") )

tmclineRaw.registerTempTable("tblineraw")


# COMMAND ----------


pqtmcline=sqlContext.sql("""
select df.hwkey, df.secdiff, df.speed, df.vtype, df.roadid, df.next_roadid,df.epoch, df.epoch8_weekday  as  weekday,
floor((df.epoch8_hour*60+epoch8_minute)/15) as quarter_count_of_day, df.tmcspeed 
from tblineraw df
""")


pqtmcline.registerTempTable("tbfilterpq")

# COMMAND ----------


#建立權重欄位
dfdwts=sqlContext.sql("""
SELECT f.roadid AS wroadid, f.weekday AS wweekday  , f.quarter_count_of_day AS wquarter_count_of_day , (86400/abs(unix_timestamp()-f.epoch)) AS wtimediff ,
(86400/abs(unix_timestamp()-f.epoch))*f.speed AS wweight ,f.speed AS wspeed , avg.avg AS Aavg
FROM tbfilterpq f
JOIN
  (SELECT roadid, weekday, quarter_count_of_day, AVG(speed) AS avg FROM tbfilterpq
  GROUP BY roadid, weekday, quarter_count_of_day) avg 
ON (f.roadid=avg.roadid AND f.weekday=avg.weekday AND f.quarter_count_of_day=avg.quarter_count_of_day)
WHERE f.speed>= avg.avg
 """)

dfdwts.registerTempTable("tbdwts")

# COMMAND ----------


#依照roadid、weekday、15m區間分群
groupbyroadtime=sqlContext.sql("""
SELECT wroadid, wweekday, wquarter_count_of_day ,SUM(wweight)/SUM(wtimediff) AS wvalue, COUNT(*) AS c , std(wspeed) AS std, AVG(wspeed) as avg
FROM tbdwts
GROUP BY wroadid, wweekday, wquarter_count_of_day
 """)

groupbyroadtime=groupbyroadtime.where(col("c")>1)

groupbyroadtime.registerTempTable("groupbyroadtime")



# COMMAND ----------



spark.conf.set(
  "fs.azure.account.key.storage0tmcdata.blob.core.windows.net",
  "YEUc30NKB2mEkwrTbj93kufyfKJJ7HM3eTMLP1nocDzyOE3/8zX+eseR52asoxmtvkP/V2kVC7a9DxyzlOU8tw==")


#計算統計資料
w7=sqlContext.sql("""
select  ori.wroadid as roadid , ori.wweekday as  weekday , ori.wquarter_count_of_day as quarter_count_of_day 
,(ori.wvalue*ori.c+nvl(plusa.wvalue,0)*nvl(plusa.c,0)+nvl(minusa.wvalue,0)*nvl(minusa.c,0))/(ori.c+nvl(plusa.c,0)+nvl(minusa.c,0)) as speed
from 
groupbyroadtime ori left join groupbyroadtime plusa on ori.wroadid=plusa.wroadid 
and ori.wweekday=plusa.wweekday and ori.wquarter_count_of_day=(plusa.wquarter_count_of_day-1) 
left join groupbyroadtime minusa 
on ori.wroadid=minusa.wroadid 
and ori.wweekday=minusa.wweekday and ori.wquarter_count_of_day=(minusa.wquarter_count_of_day+1) 
where ori.wweekday = 7
""")

w7.write.mode("overwrite").csv('wasbs://tmclinehistory@storage0tmcdata.blob.core.windows.net/w7/')


# COMMAND ----------

  
w1.write.mode("overwrite").csv('wasbs://tmclinehistory@storage0tmcdata.blob.core.windows.net/w1/')

# COMMAND ----------

all.registerTempTable("tball")

# COMMAND ----------

#存入blob
all.write.mode("overwrite").parquet("/tmclineResult_1121/tmcline_2019020304.parquet")

# COMMAND ----------

tmcline_std30c3 = sqlContext.read.parquet("/tmclineResult/tmcline_2019020304_r1.parquet/*")
display(tmcline_std30c3)

# COMMAND ----------


tmclineCmp = sqlContext.sql("""
SELECT r.roadid, r.speed AS rSpeed, r.weekday, r.quarter_count_of_day, a.speed AS aSpeed, a.std, a.avg
FROM tbRaw r
LEFT JOIN tball a
ON r.roadid=a.roadid and r.weekday=a.weekday and r.quarter_count_of_day=a.quarter_count_of_day
WHERE a.speed IS NOT null
""")

# COMMAND ----------

display(tmclineCmp.where(col("std")>10))

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r /tmcSpatial/

# COMMAND ----------

# MAGIC %fs
# MAGIC ls

# COMMAND ----------

