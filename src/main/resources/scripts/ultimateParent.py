#!/usr/bin/env python
from sys import exit
import pandas as pd
from numpy import nan
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("yarn") \
    .appName("br.com.experian.hdp.gdn.ultimateParent") \
    .config("hive.exec.dynamic.partition", "true") \
    .config("hive.exec.dynamic.partition.mode", "nonstrict") \
    .getOrCreate()


def derive_ancestor(df1):
    id = dict(zip(df1['NU_DOCUMENTO_CAD'], df1['AncestorID']))

    def ancestor(k, l):
        if k not in id or (id[k] is nan or id[k] == k or k in l):
            return k
        else:
            l.append(k)
            return ancestor(id[k], l)

    return [ancestor(s, []) for s in df1['NU_DOCUMENTO_CAD']]


print("selecionando quadrosocietario e criando dataframe ")
df = spark.sql(
    "select b.CDPM AncestorID ,a.NU_DOCUMENTO_CAD  from gdn_brazil.wrk_quadrosocietario a, gdn_brazil.wrk_quadrosocietario b where a.iddk = 'J'and a.petot > 500 and a.cdpm = b.cdpm and a.nu_documento_cad = b.nu_documento_cad and a.petot = b.petot  group by a.nu_documento_cad,b.cdpm ")

print ("convertendo dataframe para pandas")
dfpandas = df.toPandas()

print("criando a coluna CDPM")
dfpandas.insert(2, 'CDPM', derive_ancestor(dfpandas))

spark_df = spark.createDataFrame(dfpandas)

spark_df.write.mode("overwrite").saveAsTable("gdn_brazil.wrk_tbultimate")

spark.stop()
exit()
