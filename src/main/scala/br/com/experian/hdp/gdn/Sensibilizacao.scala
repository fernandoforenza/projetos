package br.com.experian.hdp.gdn

import java.time.LocalDateTime

import org.apache.spark.sql.SparkSession

object Sensibilizacao {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .master("yarn")
      .appName("br.com.experian.hdp.gdn.GdnBrazilSemanal")
      .enableHiveSupport()
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .config ("spark.debug.maxToStringFields", 10000)
      .getOrCreate()

    spark.catalog.clearCache()
    spark.catalog.setCurrentDatabase("gdn_brazil")

    import spark.implicits._

    println(">>>>>>>>>>>>>>INICIO DO PROCESSAMENTO DO GDN BRAZIL SENSIBILIZACAO-> " + LocalDateTime.now() + "<<<<<<<<<<<<<<<<<<<")
    Validation.process() //nada para validar, dados validados na origem mainframe

    println(">>>>>>>>>>>>>>ATUALIZANDO TABELAS DA ÁREA DE WORK " + LocalDateTime.now() + "<<<<<<<<<<<<<<<<<<<")
    Acceptance.writeInHiveTable(spark, "stg_sensibilizacao", "wrk_sensibilizacao")

    println(">>>>>>>>>>>>>>ATUALIZANDO TABELA SENSIBILIZACAO DO HBASE " + LocalDateTime.now() + "<<<<<<<<<<<<<<<<<<<")
    val DfAdminist = spark.sql(
      """
        |  select concat(tp_doc,nu_doc) as key
        |  , a.* from gdn_brazil.wrk_sensibiliza a
      """.stripMargin)
    DataFrameToHbase.insertInto(DfAdminist,"gdn_brazil:basona","SENSIBILIZA","key",10000)

    println(">>>>>>>>>>>>>>TERMINO DO PROCESSAMENTO DO GDN BRAZIL SENSIBILIZACAO-> " + LocalDateTime.now() + "<<<<<<<<<<<<<<<<<<<")
  }

}
