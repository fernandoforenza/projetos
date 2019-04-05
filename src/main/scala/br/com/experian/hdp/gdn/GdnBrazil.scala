package br.com.experian.hdp.gdn

import java.time.LocalDateTime
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

object GdnBrazil {

  def main(args: Array[String]){

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

    println(">>>>>>>>>>>>>>INICIO DO PROCESSAMENTO DO GDN SEMANAL-> " + LocalDateTime.now() + "<<<<<<<<<<<<<<<<<<<")
    Validation.process() //nada para validar, dados validados na origem mainframe

    println(">>>>>>>>>>>>>>ATUALIZANDO TABELAS DA ÁREA DE WORK " + LocalDateTime.now() + "<<<<<<<<<<<<<<<<<<<")
    Acceptance.writeInHiveTable(spark, "stg_administradores", "wrk_administradores")
    Acceptance.writeInHiveTable(spark, "stg_antecessoras", "wrk_antecessoras")
    Acceptance.writeInHiveTable(spark, "stg_cadastropj", "wrk_cadastropj")
    Acceptance.writeInHiveTable(spark, "stg_conselho", "wrk_conselho")
    Acceptance.writeInHiveTable(spark, "stg_enderecotelefone", "wrk_enderecotelefone")
    Acceptance.writeInHiveTable(spark, "stg_historicoriscopj", "wrk_historicoriscopj")
    Acceptance.writeInHiveTable(spark, "stg_inscricaoestadual", "wrk_inscricaoestadual")
    Acceptance.writeInHiveTable(spark, "stg_participacoes", "wrk_participacoes")
    Acceptance.writeInHiveTable(spark, "stg_quadrosocietario", "wrk_quadrosocietario")
    Acceptance.writeInHiveTable(spark, "stg_filiais", "wrk_filiais")
    Acceptance.writeInHiveTable(spark, "stg_countrys", "wrk_countrys")
    Acceptance.writeInHiveTable(spark, "stg_isic", "wrk_isic")
    Acceptance.writeInHiveTable(spark, "stg_naturezajuridica", "wrk_naturezajuridica")
    Acceptance.writeInHiveTable(spark, "stg_descricaocodserasa", "wrk_descricaocodserasa")
    Acceptance.writeInHiveTable(spark, "stg_sensibiliza", "wrk_sensibiliza")
    Acceptance.writeInHiveTable(spark, "stg_sistcompsm", "wrk_sistcompsm")

    println(">>>>>>>>>>>>>>Enriquecendo dados das tabelas de work-> " + LocalDateTime.now() + "<<<<<<<<<<<<<<<<<<<")
    Pinning.processCadastroPJ(spark)
    Pinning.processSisterCompany(spark)


    println(">>>>>>>>>>>>>>Filtros nas tabelas de natureza juridica e ISIC-> " + LocalDateTime.now() + "<<<<<<<<<<<<<<<<<<<")
    Curation.process(spark)

    println(">>>>>>>>>>>>>>Ingestao no hbase tabela gdn:basona-> " + LocalDateTime.now() + "<<<<<<<<<<<<<<<<<<<")
    Pinning.processHbaseKeys(spark)

    println(">>>>>>>>>>>>>>Ingestao no hbase tabela gdn:basona-> " + LocalDateTime.now() + "<<<<<<<<<<<<<<<<<<<")
    ArquivoSaida.geraArquivoGdn(spark)
    ArquivoSaida.geraArquivoGdnFiliais(spark)

    println(">>>>>>>>>>>>>>TERMINO DO PROCESSAMENTO DO GDN BRAZIL SEMANAL-> " + LocalDateTime.now() + "<<<<<<<<<<<<<<<<<<<")

  }
}
