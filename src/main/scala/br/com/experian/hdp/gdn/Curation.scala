package br.com.experian.hdp.gdn

import org.apache.spark.sql.SparkSession

object Curation {

  def process(spark: SparkSession): Unit = {

    def filterTable(tabelaOrigem: String,colunas: String, filtro: String, valor: String, tabelaDestino: String): Unit = {
      spark.sql(
        "insert OVERWRITE table "
          + tabelaDestino
          + " select "+colunas+" from gdn_brazil."
          + tabelaOrigem
          + " where "
          + filtro
          + "='"
          + valor
          + "'"
      )
    }

    //filtrando todas as colunas para a tabela participada
    filterTable("wrk_participacoes","*", "col12", "010501", "wrk_participada")

    //filtrando somente as colunas necessarias para a tabela de participantes
    val colunas = "col01, col02, col03, col04, col05, col06, col07, col08, col09, col10, col11, col12, col13, col14, col15, col16, col17, col18, col19, col20, col21, col22, col23, col24, col25, col26, col27, col28, col29"
    filterTable("wrk_participacoes",colunas, "col12", "010502", "wrk_participantes")
  }


}
