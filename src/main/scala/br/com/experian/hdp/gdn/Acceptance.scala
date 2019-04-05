package br.com.experian.hdp.gdn

import org.apache.spark.sql.{DataFrame, SparkSession}

object Acceptance {

  // CONVERTENDO OS ARQUIVOS PARA PARQUET PARA MELHORAR ARMAZENAMENTO E PERFORMANCE NO PROCESSAMENTO, DEPENDE DO FORMATO DO DDL DA TABELA DA WORK VER SCRIPTS HQL
    def writeInHiveTable(sparkSession: SparkSession,tabelaStage: String, tabelaWork:String): Unit = {
    sparkSession.sql("insert overwrite table gdn_brazil."+tabelaWork+" select * from gdn_brazil."+tabelaStage)
  }

}
