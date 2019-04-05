package br.com.experian.hdp.gdn

import org.apache.spark.sql.SparkSession

object ArquivoSaida {

  def geraArquivoGdn(spark: SparkSession): Unit ={

    spark.sql(
      """
        |insert OVERWRITE table gdn_brazil.wrk_arquivosaidagdn
        |select DISTINCT
        |     'D'
        |    ,' '
        |    ,COALESCE(lpad(cad.nu_documento_cad,8,'0'),' ')
        |    ,cad.rzsc
        |    ,'REG'
        |    ,COALESCE(edt.endereco,' ')
        |    ,COALESCE(edt.bairro,' ')
        |    ,COALESCE(edt.uf,' ')
        |    ,COALESCE(edt.municipio,' ')
        |    ,COALESCE(lpad(edt.cep,8,'0'),' ')
        |    ,'BRA'
        |    ,case
        |        when cast(co_situacaon as int) = 2 then ' '
        |        when cast(co_situacaon as int) = 0 then '1'
        |        when cast(co_situacaon as int) = 4 then '6'
        |        when cast(co_situacaon as int) = 6 then '1'
        |        when cast(co_situacaon as int) = 7 then  '5'
        |        when cast(co_situacaon as int) = 9 then '6'
        |     else ' '
        |     end
        |    ,COALESCE(cad.nmfs,' ')
        |    ,case
        |        when cad.nmfs is null or trim(cad.nmfs)='' then ' '
        |     else 'AKA'
        |     end
        |    ,COALESCE(concat('+55','-',edt.ddd_tel01,'-',edt.num_tel01),' ')
        |    ,' '
        |    ,COALESCE(concat('+55','-',edt.ddd_tel02,'-',edt.num_tel02),' ')
        |    ,COALESCE(concat(lpad(cad.nu_documento_cad,8,'0'),lpad(cad.co_compldoc_cad,4,'0'),lpad(cad.nu_seq,2,'0')),' ')
        |    ,'30'
        |    ,' '
        |    ,' '
        |    ,' '
        |    ,' '
        |    ,' '
        |    ,' '
        |    ,' '
        |    ,' '
        |    ,COALESCE(cad.co_internsite,' ')
        |    ,case
        |         when qds.nman is null or trim(qds.nman)='' then ' '
        |     else COALESCE(qds.cdpm,' ')
        |     end
        |    ,COALESCE(qds.nman,' ')
        |    ,COALESCE(COALESCE(trim(ucase(ct.iso)), substr(qds.cdnahg,1,3)),' ')
        |    ,' '
        |    ,case
        |         when round(cast(concat(substr(qds.petot, 1, length(qds.petot)-1),'.',substr(qds.petot, length(qds.petot),1)) as float)) >=51 then round(cast(concat(substr(qds.petot, 1, length(qds.petot)-1),'.',substr(qds.petot, length(qds.petot),1)) as float))
        |     else 100
        |     end
        |    ,' '
        |    ,from_unixtime(unix_timestamp(current_date()), 'yyyyMMdd')
        |
        |from gdn_brazil.wrk_cadastropj cad
        |    left join gdn_brazil.wrk_enderecotelefone edt
        |        on cad.nu_documento_cad = edt.nu_documento_cad
        |            and cad.co_tipo_doc_cad = edt.co_tipo_doc_cad
        |            and cad.co_compldoc_cad = edt.co_compldoc_cad
        |            and edt.nu_seq_priori='1'
        |    left join gdn_brazil.wrk_quadrosocietario qds
        |        on cad.nu_documento_cad = qds.nu_documento_cad
        |            and cad.co_tipo_doc_cad = qds.co_tipo_doc_cad
        |            and qds.nu_seq_priori='1'
        |    left join gdn_brazil.wrk_countrys ct on trim(ucase(qds.cdnahg)) = trim(ucase(ct.country))
        |where  cad.nu_matriz = cad.co_compldoc_cad
        |
        |
      """.stripMargin)
  }

  def geraArquivoGdnFiliais(spark: SparkSession): Unit = {

    spark.sql(
      """
        |insert OVERWRITE table gdn_brazil.wrk_arquivosaidafil
        |select  DISTINCT
        |     'D'
        |    ,' '
        |    ,COALESCE(lpad(cad.nu_documento_cad,8,'0'),' ') as ndoc
        |    ,cad.rzsc
        |    ,'REG'
        |    ,COALESCE(edt.endereco,' ')
        |    ,COALESCE(edt.bairro,' ')
        |    ,COALESCE(edt.uf,' ')
        |    ,COALESCE(edt.municipio,' ')
        |    ,COALESCE(lpad(edt.cep,8,'0'),' ')
        |    ,'BRA'
        |    ,' '
        |    ,' '
        |    ,' '
        |    ,' '
        |    ,' '
        |    ,' '
        |    ,COALESCE(concat(lpad(cad.nu_documento_cad,8,'0'),lpad(cad.co_compldoc_cad,4,'0'),lpad(cad.nu_seq,2,'0')),' ')
        |    ,'30'
        |    ,' '
        |    ,' '
        |    ,' '
        |    ,' '
        |    ,' '
        |    ,' '
        |    ,' '
        |    ,' '
        |    ,' '
        |    ,' '
        |    ,' '
        |    ,' '
        |    ,' '
        |    ,' '
        |    ,' '
        |    ,from_unixtime(unix_timestamp(current_date()), 'yyyyMMdd')
        |from gdn_brazil.wrk_cadastropj cad
        |
        |inner join gdn_brazil.wrk_enderecotelefone edt
        |   on cad.NU_DOCUMENTO_CAD = edt.NU_DOCUMENTO_CAD
        |     and cad.CO_TIPO_DOC_CAD = edt.CO_TIPO_DOC_CAD
        |     and cad.CO_COMPLDOC_CAD = edt.CO_COMPLDOC_CAD
        |     and edt.NU_SEQ_PRIORI='1'
        |
        |where  cad.NU_MATRIZ <> cad.CO_COMPLDOC_CAD
        |order by ndoc
      """.stripMargin)
  }
}
