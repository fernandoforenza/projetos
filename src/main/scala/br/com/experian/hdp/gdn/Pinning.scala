package br.com.experian.hdp.gdn

import org.apache.spark.sql.{DataFrame, SparkSession}

object Pinning {

  def processCadastroPJ(spark: SparkSession): Unit = {

    spark.sql(
      """
        |insert OVERWRITE table gdn_brazil.wrk_cadastropjenriquecida
        |select
        |       nu_documento_cad,
        |       co_tipo_doc_cad,
        |       co_compldoc_cad,
        |       nu_seq,
        |       co_tipo_reg,
        |       nu_ordem_exib,
        |       nu_seq_priori,
        |       co_valido,
        |       nu_nsu_cen,
        |       dt_inicio,
        |       dt_final,
        |       cdmt,
        |       rzsc,
        |       dtutmf,
        |       hrutmf,
        |       totqtvlcv,
        |       topeaocv,
        |       cdog,
        |       cdtpsc,
        |       dtutrg,
        |       nrutrg,
        |       cdsa,
        |       cdsgsa,
        |       cpsc,
        |       cpra,
        |       dtfdcg,
        |       cpar,
        |       nreg,
        |       dtblgf,
        |       qtvlnu,
        |       cdcrao,
        |       cdim,
        |       cdep,
        |       nmlf,
        |       nrtflf,
        |       nmfs,
        |       cdrcfe,
        |       cpaz,
        |       cdttii,
        |       cddalf,
        |       peepvd,
        |       peimcf,
        |       cder,
        |       nmgrec,
        |       toqtvlsv,
        |       cdrblf,
        |       cdna,
        |       qtvlnonu,
        |       topeaosv,
        |       cdstjtem,
        |       cdat,
        |       cdstcg,
        |       dtatrz,
        |       dtatpo,
        |       e3v2,
        |       dtgsgp,
        |       flfmcn,
        |       dtfg,
        |       cdrcfen,
        |       dtininoramo,
        |       cdinstsede,
        |       nu_nire,
        |       co_sgrcfen,
        |       co_ctcapit,
        |       co_origem_cap,
        |       co_relviscdc,
        |       nu_sq_ultaltco,
        |       co_incdqcadpjb,
        |       co_ind_ativsoc,
        |       co_sitjunta,
        |       nu_inscrest,
        |       nu_inscrmun,
        |       co_proc_at,
        |       dt_inscreceit,
        |       dt_cadastro,
        |       co_ind_filial,
        |       co_ativserasa,
        |       nu_matriz,
        |       co_opc_simples,
        |       dt_inf_simples,
        |       co_situacaon,
        |       dt_indoperac,
        |       co_indempmei,
        |       co_situacao,
        |       co_naturrec,
        |       dt_alterarf,
        |       qt_filiarf,
        |       co_opcsimei,
        |       dt_sitsimei,
        |       dt_sitsimpl,
        |       co_ativrecv,
        |       co_cnaesec1,
        |       co_cnaesec2,
        |       co_cnaesec3,
        |       co_cnaesec4,
        |       co_cnaesec5,
        |       co_cnaesec6,
        |       co_cnaesec7,
        |       co_cnaesec8,
        |       co_cnaesec9,
        |       co_cnaesec10,
        |       co_natur_recn_emp,
        |       dt_situac_emp,
        |       email,
        |       co_internmail,
        |       co_internsite,
        |       de_opsimei_ant,
        |       porte,
        |       ds_porte,
        |       dtemisficha,
        |       vl1conta,
        |       dt1_balanco,
        |       vl2conta,
        |       dt2_balanco,
        |       vl3conta,
        |       dt3_balanco,
        |       vl4conta,
        |       dt4_balanco,
        |       vl5conta,
        |       dt5_balanco,
        |       ipalerta,
        |       gfclient,
        |       co_grpecon,
        |       no_grpoecon,
        |       co_vincasso,
        |       controlagrp,
        |       flagneg,
        |       neg_flgprot,
        |       neg_qtdprot,
        |       neg_flgacao,
        |       neg_qtdacao,
        |       neg_flgfale,
        |       neg_qtdfale,
        |       neg_flgconc,
        |       neg_qtdconc,
        |       neg_flgrefi,
        |       neg_qtdrefi,
        |       neg_flgache,
        |       neg_qtdache,
        |       neg_flgccf,
        |       neg_qtdccf,
        |       neg_flgpefi,
        |       neg_qtdpefi,
        |       neg_flgconv,
        |       neg_qtdconv,
        |       neg_cheque,
        |       neg_qtdcheq,
        |       codisic,
        |       isic,
        |       descricaoport,
        |       descricaoingles,
        |       indicadorsfl,
        |       codigoserasapadrao,
        |       novocodigoserasa,
        |       descricaocodigoserasa_port,
        |       descricaoresumidacodserasa_port,
        |       descricaocodigoserasa_eng,
        |       descricaoresumidacodserasa_eng,
        |       cnaedescricao ,
        |       ultimateparent,
        |       totalparticipacao
        |from
        |(
        |select
        |       row_number() over(partition by concat(c.nu_documento_cad, c.nu_matriz, c.co_compldoc_cad)
        |                    order by concat(c.nu_documento_cad, c.nu_matriz, c.co_compldoc_cad)  asc) cod,
        |       c.nu_documento_cad,
        |       c.co_tipo_doc_cad,
        |       c.co_compldoc_cad,
        |       c.nu_seq,
        |       c.co_tipo_reg,
        |       c.nu_ordem_exib,
        |       c.nu_seq_priori,
        |       c.co_valido,
        |       c.nu_nsu_cen,
        |       c.dt_inicio,
        |       c.dt_final,
        |       c.cdmt,
        |       c.rzsc,
        |       c.dtutmf,
        |       c.hrutmf,
        |       c.totqtvlcv,
        |       c.topeaocv,
        |       c.cdog,
        |       c.cdtpsc,
        |       c.dtutrg,
        |       c.nrutrg,
        |       c.cdsa,
        |       c.cdsgsa,
        |       c.cpsc,
        |       c.cpra,
        |       c.dtfdcg,
        |       c.cpar,
        |       c.nreg,
        |       c.dtblgf,
        |       c.qtvlnu,
        |       c.cdcrao,
        |       c.cdim,
        |       c.cdep,
        |       c.nmlf,
        |       c.nrtflf,
        |       c.nmfs,
        |       c.cdrcfe,
        |       c.cpaz,
        |       c.cdttii,
        |       c.cddalf,
        |       c.peepvd,
        |       c.peimcf,
        |       c.cder,
        |       c.nmgrec,
        |       c.toqtvlsv,
        |       c.cdrblf,
        |       c.cdna,
        |       c.qtvlnonu,
        |       c.topeaosv,
        |       c.cdstjtem,
        |       c.cdat,
        |       c.cdstcg,
        |       c.dtatrz,
        |       c.dtatpo,
        |       c.e3v2,
        |       c.dtgsgp,
        |       c.flfmcn,
        |       c.dtfg,
        |       c.cdrcfen,
        |       c.dtininoramo,
        |       c.cdinstsede,
        |       c.nu_nire,
        |       c.co_sgrcfen,
        |       c.co_ctcapit,
        |       c.co_origem_cap,
        |       c.co_relviscdc,
        |       c.nu_sq_ultaltco,
        |       c.co_incdqcadpjb,
        |       c.co_ind_ativsoc,
        |       c.co_sitjunta,
        |       c.nu_inscrest,
        |       c.nu_inscrmun,
        |       c.co_proc_at,
        |       c.dt_inscreceit,
        |       c.dt_cadastro,
        |       c.co_ind_filial,
        |       c.co_ativserasa,
        |       c.nu_matriz,
        |       c.co_opc_simples,
        |       c.dt_inf_simples,
        |       c.co_situacaon,
        |       c.dt_indoperac,
        |       c.co_indempmei,
        |       c.co_situacao,
        |       c.co_naturrec,
        |       c.dt_alterarf,
        |       c.qt_filiarf,
        |       c.co_opcsimei,
        |       c.dt_sitsimei,
        |       c.dt_sitsimpl,
        |       c.co_ativrecv,
        |       c.co_cnaesec1,
        |       c.co_cnaesec2,
        |       c.co_cnaesec3,
        |       c.co_cnaesec4,
        |       c.co_cnaesec5,
        |       c.co_cnaesec6,
        |       c.co_cnaesec7,
        |       c.co_cnaesec8,
        |       c.co_cnaesec9,
        |       c.co_cnaesec10,
        |       c.co_natur_recn_emp,
        |       c.dt_situac_emp,
        |       c.email,
        |       c.co_internmail,
        |       c.co_internsite,
        |       c.de_opsimei_ant,
        |       c.porte,
        |       c.ds_porte,
        |       c.dtemisficha,
        |       c.vl1conta,
        |       c.dt1_balanco,
        |       c.vl2conta,
        |       c.dt2_balanco,
        |       c.vl3conta,
        |       c.dt3_balanco,
        |       c.vl4conta,
        |       c.dt4_balanco,
        |       c.vl5conta,
        |       c.dt5_balanco,
        |       c.ipalerta,
        |       c.gfclient,
        |       c.co_grpecon,
        |       c.no_grpoecon,
        |       c.co_vincasso,
        |       c.controlagrp,
        |       c.flagneg,
        |       c.neg_flgprot,
        |       c.neg_qtdprot,
        |       c.neg_flgacao,
        |       c.neg_qtdacao,
        |       c.neg_flgfale,
        |       c.neg_qtdfale,
        |       c.neg_flgconc,
        |       c.neg_qtdconc,
        |       c.neg_flgrefi,
        |       c.neg_qtdrefi,
        |       c.neg_flgache,
        |       c.neg_qtdache,
        |       c.neg_flgccf,
        |       c.neg_qtdccf,
        |       c.neg_flgpefi,
        |       c.neg_qtdpefi,
        |       c.neg_flgconv,
        |       c.neg_qtdconv,
        |       c.neg_cheque,
        |       c.neg_qtdcheq,
        |       i.codisic,
        |       i.isic,
        |       n.descricaoport,
        |       n.descricaoingles,
        |       n.indicadorsfl,
        |       d.codigoserasapadrao,
        |       d.novocodigoserasa,
        |       d.descricaocodigoserasa descricaocodigoserasa_port,
        |       d.descricaoresumidacodserasa descricaoresumidacodserasa_port,
        |       e.descricaocodigoserasa descricaocodigoserasa_eng,
        |       e.descricaoresumidacodserasa descricaoresumidacodserasa_eng,
        |       i.cnae cnaedescricao ,
        |       u.cdpm ultimateparent,
        |       c.totalparticipacao
        |
        |       from gdn_brazil.wrk_cadastropj c
        |       left join gdn_brazil.wrk_isic i on c.CDRCFEN = i.codcnae
        |       left join gdn_brazil.wrk_naturezajuridica n on c.CO_NATUR_RECN_EMP = n.codreceita
        |       left join gdn_brazil.wrk_descricaocodserasa d on c.cdsa = d.novocodigoserasa and  c.CDSGSA = d.segmentocodigoserasa  and d.codigoidioma <> '1'
        |       left join gdn_brazil.wrk_descricaocodserasa e on c.cdsa = e.novocodigoserasa and  c.CDSGSA = e.segmentocodigoserasa  and e.codigoidioma = '1'
        |       left join gdn_brazil.wrk_tbultimate u on u.NU_DOCUMENTO_CAD = c.NU_DOCUMENTO_CAD
        |
        |) tab
        |where cod = 1
        |
        |order by NU_DOCUMENTO_CAD desc
      """.stripMargin)

  }

  def processSisterCompany(sparkSession: SparkSession): Unit = {

    sparkSession.sql(
      """
        |insert OVERWRITE table gdn_brazil.wrk_sistcompsm
        |select distinct
        |q.nu_documento_cad,
        |q.cdpm,
        |q.petot
        |from gdn_brazil.wrk_cadastropjenriquecida c
        |inner join gdn_brazil.wrk_quadrosocietario q on c.nu_documento_cad = q.nu_documento_cad
        |where q.petot > 500
      """.stripMargin)

  }

  def processHbaseKeys(spark: SparkSession): Unit = {

    val DfAdminist = spark.sql(
      """
        |  select concat(lpad(a.nu_documento_cad,8,'0'),a.co_tipo_doc_cad, lpad(a.nu_seqriori,5,'0')) as key
        |  , a.* from gdn_brazil.wrk_administradores a
      """.stripMargin)
    DataFrameToHbase.insertInto(DfAdminist,"gdn_brazil:basona","ADMINIST","key",10000)

    val DFAntecess = spark.sql(
      """
        |  select concat(lpad(a.nu_documento_cad,8,'0'),a.co_tipo_doc_cad) as key
        |  , a.* from gdn_brazil.wrk_antecessoras a
      """.stripMargin)
    DataFrameToHbase.insertInto(DFAntecess,"gdn_brazil:basona","ADMINIST","key",10000)

    val DFCadastpjm = spark.sql(
      """
        |  select lpad(a.nu_documento_cad,8,'0') as key
        |  , a.* from gdn_brazil.wrk_cadastropjenriquecida a
        |  where  a.nu_matriz = a.co_compldoc_cad
      """.stripMargin)
    DataFrameToHbase.insertInto(DFCadastpjm,"gdn_brazil:basona","ADMINIST","key",10000)

    val DFCadastropj = spark.sql(
      """
        |  select concat(lpad(a.nu_documento_cad,8,'0'),a.co_tipo_doc_cad,lpad(a.co_compldoc_cad,4,'0')) as key
        |  , a.* from gdn_brazil.wrk_cadastropjenriquecida a
      """.stripMargin)
    DataFrameToHbase.insertInto(DFCadastropj,"gdn_brazil:basona","ADMINIST","key",10000)

    val DFFiliais = spark.sql(
      """
        |  select concat(lpad(a.nu_documento_cad,8,'0'),a.co_tipo_doc_cad) as key
        |  , a.* from gdn_brazil.wrk_filiais a
      """.stripMargin)
    DataFrameToHbase.insertInto(DFFiliais,"gdn_brazil:basona","ADMINIST","key",10000)

    val DFConselho = spark.sql(
      """
        |  select concat(lpad(a.nu_documento_cad,8,'0'),a.co_tipo_doc_cad,lpad(a.nu_seqriori,5,'0')) as key
        |  , a.* from gdn_brazil.wrk_conselho a
      """.stripMargin)
    DataFrameToHbase.insertInto(DFConselho,"gdn_brazil:basona","ADMINIST","key",10000)

    val DFEnderecoTel = spark.sql(
      """
        |  select concat(lpad(a.nu_documento_cad,8,'0'),a.co_tipo_doc_cad,lpad(a.co_compldoc_cad,4,'0'),lpad(a.nu_seq_priori,5,'0')) as key
        |  , a.* from gdn_brazil.wrk_enderecotelefone a
      """.stripMargin)
    DataFrameToHbase.insertInto(DFEnderecoTel,"gdn_brazil:basona","ADMINIST","key",10000)

    val DFInscrest = spark.sql(
      """
        |  select concat(lpad(a.nu_documento_cad,8,'0'),a.co_tipo_doc_cad,lpad(a.co_compldoc_cad,4,'0')) as key
        |  , a.* from gdn_brazil.wrk_inscricaoestadual a
      """.stripMargin)
    DataFrameToHbase.insertInto(DFInscrest,"gdn_brazil:basona","ADMINIST","key",10000)

    val DfHistrisk = spark.sql(
      """
        |  select concat(lpad(a.nu_documento_cad,8,'0'),a.co_tipo_doc_cad,lpad(a.nu_seq_priori,5,'0')) as key
        |  , a.* from gdn_brazil.wrk_historicoriscopj a
      """.stripMargin)
    DataFrameToHbase.insertInto(DfHistrisk,"gdn_brazil:basona","ADMINIST","key",10000)

    val DFParticipada = spark.sql(
      """
        |  select concat(lpad(a.nu_documento_cad,8,'0'),a.co_tipo_doc_cad,lpad(a.nu_seqriori,5,'0')) as key
        |  , a.* from gdn_brazil.wrk_participada a
      """.stripMargin)
    DataFrameToHbase.insertInto(DFParticipada,"gdn_brazil:basona","ADMINIST","key",10000)

    val DFParticipante = spark.sql(
      """
        |  select concat(lpad(a.nu_documento_cad,8,'0'),a.co_tipo_doc_cad,lpad(a.nu_seqriori,5,'0')) as key
        |  , a.* from gdn_brazil.wrk_participantes a
      """.stripMargin)
    DataFrameToHbase.insertInto(DFParticipante,"gdn_brazil:basona","ADMINIST","key",10000)

    val DFQuadrosoc = spark.sql(
      """
        |  select concat(lpad(a.nu_documento_cad,8,'0'),a.co_tipo_doc_cad,lpad(a.nu_seq_priori,5,'0')) as key
        |  , a.* from gdn_brazil.wrk_quadrosocietario a
      """.stripMargin)
    DataFrameToHbase.insertInto(DFQuadrosoc,"gdn_brazil:basona","ADMINIST","key",10000)

  }
}
