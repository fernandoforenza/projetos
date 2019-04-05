--###################################################################################################################################
--# Script hive metadados gdn brazil
--# Autor: Fernando Forenza
--# Data: 25/03/2019
--# Versao 1.0
--###################################################################################################################################

use gdn_brazil;

CREATE EXTERNAL TABLE IF NOT EXISTS gdn_brazil.hbase_sensibiliza (
    key              string
    ,tp_doc          string
    ,nu_doc          string
    ,tp_sensibiliza  string
)
STORED BY "org.apache.hadoop.hive.hbase.HBaseStorageHandler"
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,SENSIBILIZA:tp_doc,SENSIBILIZA:nu_doc,SENSIBILIZA:tp_sensibiliza")
TBLPROPERTIES("hbase.table.name" = "gdn:basona");

CREATE EXTERNAL TABLE IF NOT EXISTS gdn_brazil.hbase_administradores (
     key                string
    ,nu_documento_cad   string
    ,co_tipo_doc_cad    string
    ,co_compldoc_cad    string
    ,nu_seq             string
    ,co_tipo_reg        string
    ,nu_ordem_exib      string
    ,nu_seqriori        string
    ,co_valido          string
    ,nusu_cen           string
    ,dt_inicio          string
    ,dt_final           string
    ,chave              string
    ,iddk               string
    ,cdpm               string
    ,cdsqpm             string
    ,dgcrpf             string
    ,nman               string
    ,nrrggl             string
    ,ogrggl             string
    ,cdna               string
    ,cdnahg             string
    ,cdei               string
    ,cdeihg             string
    ,cdcjam             string
    ,cdcjamhg           string
    ,dtilam             string
    ,dtfmam             string
    ,tmdu               string
    ,dtam               string
    ,dtns               string
    ,nrfi               string
    ,dtatualiza         string
    ,cdpfch             string
    ,dtnsch             string
    ,nrrgch             string
    ,orrgch             string
    ,cdnach             string
    ,cdnachhg           string
    ,dtfdcg_cg          string
    ,origem             string
    ,flgegativos        string
    ,flgeg1             string
    ,qtdeg1             string
    ,flgeg2             string
    ,qtdeg2             string
    ,flgeg3             string
    ,qtdeg3             string
    ,flgeg4             string
    ,qtdeg4             string
    ,flgeg5             string
    ,qtdeg5             string
    ,flgeg6             string
    ,qtdeg6             string
    ,flgeg7             string
    ,qtdeg7             string
    ,flgeg8             string
    ,qtdeg8             string
    ,flgeg9             string
    ,qtdeg9             string
)
STORED BY "org.apache.hadoop.hive.hbase.HBaseStorageHandler"
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,ADMINIST:nu_documento_cad,ADMINIST:co_tipo_doc_cad,ADMINIST:co_compldoc_cad,ADMINIST:nu_seq,ADMINIST:co_tipo_reg,ADMINIST:nu_ordem_exib,ADMINIST:nu_seqriori,ADMINIST:co_valido,ADMINIST:nusu_cen,ADMINIST:dt_inicio,ADMINIST:dt_final,ADMINIST:chave,ADMINIST:iddk,ADMINIST:cdpm,ADMINIST:cdsqpm,ADMINIST:dgcrpf,ADMINIST:nman,ADMINIST:nrrggl,ADMINIST:ogrggl,ADMINIST:cdna,ADMINIST:cdnahg,ADMINIST:cdei,ADMINIST:cdeihg,ADMINIST:cdcjam,ADMINIST:cdcjamhg,ADMINIST:dtilam,ADMINIST:dtfmam,ADMINIST:tmdu,ADMINIST:dtam,ADMINIST:dtns,ADMINIST:nrfi,ADMINIST:dtatualiza,ADMINIST:cdpfch,ADMINIST:dtnsch,ADMINIST:nrrgch,ADMINIST:orrgch,ADMINIST:cdnach,ADMINIST:cdnachhg,ADMINIST:dtfdcg_cg,ADMINIST:origem,ADMINIST:flgegativos,ADMINIST:flgeg1,ADMINIST:qtdeg1,ADMINIST:flgeg2,ADMINIST:qtdeg2,ADMINIST:flgeg3,ADMINIST:qtdeg3,ADMINIST:flgeg4,ADMINIST:qtdeg4,ADMINIST:flgeg5,ADMINIST:qtdeg5,ADMINIST:flgeg6,ADMINIST:qtdeg6,ADMINIST:flgeg7,ADMINIST:qtdeg7,ADMINIST:flgeg8,ADMINIST:qtdeg8,ADMINIST:flgeg9,ADMINIST:qtdeg9")
TBLPROPERTIES("hbase.table.name" = "gdn:basona");

CREATE EXTERNAL TABLE IF NOT EXISTS gdn_brazil.hbase_antecessoras (
     key                 string
    ,nu_documento_cad    string
    ,co_tipo_doc_cad     string
    ,co_compldoc_cad     string
    ,nu_seq              string
    ,co_tipo_reg         string
    ,nu_ordem_exib       string
    ,nu_seq_priori       string
    ,co_valido           string
    ,nu_nsu_cen          string
    ,dt_inicio           string
    ,dt_final            string
    ,qtde                string
    ,antec1              string
    ,antec1_dtmt         string
    ,antec2              string
    ,antec2_dtmt         string
    ,antec3              string
    ,antec3_dtmt         string
    ,antec4              string
    ,antec4_dtmt         string
    ,antec5              string
    ,antec5_dtmt         string
)
STORED BY "org.apache.hadoop.hive.hbase.HBaseStorageHandler"
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,ANTECESS:nu_documento_cad,ANTECESS:co_tipo_doc_cad,ANTECESS:co_compldoc_cad,ANTECESS:nu_seq,ANTECESS:co_tipo_reg,ANTECESS:nu_ordem_exib,ANTECESS:nu_seq_priori,ANTECESS:co_valido,ANTECESS:nu_nsu_cen,ANTECESS:dt_inicio,ANTECESS:dt_final,ANTECESS:qtde,ANTECESS:antec1,ANTECESS:antec1_dtmt,ANTECESS:antec2,ANTECESS:antec2_dtmt,ANTECESS:antec3,ANTECESS:antec3_dtmt,ANTECESS:antec4,ANTECESS:antec4_dtmt,ANTECESS:antec5,ANTECESS:antec5_dtmt")
TBLPROPERTIES("hbase.table.name" = "gdn:basona");

CREATE EXTERNAL TABLE IF NOT EXISTS gdn_brazil.hbase_cadastropj (
     key                                 string
    ,nu_documento_cad                    string
    ,co_tipo_doc_cad                     string
    ,co_compldoc_cad                     string
    ,nu_seq                              string
    ,co_tipo_reg                         string
    ,nu_ordem_exib                       string
    ,nu_seq_priori                       string
    ,co_valido                           string
    ,nu_nsu_cen                          string
    ,dt_inicio                           string
    ,dt_final                            string
    ,cdmt                                string
    ,rzsc                                string
    ,dtutmf                              string
    ,hrutmf                              string
    ,totqtvlcv                           string
    ,topeaocv                            string
    ,cdog                                string
    ,cdtpsc                              string
    ,dtutrg                              string
    ,nrutrg                              string
    ,cdsa                                string
    ,cdsgsa                              string
    ,cpsc                                string
    ,cpra                                string
    ,dtfdcg                              string
    ,cpar                                string
    ,nreg                                string
    ,dtblgf                              string
    ,qtvlnu                              string
    ,cdcrao                              string
    ,cdim                                string
    ,cdep                                string
    ,nmlf                                string
    ,nrtflf                              string
    ,nmfs                                string
    ,cdrcfe                              string
    ,cpaz                                string
    ,cdttii                              string
    ,cddalf                              string
    ,peepvd                              string
    ,peimcf                              string
    ,cder                                string
    ,nmgrec                              string
    ,toqtvlsv                            string
    ,cdrblf                              string
    ,cdna                                string
    ,qtvlnonu                            string
    ,topeaosv                            string
    ,cdstjtem                            string
    ,cdat                                string
    ,cdstcg                              string
    ,dtatrz                              string
    ,dtatpo                              string
    ,e3v2                                string
    ,dtgsgp                              string
    ,flfmcn                              string
    ,dtfg                                string
    ,cdrcfen                             string
    ,dtininoramo                         string
    ,cdinstsede                          string
    ,nu_nire                             string
    ,co_sgrcfen                          string
    ,co_ctcapit                          string
    ,co_origem_cap                       string
    ,co_relviscdc                        string
    ,nu_sq_ultaltco                      string
    ,co_incdqcadpjb                      string
    ,co_ind_ativsoc                      string
    ,co_sitjunta                         string
    ,nu_inscrest                         string
    ,nu_inscrmun                         string
    ,co_proc_at                          string
    ,dt_inscreceit                       string
    ,dt_cadastro                         string
    ,co_ind_filial                       string
    ,co_ativserasa                       string
    ,nu_matriz                           string
    ,co_opc_simples                      string
    ,dt_inf_simples                      string
    ,co_situacaon                        string
    ,dt_indoperac                        string
    ,co_indempmei                        string
    ,co_situacao                         string
    ,co_naturrec                         string
    ,dt_alterarf                         string
    ,qt_filiarf                          string
    ,co_opcsimei                         string
    ,dt_sitsimei                         string
    ,dt_sitsimpl                         string
    ,co_ativrecv                         string
    ,co_cnaesec1                         string
    ,co_cnaesec2                         string
    ,co_cnaesec3                         string
    ,co_cnaesec4                         string
    ,co_cnaesec5                         string
    ,co_cnaesec6                         string
    ,co_cnaesec7                         string
    ,co_cnaesec8                         string
    ,co_cnaesec9                         string
    ,co_cnaesec10                        string
    ,co_natur_recn_emp                   string
    ,dt_situac_emp                       string
    ,email                               string
    ,co_internmail                       string
    ,co_internsite                       string
    ,de_opsimei_ant                      string
    ,porte                               string
    ,ds_porte                            string
    ,dtemisficha                         string
    ,vl1conta                            string
    ,dt1_balanco                         string
    ,vl2conta                            string
    ,dt2_balanco                         string
    ,vl3conta                            string
    ,dt3_balanco                         string
    ,vl4conta                            string
    ,dt4_balanco                         string
    ,vl5conta                            string
    ,dt5_balanco                         string
    ,ipalerta                            string
    ,gfclient                            string
    ,co_grpecon                          string
    ,no_grpoecon                         string
    ,co_vincasso                         string
    ,controlagrp                         string
    ,flagneg                             string
    ,neg_flgprot                         string
    ,neg_qtdprot                         string
    ,neg_flgacao                         string
    ,neg_qtdacao                         string
    ,neg_flgfale                         string
    ,neg_qtdfale                         string
    ,neg_flgconc                         string
    ,neg_qtdconc                         string
    ,neg_flgrefi                         string
    ,neg_qtdrefi                         string
    ,neg_flgache                         string
    ,neg_qtdache                         string
    ,neg_flgccf                          string
    ,neg_qtdccf                          string
    ,neg_flgpefi                         string
    ,neg_qtdpefi                         string
    ,neg_flgconv                         string
    ,neg_qtdconv                         string
    ,neg_cheque                          string
    ,neg_qtdcheq                         string
    ,codisic                             string
    ,descisic                            string
    ,descricaonjport                     string
    ,descricaonjingles                   string
    ,indicadornjsfl                      string
    ,codigoserasapadrao                  string
    ,novocodigoserasa                    string
    ,descricaocodigoserasa_port          string
    ,descricaoresumidacodserasa_port     string
    ,descricaocodigoserasa_eng           string
    ,descricaoresumidacodserasa_eng      string
    ,cnaedescricao                       string
    ,ultimateparent                      string
    ,totalparticipacao                   string
)
STORED BY "org.apache.hadoop.hive.hbase.HBaseStorageHandler"
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,CADASTPJ:nu_documento_cad,CADASTPJ:co_tipo_doc_cad,CADASTPJ:co_compldoc_cad,CADASTPJ:nu_seq,CADASTPJ:co_tipo_reg,CADASTPJ:nu_ordem_exib,CADASTPJ:nu_seq_priori,CADASTPJ:co_valido,CADASTPJ:nu_nsu_cen,CADASTPJ:dt_inicio,CADASTPJ:dt_final,CADASTPJ:cdmt,CADASTPJ:rzsc,CADASTPJ:dtutmf,CADASTPJ:hrutmf,CADASTPJ:totqtvlcv,CADASTPJ:topeaocv,CADASTPJ:cdog,CADASTPJ:cdtpsc,CADASTPJ:dtutrg,CADASTPJ:nrutrg,CADASTPJ:cdsa,CADASTPJ:cdsgsa,CADASTPJ:cpsc,CADASTPJ:cpra,CADASTPJ:dtfdcg,CADASTPJ:cpar,CADASTPJ:nreg,CADASTPJ:dtblgf,CADASTPJ:qtvlnu,CADASTPJ:cdcrao,CADASTPJ:cdim,CADASTPJ:cdep,CADASTPJ:nmlf,CADASTPJ:nrtflf,CADASTPJ:nmfs,CADASTPJ:cdrcfe,CADASTPJ:cpaz,CADASTPJ:cdttii,CADASTPJ:cddalf,CADASTPJ:peepvd,CADASTPJ:peimcf,CADASTPJ:cder,CADASTPJ:nmgrec,CADASTPJ:toqtvlsv,CADASTPJ:cdrblf,CADASTPJ:cdna,CADASTPJ:qtvlnonu,CADASTPJ:topeaosv,CADASTPJ:cdstjtem,CADASTPJ:cdat,CADASTPJ:cdstcg,CADASTPJ:dtatrz,CADASTPJ:dtatpo,CADASTPJ:e3v2,CADASTPJ:dtgsgp,CADASTPJ:flfmcn,CADASTPJ:dtfg,CADASTPJ:cdrcfen,CADASTPJ:dtininoramo,CADASTPJ:cdinstsede,CADASTPJ:nu_nire,CADASTPJ:co_sgrcfen,CADASTPJ:co_ctcapit,CADASTPJ:co_origem_cap,CADASTPJ:co_relviscdc,CADASTPJ:nu_sq_ultaltco,CADASTPJ:co_incdqcadpjb,CADASTPJ:co_ind_ativsoc,CADASTPJ:co_sitjunta,CADASTPJ:nu_inscrest,CADASTPJ:nu_inscrmun,CADASTPJ:co_proc_at,CADASTPJ:dt_inscreceit,CADASTPJ:dt_cadastro,CADASTPJ:co_ind_filial,CADASTPJ:co_ativserasa,CADASTPJ:nu_matriz,CADASTPJ:co_opc_simples,CADASTPJ:dt_inf_simples,CADASTPJ:co_situacaon,CADASTPJ:dt_indoperac,CADASTPJ:co_indempmei,CADASTPJ:co_situacao,CADASTPJ:co_naturrec,CADASTPJ:dt_alterarf,CADASTPJ:qt_filiarf,CADASTPJ:co_opcsimei,CADASTPJ:dt_sitsimei,CADASTPJ:dt_sitsimpl,CADASTPJ:co_ativrecv,CADASTPJ:co_cnaesec1,CADASTPJ:co_cnaesec2,CADASTPJ:co_cnaesec3,CADASTPJ:co_cnaesec4,CADASTPJ:co_cnaesec5,CADASTPJ:co_cnaesec6,CADASTPJ:co_cnaesec7,CADASTPJ:co_cnaesec8,CADASTPJ:co_cnaesec9,CADASTPJ:co_cnaesec10,CADASTPJ:co_natur_recn_emp,CADASTPJ:dt_situac_emp,CADASTPJ:email,CADASTPJ:co_internmail,CADASTPJ:co_internsite,CADASTPJ:de_opsimei_ant,CADASTPJ:porte,CADASTPJ:ds_porte,CADASTPJ:dtemisficha,CADASTPJ:vl1conta,CADASTPJ:dt1_balanco,CADASTPJ:vl2conta,CADASTPJ:dt2_balanco,CADASTPJ:vl3conta,CADASTPJ:dt3_balanco,CADASTPJ:vl4conta,CADASTPJ:dt4_balanco,CADASTPJ:vl5conta,CADASTPJ:dt5_balanco,CADASTPJ:ipalerta,CADASTPJ:gfclient,CADASTPJ:co_grpecon,CADASTPJ:no_grpoecon,CADASTPJ:co_vincasso,CADASTPJ:controlagrp,CADASTPJ:flagneg,CADASTPJ:neg_flgprot,CADASTPJ:neg_qtdprot,CADASTPJ:neg_flgacao,CADASTPJ:neg_qtdacao,CADASTPJ:neg_flgfale,CADASTPJ:neg_qtdfale,CADASTPJ:neg_flgconc,CADASTPJ:neg_qtdconc,CADASTPJ:neg_flgrefi,CADASTPJ:neg_qtdrefi,CADASTPJ:neg_flgache,CADASTPJ:neg_qtdache,CADASTPJ:neg_flgccf,CADASTPJ:neg_qtdccf,CADASTPJ:neg_flgpefi,CADASTPJ:neg_qtdpefi,CADASTPJ:neg_flgconv,CADASTPJ:neg_qtdconv,CADASTPJ:neg_cheque,CADASTPJ:neg_qtdcheq,CADASTPJ:codisic,CADASTPJ:descisic,CADASTPJ:descricaonjport,CADASTPJ:descricaonjingles,CADASTPJ:indicadornjsfl,CADASTPJ:codigoserasapadrao,CADASTPJ:novocodigoserasa,CADASTPJ:descricaocodigoserasa_port,CADASTPJ:descricaoresumidacodserasa_port,CADASTPJ:descricaocodigoserasa_eng,CADASTPJ:descricaoresumidacodserasa_eng,CADASTPJ:cnaedescricao,CADASTPJ:ultimateparent,CADASTPJ:totalparticipacao")
TBLPROPERTIES("hbase.table.name" = "gdn:basona");

CREATE EXTERNAL TABLE IF NOT EXISTS gdn_brazil.hbase_cadastropjm (
     key                                 string
    ,nu_documento_cad                    string
    ,co_tipo_doc_cad                     string
    ,co_compldoc_cad                     string
    ,nu_seq                              string
    ,co_tipo_reg                         string
    ,nu_ordem_exib                       string
    ,nu_seq_priori                       string
    ,co_valido                           string
    ,nu_nsu_cen                          string
    ,dt_inicio                           string
    ,dt_final                            string
    ,cdmt                                string
    ,rzsc                                string
    ,dtutmf                              string
    ,hrutmf                              string
    ,totqtvlcv                           string
    ,topeaocv                            string
    ,cdog                                string
    ,cdtpsc                              string
    ,dtutrg                              string
    ,nrutrg                              string
    ,cdsa                                string
    ,cdsgsa                              string
    ,cpsc                                string
    ,cpra                                string
    ,dtfdcg                              string
    ,cpar                                string
    ,nreg                                string
    ,dtblgf                              string
    ,qtvlnu                              string
    ,cdcrao                              string
    ,cdim                                string
    ,cdep                                string
    ,nmlf                                string
    ,nrtflf                              string
    ,nmfs                                string
    ,cdrcfe                              string
    ,cpaz                                string
    ,cdttii                              string
    ,cddalf                              string
    ,peepvd                              string
    ,peimcf                              string
    ,cder                                string
    ,nmgrec                              string
    ,toqtvlsv                            string
    ,cdrblf                              string
    ,cdna                                string
    ,qtvlnonu                            string
    ,topeaosv                            string
    ,cdstjtem                            string
    ,cdat                                string
    ,cdstcg                              string
    ,dtatrz                              string
    ,dtatpo                              string
    ,e3v2                                string
    ,dtgsgp                              string
    ,flfmcn                              string
    ,dtfg                                string
    ,cdrcfen                             string
    ,dtininoramo                         string
    ,cdinstsede                          string
    ,nu_nire                             string
    ,co_sgrcfen                          string
    ,co_ctcapit                          string
    ,co_origem_cap                       string
    ,co_relviscdc                        string
    ,nu_sq_ultaltco                      string
    ,co_incdqcadpjb                      string
    ,co_ind_ativsoc                      string
    ,co_sitjunta                         string
    ,nu_inscrest                         string
    ,nu_inscrmun                         string
    ,co_proc_at                          string
    ,dt_inscreceit                       string
    ,dt_cadastro                         string
    ,co_ind_filial                       string
    ,co_ativserasa                       string
    ,nu_matriz                           string
    ,co_opc_simples                      string
    ,dt_inf_simples                      string
    ,co_situacaon                        string
    ,dt_indoperac                        string
    ,co_indempmei                        string
    ,co_situacao                         string
    ,co_naturrec                         string
    ,dt_alterarf                         string
    ,qt_filiarf                          string
    ,co_opcsimei                         string
    ,dt_sitsimei                         string
    ,dt_sitsimpl                         string
    ,co_ativrecv                         string
    ,co_cnaesec1                         string
    ,co_cnaesec2                         string
    ,co_cnaesec3                         string
    ,co_cnaesec4                         string
    ,co_cnaesec5                         string
    ,co_cnaesec6                         string
    ,co_cnaesec7                         string
    ,co_cnaesec8                         string
    ,co_cnaesec9                         string
    ,co_cnaesec10                        string
    ,co_natur_recn_emp                   string
    ,dt_situac_emp                       string
    ,email                               string
    ,co_internmail                       string
    ,co_internsite                       string
    ,de_opsimei_ant                      string
    ,porte                               string
    ,ds_porte                            string
    ,dtemisficha                         string
    ,vl1conta                            string
    ,dt1_balanco                         string
    ,vl2conta                            string
    ,dt2_balanco                         string
    ,vl3conta                            string
    ,dt3_balanco                         string
    ,vl4conta                            string
    ,dt4_balanco                         string
    ,vl5conta                            string
    ,dt5_balanco                         string
    ,ipalerta                            string
    ,gfclient                            string
    ,co_grpecon                          string
    ,no_grpoecon                         string
    ,co_vincasso                         string
    ,controlagrp                         string
    ,flagneg                             string
    ,neg_flgprot                         string
    ,neg_qtdprot                         string
    ,neg_flgacao                         string
    ,neg_qtdacao                         string
    ,neg_flgfale                         string
    ,neg_qtdfale                         string
    ,neg_flgconc                         string
    ,neg_qtdconc                         string
    ,neg_flgrefi                         string
    ,neg_qtdrefi                         string
    ,neg_flgache                         string
    ,neg_qtdache                         string
    ,neg_flgccf                          string
    ,neg_qtdccf                          string
    ,neg_flgpefi                         string
    ,neg_qtdpefi                         string
    ,neg_flgconv                         string
    ,neg_qtdconv                         string
    ,neg_cheque                          string
    ,neg_qtdcheq                         string
    ,codisic                             string
    ,descisic                            string
    ,descricaonjport                     string
    ,descricaonjingles                   string
    ,indicadornjsfl                      string
    ,codigoserasapadrao                  string
    ,novocodigoserasa                    string
    ,descricaocodigoserasa_port          string
    ,descricaoresumidacodserasa_port     string
    ,descricaocodigoserasa_eng           string
    ,descricaoresumidacodserasa_eng      string
    ,cnaedescricao                       string
    ,ultimateparent                      string
    ,totalparticipacao                   string
)
STORED BY "org.apache.hadoop.hive.hbase.HBaseStorageHandler"
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,CADASTPJM:nu_documento_cad,CADASTPJM:co_tipo_doc_cad,CADASTPJM:co_compldoc_cad,CADASTPJM:nu_seq,CADASTPJM:co_tipo_reg,CADASTPJM:nu_ordem_exib,CADASTPJM:nu_seq_priori,CADASTPJM:co_valido,CADASTPJM:nu_nsu_cen,CADASTPJM:dt_inicio,CADASTPJM:dt_final,CADASTPJM:cdmt,CADASTPJM:rzsc,CADASTPJM:dtutmf,CADASTPJM:hrutmf,CADASTPJM:totqtvlcv,CADASTPJM:topeaocv,CADASTPJM:cdog,CADASTPJM:cdtpsc,CADASTPJM:dtutrg,CADASTPJM:nrutrg,CADASTPJM:cdsa,CADASTPJM:cdsgsa,CADASTPJM:cpsc,CADASTPJM:cpra,CADASTPJM:dtfdcg,CADASTPJM:cpar,CADASTPJM:nreg,CADASTPJM:dtblgf,CADASTPJM:qtvlnu,CADASTPJM:cdcrao,CADASTPJM:cdim,CADASTPJM:cdep,CADASTPJM:nmlf,CADASTPJM:nrtflf,CADASTPJM:nmfs,CADASTPJM:cdrcfe,CADASTPJM:cpaz,CADASTPJM:cdttii,CADASTPJM:cddalf,CADASTPJM:peepvd,CADASTPJM:peimcf,CADASTPJM:cder,CADASTPJM:nmgrec,CADASTPJM:toqtvlsv,CADASTPJM:cdrblf,CADASTPJM:cdna,CADASTPJM:qtvlnonu,CADASTPJM:topeaosv,CADASTPJM:cdstjtem,CADASTPJM:cdat,CADASTPJM:cdstcg,CADASTPJM:dtatrz,CADASTPJM:dtatpo,CADASTPJM:e3v2,CADASTPJM:dtgsgp,CADASTPJM:flfmcn,CADASTPJM:dtfg,CADASTPJM:cdrcfen,CADASTPJM:dtininoramo,CADASTPJM:cdinstsede,CADASTPJM:nu_nire,CADASTPJM:co_sgrcfen,CADASTPJM:co_ctcapit,CADASTPJM:co_origem_cap,CADASTPJM:co_relviscdc,CADASTPJM:nu_sq_ultaltco,CADASTPJM:co_incdqcadpjb,CADASTPJM:co_ind_ativsoc,CADASTPJM:co_sitjunta,CADASTPJM:nu_inscrest,CADASTPJM:nu_inscrmun,CADASTPJM:co_proc_at,CADASTPJM:dt_inscreceit,CADASTPJM:dt_cadastro,CADASTPJM:co_ind_filial,CADASTPJM:co_ativserasa,CADASTPJM:nu_matriz,CADASTPJM:co_opc_simples,CADASTPJM:dt_inf_simples,CADASTPJM:co_situacaon,CADASTPJM:dt_indoperac,CADASTPJM:co_indempmei,CADASTPJM:co_situacao,CADASTPJM:co_naturrec,CADASTPJM:dt_alterarf,CADASTPJM:qt_filiarf,CADASTPJM:co_opcsimei,CADASTPJM:dt_sitsimei,CADASTPJM:dt_sitsimpl,CADASTPJM:co_ativrecv,CADASTPJM:co_cnaesec1,CADASTPJM:co_cnaesec2,CADASTPJM:co_cnaesec3,CADASTPJM:co_cnaesec4,CADASTPJM:co_cnaesec5,CADASTPJM:co_cnaesec6,CADASTPJM:co_cnaesec7,CADASTPJM:co_cnaesec8,CADASTPJM:co_cnaesec9,CADASTPJM:co_cnaesec10,CADASTPJM:co_natur_recn_emp,CADASTPJM:dt_situac_emp,CADASTPJM:email,CADASTPJM:co_internmail,CADASTPJM:co_internsite,CADASTPJM:de_opsimei_ant,CADASTPJM:porte,CADASTPJM:ds_porte,CADASTPJM:dtemisficha,CADASTPJM:vl1conta,CADASTPJM:dt1_balanco,CADASTPJM:vl2conta,CADASTPJM:dt2_balanco,CADASTPJM:vl3conta,CADASTPJM:dt3_balanco,CADASTPJM:vl4conta,CADASTPJM:dt4_balanco,CADASTPJM:vl5conta,CADASTPJM:dt5_balanco,CADASTPJM:ipalerta,CADASTPJM:gfclient,CADASTPJM:co_grpecon,CADASTPJM:no_grpoecon,CADASTPJM:co_vincasso,CADASTPJM:controlagrp,CADASTPJM:flagneg,CADASTPJM:neg_flgprot,CADASTPJM:neg_qtdprot,CADASTPJM:neg_flgacao,CADASTPJM:neg_qtdacao,CADASTPJM:neg_flgfale,CADASTPJM:neg_qtdfale,CADASTPJM:neg_flgconc,CADASTPJM:neg_qtdconc,CADASTPJM:neg_flgrefi,CADASTPJM:neg_qtdrefi,CADASTPJM:neg_flgache,CADASTPJM:neg_qtdache,CADASTPJM:neg_flgccf,CADASTPJM:neg_qtdccf,CADASTPJM:neg_flgpefi,CADASTPJM:neg_qtdpefi,CADASTPJM:neg_flgconv,CADASTPJM:neg_qtdconv,CADASTPJM:neg_cheque,CADASTPJM:neg_qtdcheq,CADASTPJM:codisic,CADASTPJM:descisic,CADASTPJM:descricaonjport,CADASTPJM:descricaonjingles,CADASTPJM:indicadornjsfl,CADASTPJM:codigoserasapadrao,CADASTPJM:novocodigoserasa,CADASTPJM:descricaocodigoserasa_port,CADASTPJM:descricaoresumidacodserasa_port,CADASTPJM:descricaocodigoserasa_eng,CADASTPJM:descricaoresumidacodserasa_eng,CADASTPJM:cnaedescricao,CADASTPJM:ultimateparent,CADASTPJM:totalparticipacao")
TBLPROPERTIES("hbase.table.name" = "gdn:basona");

CREATE EXTERNAL TABLE IF NOT EXISTS gdn_brazil.hbase_conselho (
     key                 string
    ,nu_documento_cad    string
    ,co_tipo_doc_cad     string
    ,co_compldoc_cad     string
    ,nu_seq              string
    ,co_tipo_reg         string
    ,nu_ordem_exib       string
    ,nu_seqriori         string
    ,co_valido           string
    ,nusu_cen            string
    ,dt_inicio           string
    ,dt_final            string
    ,chave               string
    ,iddk                string
    ,cdpm                string
    ,cdsqpm              string
    ,dgcrpf              string
    ,nman                string
    ,cdcjck              string
    ,dtfmck              string
    ,cdcjckhg            string
    ,nrrggl              string
    ,ogrggl              string
    ,cdna                string
    ,cdnahg              string
    ,cdei                string
    ,cdeihg              string
    ,dtilck              string
    ,dtns                string
    ,col29               string
    ,flgegativos         string
    ,flgeg1              string
    ,qtdeg1              string
    ,flgeg2              string
    ,qtdeg2              string
    ,flgeg3              string
    ,qtdeg3              string
    ,flgeg4              string
    ,qtdeg4              string
    ,flgeg5              string
    ,qtdeg5              string
    ,flgeg6              string
    ,qtdeg6              string
    ,flgeg7              string
    ,qtdeg7              string
    ,flgeg8              string
    ,qtdeg8              string
    ,flgeg9              string
    ,qtdeg9              string
)
STORED BY "org.apache.hadoop.hive.hbase.HBaseStorageHandler"
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,CONSELHO:nu_documento_cad,CONSELHO:co_tipo_doc_cad,CONSELHO:co_compldoc_cad,CONSELHO:nu_seq,CONSELHO:co_tipo_reg,CONSELHO:nu_ordem_exib,CONSELHO:nu_seqriori,CONSELHO:co_valido,CONSELHO:nusu_cen,CONSELHO:dt_inicio,CONSELHO:dt_final,CONSELHO:chave,CONSELHO:iddk,CONSELHO:cdpm,CONSELHO:cdsqpm,CONSELHO:dgcrpf,CONSELHO:nman,CONSELHO:cdcjck,CONSELHO:dtfmck,CONSELHO:cdcjckhg,CONSELHO:nrrggl,CONSELHO:ogrggl,CONSELHO:cdna,CONSELHO:cdnahg,CONSELHO:cdei,CONSELHO:cdeihg,CONSELHO:dtilck,CONSELHO:dtns,CONSELHO:col29,CONSELHO:flgegativos,CONSELHO:flgeg1,CONSELHO:qtdeg1,CONSELHO:flgeg2,CONSELHO:qtdeg2,CONSELHO:flgeg3,CONSELHO:qtdeg3,CONSELHO:flgeg4,CONSELHO:qtdeg4,CONSELHO:flgeg5,CONSELHO:qtdeg5,CONSELHO:flgeg6,CONSELHO:qtdeg6,CONSELHO:flgeg7,CONSELHO:qtdeg7,CONSELHO:flgeg8,CONSELHO:qtdeg8,CONSELHO:flgeg9,CONSELHO:qtdeg9")
TBLPROPERTIES("hbase.table.name" = "gdn:basona");

CREATE EXTERNAL TABLE IF NOT EXISTS gdn_brazil.hbase_enderecotelefone (
     key                 string
    ,nu_documento_cad    string
    ,co_tipo_doc_cad     string
    ,co_compldoc_cad     string
    ,nu_seq              string
    ,co_tipo_reg         string
    ,nu_ordem_exib       string
    ,nu_seq_priori       string
    ,co_valido           string
    ,nu_nsu_cen          string
    ,dt_inicio           string
    ,dt_final            string
    ,peso                string
    ,comp_end            string
    ,publ_end            string
    ,bloq_end            string
    ,cep_valido          string
    ,endereco            string
    ,bairro              string
    ,municipio           string
    ,uf                  string
    ,cep                 string
    ,dt_informacao       string
    ,nsu                 string
    ,origem_end          string
    ,ordem_tel           string
    ,qtde_tel            string
    ,num_tel01           string
    ,ddd_tel01           string
    ,origem_tel01        string
    ,data_tel01          string
    ,comp_tel01          string
    ,publ_tel01          string
    ,bloq_tel01          string
    ,assinante01         string
    ,procon01            string
    ,num_tel02           string
    ,ddd_tel02           string
    ,origem_tel02        string
    ,data_tel02          string
    ,comp_tel02          string
    ,publ_tel02          string
    ,bloq_tel02          string
    ,assinante02         string
    ,procon02            string
    ,num_tel03           string
    ,ddd_tel03           string
    ,origem_tel03        string
    ,data_tel03          string
    ,comp_tel03          string
    ,publ_tel03          string
    ,bloq_tel03          string
    ,assinante03         string
    ,procon03            string
    ,num_tel04           string
    ,ddd_tel04           string
    ,origem_tel04        string
    ,data_tel04          string
    ,comp_tel04          string
    ,publ_tel04          string
    ,bloq_tel04          string
    ,assinante04         string
    ,procon04            string
    ,num_tel05           string
    ,ddd_tel05           string
    ,origem_tel05        string
    ,data_tel05          string
    ,comp_tel05          string
    ,publ_tel05          string
    ,bloq_tel05          string
    ,assinante05         string
    ,procon05            string
    ,num_tel06           string
    ,ddd_tel06           string
    ,origem_tel06        string
    ,data_tel06          string
    ,comp_tel06          string
    ,publ_tel06          string
    ,bloq_tel06          string
    ,assinante06         string
    ,procon06            string
    ,num_tel07           string
    ,ddd_tel07           string
    ,origem_tel07        string
    ,data_tel07          string
    ,comp_tel07          string
    ,publ_tel07          string
    ,bloq_tel07          string
    ,assinante07         string
    ,procon07            string
    ,num_tel08           string
    ,ddd_tel08           string
    ,origem_tel08        string
    ,data_tel08          string
    ,comp_tel08          string
    ,publ_tel08          string
    ,bloq_tel08          string
    ,assinante08         string
    ,procon08            string
    ,num_tel09           string
    ,ddd_tel09           string
    ,origem_tel09        string
    ,data_tel09          string
    ,comp_tel09          string
    ,publ_tel09          string
    ,bloq_tel09          string
    ,assinante09         string
    ,procon09            string
    ,num_tel10           string
    ,ddd_tel10           string
    ,origem_tel10        string
    ,data_tel10          string
    ,comp_tel10          string
    ,publ_tel10          string
    ,bloq_tel10          string
    ,assinante10         string
    ,procon10            string
)
STORED BY "org.apache.hadoop.hive.hbase.HBaseStorageHandler"
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,ENDERTEL:nu_documento_cad,ENDERTEL:co_tipo_doc_cad,ENDERTEL:co_compldoc_cad,ENDERTEL:nu_seq,ENDERTEL:co_tipo_reg,ENDERTEL:nu_ordem_exib,ENDERTEL:nu_seq_priori,ENDERTEL:co_valido,ENDERTEL:nu_nsu_cen,ENDERTEL:dt_inicio,ENDERTEL:dt_final,ENDERTEL:peso,ENDERTEL:comp_end,ENDERTEL:publ_end,ENDERTEL:bloq_end,ENDERTEL:cep_valido,ENDERTEL:endereco,ENDERTEL:bairro,ENDERTEL:municipio,ENDERTEL:uf,ENDERTEL:cep,ENDERTEL:dt_informacao,ENDERTEL:nsu,ENDERTEL:origem_end,ENDERTEL:ordem_tel,ENDERTEL:qtde_tel,ENDERTEL:num_tel01,ENDERTEL:ddd_tel01,ENDERTEL:origem_tel01,ENDERTEL:data_tel01,ENDERTEL:comp_tel01,ENDERTEL:publ_tel01,ENDERTEL:bloq_tel01,ENDERTEL:assinante01,ENDERTEL:procon01,ENDERTEL:num_tel02,ENDERTEL:ddd_tel02,ENDERTEL:origem_tel02,ENDERTEL:data_tel02,ENDERTEL:comp_tel02,ENDERTEL:publ_tel02,ENDERTEL:bloq_tel02,ENDERTEL:assinante02,ENDERTEL:procon02,ENDERTEL:num_tel03,ENDERTEL:ddd_tel03,ENDERTEL:origem_tel03,ENDERTEL:data_tel03,ENDERTEL:comp_tel03,ENDERTEL:publ_tel03,ENDERTEL:bloq_tel03,ENDERTEL:assinante03,ENDERTEL:procon03,ENDERTEL:num_tel04,ENDERTEL:ddd_tel04,ENDERTEL:origem_tel04,ENDERTEL:data_tel04,ENDERTEL:comp_tel04,ENDERTEL:publ_tel04,ENDERTEL:bloq_tel04,ENDERTEL:assinante04,ENDERTEL:procon04,ENDERTEL:num_tel05,ENDERTEL:ddd_tel05,ENDERTEL:origem_tel05,ENDERTEL:data_tel05,ENDERTEL:comp_tel05,ENDERTEL:publ_tel05,ENDERTEL:bloq_tel05,ENDERTEL:assinante05,ENDERTEL:procon05,ENDERTEL:num_tel06,ENDERTEL:ddd_tel06,ENDERTEL:origem_tel06,ENDERTEL:data_tel06,ENDERTEL:comp_tel06,ENDERTEL:publ_tel06,ENDERTEL:bloq_tel06,ENDERTEL:assinante06,ENDERTEL:procon06,ENDERTEL:num_tel07,ENDERTEL:ddd_tel07,ENDERTEL:origem_tel07,ENDERTEL:data_tel07,ENDERTEL:comp_tel07,ENDERTEL:publ_tel07,ENDERTEL:bloq_tel07,ENDERTEL:assinante07,ENDERTEL:procon07,ENDERTEL:num_tel08,ENDERTEL:ddd_tel08,ENDERTEL:origem_tel08,ENDERTEL:data_tel08,ENDERTEL:comp_tel08,ENDERTEL:publ_tel08,ENDERTEL:bloq_tel08,ENDERTEL:assinante08,ENDERTEL:procon08,ENDERTEL:num_tel09,ENDERTEL:ddd_tel09,ENDERTEL:origem_tel09,ENDERTEL:data_tel09,ENDERTEL:comp_tel09,ENDERTEL:publ_tel09,ENDERTEL:bloq_tel09,ENDERTEL:assinante09,ENDERTEL:procon09,ENDERTEL:num_tel10,ENDERTEL:ddd_tel10,ENDERTEL:origem_tel10,ENDERTEL:data_tel10,ENDERTEL:comp_tel10,ENDERTEL:publ_tel10,ENDERTEL:bloq_tel10,ENDERTEL:assinante10,ENDERTEL:procon10")
TBLPROPERTIES("hbase.table.name" = "gdn:basona");

CREATE EXTERNAL TABLE IF NOT EXISTS gdn_brazil.hbase_historicoriscopj (
     key                     string
    ,nu_documento_cad        string
    ,co_tipo_doc_cad         string
    ,co_compldoc_cad         string
    ,nu_seq                  string
    ,co_tipo_reg             string
    ,nu_ordem_exib           string
    ,nu_seq_priori           string
    ,co_valido               string
    ,nu_nsu_cen              string
    ,dt_inicio               string
    ,dt_final                string
    ,co_score_lor            string
    ,hist_model              string
    ,prin                    string
    ,datade                  string
    ,dataate                 string
    ,codret                  string
    ,msgret                  string
    ,hist_default            string
    ,faixai                  string
    ,faixaf                  string
    ,hist_range              string
    ,fatpo                   string
    ,rati1                   string
    ,cfp2                    string
)
STORED BY "org.apache.hadoop.hive.hbase.HBaseStorageHandler"
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,HISTRISK:nu_documento_cad,HISTRISK:co_tipo_doc_cad,HISTRISK:co_compldoc_cad,HISTRISK:nu_seq,HISTRISK:co_tipo_reg,HISTRISK:nu_ordem_exib,HISTRISK:nu_seq_priori,HISTRISK:co_valido,HISTRISK:nu_nsu_cen,HISTRISK:dt_inicio,HISTRISK:dt_final,HISTRISK:co_score_lor,HISTRISK:hist_model,HISTRISK:prin,HISTRISK:datade,HISTRISK:dataate,HISTRISK:codret,HISTRISK:msgret,HISTRISK:hist_default,HISTRISK:faixai,HISTRISK:faixaf,HISTRISK:hist_range,HISTRISK:fatpo,HISTRISK:rati1,HISTRISK:cfp2")
TBLPROPERTIES("hbase.table.name" = "gdn:basona");

CREATE EXTERNAL TABLE IF NOT EXISTS gdn_brazil.hbase_inscricaoestadual (
     key                 string
    ,nu_documento_cad    string
    ,co_tipo_doc_cad     string
    ,co_compldoc_cad     string
    ,nu_seq              string
    ,co_tipo_reg         string
    ,nu_ordem_exib       string
    ,nu_seq_priori       string
    ,co_valido           string
    ,nu_nsu_cen          string
    ,dt_inicio           string
    ,dt_final            string
    ,inscr_est           string
    ,dt_insc             string
    ,status              string
)
STORED BY "org.apache.hadoop.hive.hbase.HBaseStorageHandler"
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,INSCREST:nu_documento_cad,INSCREST:co_tipo_doc_cad,INSCREST:co_compldoc_cad,INSCREST:nu_seq,INSCREST:co_tipo_reg,INSCREST:nu_ordem_exib,INSCREST:nu_seq_priori,INSCREST:co_valido,INSCREST:nu_nsu_cen,INSCREST:dt_inicio,INSCREST:dt_final,INSCREST:inscr_est,INSCREST:dt_insc,INSCREST:status")
TBLPROPERTIES("hbase.table.name" = "gdn:basona");

CREATE EXTERNAL TABLE IF NOT EXISTS gdn_brazil.hbase_participada (
     key                 string
    ,nu_documento_cad    string
    ,co_tipo_doc_cad     string
    ,co_compldoc_cad     string
    ,nu_seq              string
    ,co_tipo_reg         string
    ,nu_ordem_exib       string
    ,nu_seqriori         string
    ,co_valido           string
    ,nusu_cen            string
    ,dt_inicio           string
    ,dt_final            string
    ,chave               string
    ,cdcg                string
    ,cdsq                string
    ,cdcg_erro           string
    ,nman                string
    ,cdeb                string
    ,dscdeb              string
    ,cduf                string
    ,cdfu                string
    ,nrfi                string
    ,seracgc_situacao    string
    ,flgegativos         string
    ,flgeg1              string
    ,qtdeg1              string
    ,flgeg2              string
    ,qtdeg2              string
    ,flgeg3              string
    ,qtdeg3              string
    ,flgeg4              string
    ,qtdeg4              string
    ,flgeg5              string
    ,qtdeg5              string
    ,flgeg6              string
    ,qtdeg6              string
    ,flgeg7              string
    ,qtdeg7              string
    ,flgeg8              string
    ,qtdeg8              string
    ,flgeg9              string
    ,qtdeg9              string
)
STORED BY "org.apache.hadoop.hive.hbase.HBaseStorageHandler"
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,PARTICIPADA:nu_documento_cad,PARTICIPADA:co_tipo_doc_cad,PARTICIPADA:co_compldoc_cad,PARTICIPADA:nu_seq,PARTICIPADA:co_tipo_reg,PARTICIPADA:nu_ordem_exib,PARTICIPADA:nu_seqriori,PARTICIPADA:co_valido,PARTICIPADA:nusu_cen,PARTICIPADA:dt_inicio,PARTICIPADA:dt_final,PARTICIPADA:chave,PARTICIPADA:cdcg,PARTICIPADA:cdsq,PARTICIPADA:cdcg_erro,PARTICIPADA:nman,PARTICIPADA:cdeb,PARTICIPADA:dscdeb,PARTICIPADA:cduf,PARTICIPADA:cdfu,PARTICIPADA:nrfi,PARTICIPADA:seracgc_situacao,PARTICIPADA:flgegativos,PARTICIPADA:flgeg1,PARTICIPADA:qtdeg1,PARTICIPADA:flgeg2,PARTICIPADA:qtdeg2,PARTICIPADA:flgeg3,PARTICIPADA:qtdeg3,PARTICIPADA:flgeg4,PARTICIPADA:qtdeg4,PARTICIPADA:flgeg5,PARTICIPADA:qtdeg5,PARTICIPADA:flgeg6,PARTICIPADA:qtdeg6,PARTICIPADA:flgeg7,PARTICIPADA:qtdeg7,PARTICIPADA:flgeg8,PARTICIPADA:qtdeg8,PARTICIPADA:flgeg9,PARTICIPADA:qtdeg9")
TBLPROPERTIES("hbase.table.name" = "gdn:basona");

CREATE EXTERNAL TABLE IF NOT EXISTS gdn_brazil.hbase_participantes (
     key                 string
    ,nu_documento_cad    string
    ,co_tipo_doc_cad     string
    ,co_compldoc_cad     string
    ,nu_seq              string
    ,co_tipo_reg         string
    ,nu_ordem_exib       string
    ,nu_seqriori         string
    ,co_valido           string
    ,nusu_cen            string
    ,dt_inicio           string
    ,dt_final            string
    ,chave               string
    ,cdcgart             string
    ,cdsqart             string
    ,iddk                string
    ,cdpm                string
    ,cdsqpm              string
    ,cdpm_erro           string
    ,nman                string
    ,dgcrpf              string
    ,cdtppt              string
    ,cdtppthg            string
    ,cdcjam              string
    ,cdcjamhg            string
    ,cdcjck              string
    ,cdcjckhg            string
    ,peaocv              string
    ,petot               string
    ,dtilpt              string 
)
STORED BY "org.apache.hadoop.hive.hbase.HBaseStorageHandler"
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,PARTICIPANTE:nu_documento_cad,PARTICIPANTE:co_tipo_doc_cad,PARTICIPANTE:co_compldoc_cad,PARTICIPANTE:nu_seq,PARTICIPANTE:co_tipo_reg,PARTICIPANTE:nu_ordem_exib,PARTICIPANTE:nu_seqriori,PARTICIPANTE:co_valido,PARTICIPANTE:nusu_cen,PARTICIPANTE:dt_inicio,PARTICIPANTE:dt_final,PARTICIPANTE:chave,PARTICIPANTE:cdcgart,PARTICIPANTE:cdsqart,PARTICIPANTE:iddk,PARTICIPANTE:cdpm,PARTICIPANTE:cdsqpm,PARTICIPANTE:cdpm_erro,PARTICIPANTE:nman,PARTICIPANTE:dgcrpf,PARTICIPANTE:cdtppt,PARTICIPANTE:cdtppthg,PARTICIPANTE:cdcjam,PARTICIPANTE:cdcjamhg,PARTICIPANTE:cdcjck,PARTICIPANTE:cdcjckhg,PARTICIPANTE:peaocv,PARTICIPANTE:petot,PARTICIPANTE:dtilpt")
TBLPROPERTIES("hbase.table.name" = "gdn:basona");

CREATE EXTERNAL TABLE IF NOT EXISTS gdn_brazil.hbase_quadrosocietario (
     key                 string
    ,nu_documento_cad    string
    ,co_tipo_doc_cad     string
    ,co_compldoc_cad     string
    ,nu_seq              string
    ,co_tipo_reg         string
    ,nu_ordem_exib       string
    ,nu_seq_priori       string
    ,co_valido           string
    ,nu_nsu_cen          string
    ,dt_inicio           string
    ,dt_final            string
    ,chave               string
    ,iddk                string
    ,cdpm                string
    ,cdsqpm              string
    ,sqpart              string
    ,dgcrpf              string
    ,nman                string
    ,nrrggl              string
    ,ogrggl              string
    ,cdna                string
    ,cdnahg              string
    ,cdtppt              string
    ,cdtppthg            string
    ,dtilpt              string
    ,petot               string
    ,peaocv              string
    ,cdeb                string
    ,nrfi                string
    ,dtatualiza          string
    ,cdei                string
    ,cdeihg              string
    ,cdpfch              string
    ,dgcrpfch            string
    ,nrrgch              string
    ,ogrgch              string
    ,cdnach              string
    ,cdnachhg            string
    ,cdsa                string
    ,cdsgsa              string
    ,nrdoc               string
    ,sqdoc               string
    ,dtns                string
    ,dtnsch              string
    ,dtfdcg              string
    ,origem              string
    ,qtvlaocv            string
    ,qtvlaosv            string
    ,flg_negativos       string
    ,flg_neg1            string
    ,qtd_neg1            string
    ,flg_neg2            string
    ,qtd_neg2            string
    ,flg_neg3            string
    ,qtd_neg3            string
    ,flg_neg4            string
    ,qtd_neg4            string
    ,flg_neg5            string
    ,qtd_neg5            string
    ,flg_neg6            string
    ,qtd_neg6            string
    ,flg_neg7            string
    ,qtd_neg7            string
    ,flg_neg8            string
    ,qtd_neg8            string
    ,flg_neg9            string
    ,qtd_neg9            string
)
STORED BY "org.apache.hadoop.hive.hbase.HBaseStorageHandler"
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,QUADROSOC:nu_documento_cad,QUADROSOC:co_tipo_doc_cad,QUADROSOC:co_compldoc_cad,QUADROSOC:nu_seq,QUADROSOC:co_tipo_reg,QUADROSOC:nu_ordem_exib,QUADROSOC:nu_seq_priori,QUADROSOC:co_valido,QUADROSOC:nu_nsu_cen,QUADROSOC:dt_inicio,QUADROSOC:dt_final,QUADROSOC:chave,QUADROSOC:iddk,QUADROSOC:cdpm,QUADROSOC:cdsqpm,QUADROSOC:sqpart,QUADROSOC:dgcrpf,QUADROSOC:nman,QUADROSOC:nrrggl,QUADROSOC:ogrggl,QUADROSOC:cdna,QUADROSOC:cdnahg,QUADROSOC:cdtppt,QUADROSOC:cdtppthg,QUADROSOC:dtilpt,QUADROSOC:petot,QUADROSOC:peaocv,QUADROSOC:cdeb,QUADROSOC:nrfi,QUADROSOC:dtatualiza,QUADROSOC:cdei,QUADROSOC:cdeihg,QUADROSOC:cdpfch,QUADROSOC:dgcrpfch,QUADROSOC:nrrgch,QUADROSOC:ogrgch,QUADROSOC:cdnach,QUADROSOC:cdnachhg,QUADROSOC:cdsa,QUADROSOC:cdsgsa,QUADROSOC:nrdoc,QUADROSOC:sqdoc,QUADROSOC:dtns,QUADROSOC:dtnsch,QUADROSOC:dtfdcg,QUADROSOC:origem,QUADROSOC:qtvlaocv,QUADROSOC:qtvlaosv,QUADROSOC:flg_negativos,QUADROSOC:flg_neg1,QUADROSOC:qtd_neg1,QUADROSOC:flg_neg2,QUADROSOC:qtd_neg2,QUADROSOC:flg_neg3,QUADROSOC:qtd_neg3,QUADROSOC:flg_neg4,QUADROSOC:qtd_neg4,QUADROSOC:flg_neg5,QUADROSOC:qtd_neg5,QUADROSOC:flg_neg6,QUADROSOC:qtd_neg6,QUADROSOC:flg_neg7,QUADROSOC:qtd_neg7,QUADROSOC:flg_neg8,QUADROSOC:qtd_neg8,QUADROSOC:flg_neg9,QUADROSOC:qtd_neg9")
TBLPROPERTIES("hbase.table.name" = "gdn:basona");

CREATE EXTERNAL TABLE IF NOT EXISTS gdn_brazil.hbase_filiais (
     key                 string
    ,nu_documento_cad    string
    ,co_tipo_doc_cad     string
    ,co_compldoc_cad     string
    ,nu_seq              string
    ,co_tipo_reg         string
    ,nu_ordem_exib       string
    ,nu_seq_priori       string
    ,co_valido           string
    ,nu_nsu_cen          string
    ,dt_inicio           string
    ,dt_final            string
    ,nu_tot_fil_nrt      string
    ,nu_tot_fil_nrd      string
    ,nu_tot_fil_cto      string
    ,nu_tot_fil_sud      string
    ,nu_tot_fil_sul      string
    ,nu_tot_fil_ac       string
    ,nu_tot_fil_al       string
    ,nu_tot_fil_ap       string
    ,nu_tot_fil_am       string
    ,nu_tot_fil_ba       string
    ,nu_tot_fil_ce       string
    ,nu_tot_fil_df       string
    ,nu_tot_fil_go       string
    ,nu_tot_fil_es       string
    ,nu_tot_fil_ma       string
    ,nu_tot_fil_mt       string
    ,nu_tot_fil_ms       string
    ,nu_tot_fil_mg       string
    ,nu_tot_fil_pa       string
    ,nu_tot_fil_pb       string
    ,nu_tot_fil_pr       string
    ,nu_tot_fil_pe       string
    ,nu_tot_fil_pi       string
    ,nu_tot_fil_rj       string
    ,nu_tot_fil_rn       string
    ,nu_tot_fil_rs       string
    ,nu_tot_fil_ro       string
    ,nu_tot_fil_rr       string
    ,nu_tot_fil_sp       string
    ,nu_tot_fil_sc       string
    ,nu_tot_fil_se       string
    ,nu_tot_fil_to       string 
)
STORED BY "org.apache.hadoop.hive.hbase.HBaseStorageHandler"
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,FILIAIS:nu_documento_cad,FILIAIS:co_tipo_doc_cad,FILIAIS:co_compldoc_cad,FILIAIS:nu_seq,FILIAIS:co_tipo_reg,FILIAIS:nu_ordem_exib,FILIAIS:nu_seq_priori,FILIAIS:co_valido,FILIAIS:nu_nsu_cen,FILIAIS:dt_inicio,FILIAIS:dt_final,FILIAIS:nu_tot_fil_nrt,FILIAIS:nu_tot_fil_nrd,FILIAIS:nu_tot_fil_cto,FILIAIS:nu_tot_fil_sud,FILIAIS:nu_tot_fil_sul,FILIAIS:nu_tot_fil_ac,FILIAIS:nu_tot_fil_al,FILIAIS:nu_tot_fil_ap,FILIAIS:nu_tot_fil_am,FILIAIS:nu_tot_fil_ba,FILIAIS:nu_tot_fil_ce,FILIAIS:nu_tot_fil_df,FILIAIS:nu_tot_fil_go,FILIAIS:nu_tot_fil_es,FILIAIS:nu_tot_fil_ma,FILIAIS:nu_tot_fil_mt,FILIAIS:nu_tot_fil_ms,FILIAIS:nu_tot_fil_mg,FILIAIS:nu_tot_fil_pa,FILIAIS:nu_tot_fil_pb,FILIAIS:nu_tot_fil_pr,FILIAIS:nu_tot_fil_pe,FILIAIS:nu_tot_fil_pi,FILIAIS:nu_tot_fil_rj,FILIAIS:nu_tot_fil_rn,FILIAIS:nu_tot_fil_rs,FILIAIS:nu_tot_fil_ro,FILIAIS:nu_tot_fil_rr,FILIAIS:nu_tot_fil_sp,FILIAIS:nu_tot_fil_sc,FILIAIS:nu_tot_fil_se,FILIAIS:nu_tot_fil_to")
TBLPROPERTIES("hbase.table.name" = "gdn:basona");

CREATE EXTERNAL TABLE IF NOT EXISTS gdn_brazil.hbase_sistercompany (
     key                 string
    ,nu_documento_cad    string
    ,rzsc                string
)
STORED BY "org.apache.hadoop.hive.hbase.HBaseStorageHandler"
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,SISTERCOMP:NU_DOCUMENTO_CAD,SISTERCOMP:RZSC")
TBLPROPERTIES("hbase.table.name" = "gdn:basona");



