--###################################################################################################################################
--# Script hive metadados gdn brazil
--# Autor: Fernando Forenza
--# Data: 25/03/2019
--# Versao 1.0
--###################################################################################################################################

USE gdn_brazil; 

CREATE TABLE IF NOT EXISTS gdn_brazil.stg_administradores (
    nu_documento_cad     string
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
    ,nrrggl              string
    ,ogrggl              string
    ,cdna                string
    ,cdnahg              string
    ,cdei                string
    ,cdeihg              string
    ,cdcjam              string
    ,cdcjamhg            string
    ,dtilam              string
    ,dtfmam              string
    ,tmdu                string
    ,dtam                string
    ,dtns                string
    ,nrfi                string
    ,dtatualiza          string
    ,cdpfch              string
    ,dtnsch              string
    ,nrrgch              string
    ,orrgch              string
    ,cdnach              string
    ,cdnachhg            string
    ,dtfdcg_cg           string
    ,origem              string
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
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|';
        
CREATE TABLE IF NOT EXISTS gdn_brazil.stg_antecessoras (
    nu_documento_cad   string
    ,co_tipo_doc_cad   string
    ,co_compldoc_cad   string
    ,nu_seq            string
    ,co_tipo_reg       string
    ,nu_ordem_exib     string
    ,nu_seq_priori     string
    ,co_valido         string
    ,nu_nsu_cen        string
    ,dt_inicio         string
    ,dt_final          string
    ,qtde              string
    ,antec1            string
    ,antec1_dtmt       string
    ,antec2            string
    ,antec2_dtmt       string
    ,antec3            string
    ,antec3_dtmt       string
    ,antec4            string
    ,antec4_dtmt       string
    ,antec5            string
    ,antec5_dtmt       string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|';

CREATE TABLE IF NOT EXISTS gdn_brazil.stg_cadastropj (
    nu_documento_cad                    string
    ,co_tipo_doc_cad                    string
    ,co_compldoc_cad                    string
    ,nu_seq                             string
    ,co_tipo_reg                        string
    ,nu_ordem_exib                      string
    ,nu_seq_priori                      string
    ,co_valido                          string
    ,nu_nsu_cen                         string
    ,dt_inicio                          string
    ,dt_final                           string
    ,cdmt                               string
    ,rzsc                               string
    ,dtutmf                             string
    ,hrutmf                             string
    ,totqtvlcv                          string
    ,topeaocv                           string
    ,cdog                               string
    ,cdtpsc                             string
    ,dtutrg                             string
    ,nrutrg                             string
    ,cdsa                               string
    ,cdsgsa                             string
    ,cpsc                               string
    ,cpra                               string
    ,dtfdcg                             string
    ,cpar                               string
    ,nreg                               string
    ,dtblgf                             string
    ,qtvlnu                             string
    ,cdcrao                             string
    ,cdim                               string
    ,cdep                               string
    ,nmlf                               string
    ,nrtflf                             string
    ,nmfs                               string
    ,cdrcfe                             string
    ,cpaz                               string
    ,cdttii                             string
    ,cddalf                             string
    ,peepvd                             string
    ,peimcf                             string
    ,cder                               string
    ,nmgrec                             string
    ,toqtvlsv                           string
    ,cdrblf                             string
    ,cdna                               string
    ,qtvlnonu                           string
    ,topeaosv                           string
    ,cdstjtem                           string
    ,cdat                               string
    ,cdstcg                             string
    ,dtatrz                             string
    ,dtatpo                             string
    ,e3v2                               string
    ,dtgsgp                             string
    ,flfmcn                             string
    ,dtfg                               string
    ,cdrcfen                            string
    ,dtininoramo                        string
    ,cdinstsede                         string
    ,nu_nire                            string
    ,co_sgrcfen                         string
    ,co_ctcapit                         string
    ,co_origem_cap                      string
    ,co_relviscdc                       string
    ,nu_sq_ultaltco                     string
    ,co_incdqcadpjb                     string
    ,co_ind_ativsoc                     string
    ,co_sitjunta                        string
    ,nu_inscrest                        string
    ,nu_inscrmun                        string
    ,co_proc_at                         string
    ,dt_inscreceit                      string
    ,dt_cadastro                        string
    ,co_ind_filial                      string
    ,co_ativserasa                      string
    ,nu_matriz                          string
    ,co_opc_simples                     string
    ,dt_inf_simples                     string
    ,co_situacaon                       string
    ,dt_indoperac                       string
    ,co_indempmei                       string
    ,co_situacao                        string
    ,co_naturrec                        string
    ,dt_alterarf                        string
    ,qt_filiarf                         string
    ,co_opcsimei                        string
    ,dt_sitsimei                        string
    ,dt_sitsimpl                        string
    ,co_ativrecv                        string
    ,co_cnaesec1                        string
    ,co_cnaesec2                        string
    ,co_cnaesec3                        string
    ,co_cnaesec4                        string
    ,co_cnaesec5                        string
    ,co_cnaesec6                        string
    ,co_cnaesec7                        string
    ,co_cnaesec8                        string
    ,co_cnaesec9                        string
    ,co_cnaesec10                       string
    ,co_natur_recn_emp                  string
    ,dt_situac_emp                      string
    ,email                              string
    ,co_internmail                      string
    ,co_internsite                      string
    ,de_opsimei_ant                     string
    ,porte                              string
    ,ds_porte                           string
    ,dtemisficha                        string
    ,vl1conta                           string
    ,dt1_balanco                        string
    ,vl2conta                           string
    ,dt2_balanco                        string
    ,vl3conta                           string
    ,dt3_balanco                        string
    ,vl4conta                           string
    ,dt4_balanco                        string
    ,vl5conta                           string
    ,dt5_balanco                        string
    ,ipalerta                           string
    ,gfclient                           string
    ,co_grpecon                         string
    ,no_grpoecon                        string
    ,co_vincasso                        string
    ,controlagrp                        string
    ,flagneg                            string
    ,neg_flgprot                        string
    ,neg_qtdprot                        string
    ,neg_flgacao                        string
    ,neg_qtdacao                        string
    ,neg_flgfale                        string
    ,neg_qtdfale                        string
    ,neg_flgconc                        string
    ,neg_qtdconc                        string
    ,neg_flgrefi                        string
    ,neg_qtdrefi                        string
    ,neg_flgache                        string
    ,neg_qtdache                        string
    ,neg_flgccf                         string
    ,neg_qtdccf                         string
    ,neg_flgpefi                        string
    ,neg_qtdpefi                        string
    ,neg_flgconv                        string
    ,neg_qtdconv                        string
    ,neg_cheque                         string
    ,neg_qtdcheq                        string
    ,codisic                            string
    ,descisic                           string
    ,descricaonjport                    string
    ,descricaonjingles                  string
    ,indicadornjsfl                     string
    ,codigoserasapadrao                 string
    ,novocodigoserasa                   string
    ,descricaocodigoserasa_port         string
    ,descricaoresumidacodserasa_port    string
    ,descricaocodigoserasa_eng          string
    ,descricaoresumidacodserasa_eng     string
    ,cnaedescricao                      string
    ,ultimateparent                     string
    ,totalparticipacao                  string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|';

CREATE TABLE IF NOT EXISTS gdn_brazil.stg_conselho(
    nu_documento_cad                    string
    ,co_tipo_doc_cad                    string
    ,co_compldoc_cad                    string
    ,nu_seq                             string
    ,co_tipo_reg                        string
    ,nu_ordem_exib                      string
    ,nu_seqriori                        string
    ,co_valido                          string
    ,nusu_cen                           string
    ,dt_inicio                          string
    ,dt_final                           string
    ,chave                              string
    ,iddk                               string
    ,cdpm                               string
    ,cdsqpm                             string
    ,dgcrpf                             string
    ,nman                               string
    ,cdcjck                             string
    ,dtfmck                             string
    ,cdcjckhg                           string
    ,nrrggl                             string
    ,ogrggl                             string
    ,cdna                               string
    ,cdnahg                             string
    ,cdei                               string
    ,cdeihg                             string
    ,dtilck                             string
    ,dtns                               string
    ,col29                              string
    ,flgegativos                        string
    ,flgeg1                             string
    ,qtdeg1                             string
    ,flgeg2                             string
    ,qtdeg2                             string
    ,flgeg3                             string
    ,qtdeg3                             string
    ,flgeg4                             string
    ,qtdeg4                             string
    ,flgeg5                             string
    ,qtdeg5                             string
    ,flgeg6                             string
    ,qtdeg6                             string
    ,flgeg7                             string
    ,qtdeg7                             string
    ,flgeg8                             string
    ,qtdeg8                             string
    ,flgeg9                             string
    ,qtdeg9                             string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|';

CREATE TABLE IF NOT EXISTS gdn_brazil.stg_enderecotelefone (
    nu_documento_cad                     string
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
    ,peso                                string
    ,comp_end                            string
    ,publ_end                            string
    ,bloq_end                            string
    ,cep_valido                          string
    ,endereco                            string
    ,bairro                              string
    ,municipio                           string
    ,uf                                  string
    ,cep                                 string
    ,dt_informacao                       string
    ,nsu                                 string
    ,origem_end                          string
    ,ordem_tel                           string
    ,qtde_tel                            string
    ,num_tel01                           string
    ,ddd_tel01                           string
    ,origem_tel01                        string
    ,data_tel01                          string
    ,comp_tel01                          string
    ,publ_tel01                          string
    ,bloq_tel01                          string
    ,assinante01                         string
    ,procon01                            string
    ,num_tel02                           string
    ,ddd_tel02                           string
    ,origem_tel02                        string
    ,data_tel02                          string
    ,comp_tel02                          string
    ,publ_tel02                          string
    ,bloq_tel02                          string
    ,assinante02                         string
    ,procon02                            string
    ,num_tel03                           string
    ,ddd_tel03                           string
    ,origem_tel03                        string
    ,data_tel03                          string
    ,comp_tel03                          string
    ,publ_tel03                          string
    ,bloq_tel03                          string
    ,assinante03                         string
    ,procon03                            string
    ,num_tel04                           string
    ,ddd_tel04                           string
    ,origem_tel04                        string
    ,data_tel04                          string
    ,comp_tel04                          string
    ,publ_tel04                          string
    ,bloq_tel04                          string
    ,assinante04                         string
    ,procon04                            string
    ,num_tel05                           string
    ,ddd_tel05                           string
    ,origem_tel05                        string
    ,data_tel05                          string
    ,comp_tel05                          string
    ,publ_tel05                          string
    ,bloq_tel05                          string
    ,assinante05                         string
    ,procon05                            string
    ,num_tel06                           string
    ,ddd_tel06                           string
    ,origem_tel06                        string
    ,data_tel06                          string
    ,comp_tel06                          string
    ,publ_tel06                          string
    ,bloq_tel06                          string
    ,assinante06                         string
    ,procon06                            string
    ,num_tel07                           string
    ,ddd_tel07                           string
    ,origem_tel07                        string
    ,data_tel07                          string
    ,comp_tel07                          string
    ,publ_tel07                          string
    ,bloq_tel07                          string
    ,assinante07                         string
    ,procon07                            string
    ,num_tel08                           string
    ,ddd_tel08                           string
    ,origem_tel08                        string
    ,data_tel08                          string
    ,comp_tel08                          string
    ,publ_tel08                          string
    ,bloq_tel08                          string
    ,assinante08                         string
    ,procon08                            string
    ,num_tel09                           string
    ,ddd_tel09                           string
    ,origem_tel09                        string
    ,data_tel09                          string
    ,comp_tel09                          string
    ,publ_tel09                          string
    ,bloq_tel09                          string
    ,assinante09                         string
    ,procon09                            string
    ,num_tel10                           string
    ,ddd_tel10                           string
    ,origem_tel10                        string
    ,data_tel10                          string
    ,comp_tel10                          string
    ,publ_tel10                          string
    ,bloq_tel10                          string
    ,assinante10                         string
    ,procon10                            string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|';

CREATE TABLE IF NOT EXISTS gdn_brazil.stg_historicoriscopj (
    nu_documento_cad           string
    ,co_tipo_doc_cad           string
    ,co_compldoc_cad           string
    ,nu_seq                    string
    ,co_tipo_reg               string
    ,nu_ordem_exib             string
    ,nu_seq_priori             string
    ,co_valido                 string
    ,nu_nsu_cen                string
    ,dt_inicio                 string
    ,dt_final                  string
    ,co_score_lor              string
    ,hist_model                string
    ,prin                      string
    ,datade                    string
    ,dataate                   string
    ,codret                    string
    ,msgret                    string
    ,hist_default              string
    ,faixai                    string
    ,faixaf                    string
    ,hist_range                string
    ,fatpo                     string
    ,rati1                     string
    ,cfp2                      string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|';

CREATE TABLE IF NOT EXISTS gdn_brazil.stg_inscricaoestadual (
    nu_documento_cad   string
    ,co_tipo_doc_cad   string
    ,co_compldoc_cad   string
    ,nu_seq            string
    ,co_tipo_reg       string
    ,nu_ordem_exib     string
    ,nu_seq_priori     string
    ,co_valido         string
    ,nu_nsu_cen        string
    ,dt_inicio         string
    ,dt_final          string
    ,inscr_est         string
    ,dt_insc           string
    ,status            string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|';

CREATE TABLE IF NOT EXISTS gdn_brazil.stg_participacoes (
    col01       string
    ,col02      string
    ,col03      string
    ,col04      string
    ,col05      string
    ,col06      string
    ,col07      string
    ,col08      string
    ,col09      string
    ,col10      string
    ,col11      string
    ,col12      string
    ,col13      string
    ,col14      string
    ,col15      string
    ,col16      string
    ,col17      string
    ,col18      string
    ,col19      string
    ,col20      string
    ,col21      string
    ,col22      string
    ,col23      string
    ,col24      string
    ,col25      string
    ,col26      string
    ,col27      string
    ,col28      string
    ,col29      string
    ,col30      string
    ,col31      string
    ,col32      string
    ,col33      string
    ,col34      string
    ,col35      string
    ,col36      string
    ,col37      string
    ,col38      string
    ,col39      string
    ,col40      string
    ,col41      string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|';

CREATE TABLE IF NOT EXISTS gdn_brazil.stg_quadrosocietario (
    nu_documento_cad    string
    ,co_tipo_doc_cad    string
    ,co_compldoc_cad    string
    ,nu_seq             string
    ,co_tipo_reg        string
    ,nu_ordem_exib      string
    ,nu_seq_priori      string
    ,co_valido          string
    ,nu_nsu_cen         string
    ,dt_inicio          string
    ,dt_final           string
    ,chave              string
    ,iddk               string
    ,cdpm               string
    ,cdsqpm             string
    ,sqpart             string
    ,dgcrpf             string
    ,nman               string
    ,nrrggl             string
    ,ogrggl             string
    ,cdna               string
    ,cdnahg             string
    ,cdtppt             string
    ,cdtppthg           string
    ,dtilpt             string
    ,petot              string
    ,peaocv             string
    ,cdeb               string
    ,nrfi               string
    ,dtatualiza         string
    ,cdei               string
    ,cdeihg             string
    ,cdpfch             string
    ,dgcrpfch           string
    ,nrrgch             string
    ,ogrgch             string
    ,cdnach             string
    ,cdnachhg           string
    ,cdsa               string
    ,cdsgsa             string
    ,nrdoc              string
    ,sqdoc              string
    ,dtns               string
    ,dtnsch             string
    ,dtfdcg             string
    ,origem             string
    ,qtvlaocv           string
    ,qtvlaosv           string
    ,flg_negativos      string
    ,flg_neg1           string
    ,qtd_neg1           string
    ,flg_neg2           string
    ,qtd_neg2           string
    ,flg_neg3           string
    ,qtd_neg3           string
    ,flg_neg4           string
    ,qtd_neg4           string
    ,flg_neg5           string
    ,qtd_neg5           string
    ,flg_neg6           string
    ,qtd_neg6           string
    ,flg_neg7           string
    ,qtd_neg7           string
    ,flg_neg8           string
    ,qtd_neg8           string
    ,flg_neg9           string
    ,qtd_neg9           string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|';

CREATE TABLE IF NOT EXISTS gdn_brazil.stg_filiais (
    nu_documento_cad    string 
    ,co_tipo_doc_cad    string
    ,co_compldoc_cad    string
    ,nu_seq             string
    ,co_tipo_reg        string
    ,nu_ordem_exib      string
    ,nu_seq_priori      string
    ,co_valido          string
    ,nu_nsu_cen         string
    ,dt_inicio          string
    ,dt_final           string
    ,nu_tot_fil_nrt     string
    ,nu_tot_fil_nrd     string
    ,nu_tot_fil_cto     string
    ,nu_tot_fil_sud     string
    ,nu_tot_fil_sul     string
    ,nu_tot_fil_ac      string
    ,nu_tot_fil_al      string
    ,nu_tot_fil_ap      string
    ,nu_tot_fil_am      string
    ,nu_tot_fil_ba      string
    ,nu_tot_fil_ce      string
    ,nu_tot_fil_df      string
    ,nu_tot_fil_go      string
    ,nu_tot_fil_es      string
    ,nu_tot_fil_ma      string
    ,nu_tot_fil_mt      string
    ,nu_tot_fil_ms      string
    ,nu_tot_fil_mg      string
    ,nu_tot_fil_pa      string
    ,nu_tot_fil_pb      string
    ,nu_tot_fil_pr      string
    ,nu_tot_fil_pe      string
    ,nu_tot_fil_pi      string
    ,nu_tot_fil_rj      string
    ,nu_tot_fil_rn      string
    ,nu_tot_fil_rs      string
    ,nu_tot_fil_ro      string
    ,nu_tot_fil_rr      string
    ,nu_tot_fil_sp      string
    ,nu_tot_fil_sc      string
    ,nu_tot_fil_se      string
    ,nu_tot_fil_to      string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|';

CREATE TABLE IF NOT EXISTS gdn_brazil.stg_countrys (
    country     string
    ,iso         string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|';

CREATE EXTERNAL TABLE IF NOT EXISTS gdn_brazil.stg_isic (
    codisic     string
    ,isic       string
    ,codcnae    string
    ,cnae       string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|';

CREATE EXTERNAL TABLE IF NOT EXISTS gdn_brazil.stg_naturezajuridica (
    codreceita          string
    ,descricaoport      string
    ,descricaoingles    string
    ,indicadorsfl       string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|';

CREATE EXTERNAL TABLE IF NOT EXISTS gdn_brazil.stg_descricaocodserasa (
    codigoserasa                    string
    ,segmentocodigoserasa           string
    ,codigoserasapadrao             string
    ,descricaocodigoserasa          string
    ,descricaoresumidacodserasa     string
    ,codigoidioma                   string
    ,novocodigoserasa               string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|';

CREATE EXTERNAL TABLE IF NOT EXISTS gdn_brazil.stg_sensibiliza (
    tp_doc              string
    ,nu_doc             string
    ,tp_sensibiliza     string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|';

CREATE EXTERNAL TABLE IF NOT EXISTS gdn_brazil.stg_sistcompsm (
    nu_documento_cad    string
    ,cdpm               string
    ,petot              string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|';







