#!/bin/bash
###################################################################################################################################
# Script gera arquivo gdn brazil
# Autor: Fernando Forenza
# Data: 02/04/2019
# Versao: 1.0
###################################################################################################################################

#criar arquivo de saida do GDN Brazil
caminhohdfs=/user/hive/warehouse/gdn_brazil.db
caminho=/dados/gdn/
dataprocessamento=$(date "+%Y%m%d")
horaprocessamento=$(date "+%H%M%S")

echo @@@@@@@Inicio do processamento da carga em $dataprocessamento $(date +"%T") @@@@@@@
mkdir -p $caminho/saida/
# gerando o arquivo de saida do GDN Brazil
echo @@@@@@@Gerando o arquivo de saida do GDN Brazil@@@@@@@
echo "A|902000001|Serasa Experian|$dataprocessamento|$horaprocessamento|CFR" > $caminho/saida/gdn_final.csv

bee --showHeader=false --outputformat=tsv2 -e 'set mapreduce.map.memory.mb=9000;set mapreduce.map.java.opts=-Xmx7200m;set mapreduce.reduce.memory.mb=9000;set mapreduce.reduce.java.opts=-Xmx7200m; set mapreduce.job.name=br.com.experian.gdn.export.arquivo.saida; select * from gdn_brazil.wrk_arquivosaidagdn order by 3 ;' | sed 's/[\t]/\|/g'  >> $caminho/saida/gdn_final.csv
totalregistrosarq=$(wc -l < $caminho/saida/gdn_final.csv)
totalregistros=$[$totalregistrosarq+$i]
echo "Z|$totalregistros" >> $caminho/saida/gdn_final.csv

cd $caminho/saida/
mv gdn_final.csv GDN.S027.T020.FCCVF.EUT.CBRA.D$(date +"%y%m%d").T$(date +"%H%M%S").txt; zip -m -j GDN.S027.T020.FCCVF.EUT.CBRA.D$(date +"%y%m%d").T$(date +"%H%M%S").zip GDN.S027.T020.FCCVF.EUT.CBRA.D$(date +"%y%m%d").T$(date +"%H%M%S").txt;

# gerando o arquivo de saida do GDN Brazil Filiais
echo @@@@@@@Gerando o arquivo de saida do GDN Brazil Filiais@@@@@@@
echo "A|902000001|Serasa Experian|$dataprocessamento|$horaprocessamento|CVF" > $caminho/saida/gdn_final_fil.csv
bee --showHeader=false --outputformat=tsv2 -e 'set mapreduce.map.memory.mb=9000;set mapreduce.map.java.opts=-Xmx7200m;set mapreduce.reduce.memory.mb=9000;set mapreduce.reduce.java.opts=-Xmx7200m; set mapreduce.job.name=br.com.experian.gdn.export.arquivo.saida.filiais; select * from gdn_brazil.wrk_arquivosaidafil;' | sed 's/[\t]/\|/g'  >> $caminho/saida/gdn_final_fil.csv
totalregistrosarq=$(wc -l < $caminho/saida/gdn_final_fil.csv)
totalregistros=$[$totalregistrosarq+$i]
echo "Z|$totalregistros" >> $caminho/saida/gdn_final_fil.csv

cd $caminho/saida/
mv gdn_final_fil.csv GDN.S027.T020.FCCFR.EUT.CBRA.D$(date +"%y%m%d").T$(date +"%H%M%S").txt; zip -m -j GDN.S027.T020.FCCFR.EUT.CBRA.D$(date +"%y%m%d").T$(date +"%H%M%S").zip GDN.S027.T020.FCCFR.EUT.CBRA.D$(date +"%y%m%d").T$(date +"%H%M%S").txt;

echo @@@@@@@Final do processamento dos arquivos em $dataprocessamento $(date +"%T") @@@@@@@


