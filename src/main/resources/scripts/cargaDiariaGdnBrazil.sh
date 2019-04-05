#!/bin/bash
###################################################################################################################################
# Script carga diaria gdn brazil
# Autor: Fernando Forenza
# Data: 02/04/2019
# Versao: 1.0
###################################################################################################################################

dataprocessamento=$(date "+%Y%m%d")

echo ">>>>>>>>>>>>>> Inicio do processamento da carga em $dataprocessamento $(date +"%T") >>>>>>>>>>>>>>"
caminhohdfs=/user/hive/warehouse/gdn_brazil.db

sh experian-hdp-ingestion-hdfs-gdn.sh /dados/gdn/entrada

echo ">>>>>>>>>>>>>> executa processo do spark scala para carregar tabelas da stage, work e hbase da sensibilizacao $dataprocessamento $(date +"%T") >>>>>>>>>>>>>>"
spark2-submit --master yarn \
--driver-memory 10G \
--executor-memory 6G \
--num-executors 40 \
--executor-cores 3 \
--class  br.com.experian.hdp.gdn.Sensibilizacao \
gdn-1.0-SNAPSHOT.jar


if [ $? -eq 0 ]
then
  echo "Script executado com sucesso"
  exit 0
else
  echo "Script executado com falha, verificar" >&2
  exit 1
fi

echo ">>>>>>>>>>>>>>Final do processamento da carga em $dataprocessamento $(date +"%T") >>>>>>>>>>>>>>"


