#!/bin/bash
###################################################################################################################################
# Script carga semanal gdn brazil
# Autor: Fernando Forenza
# Data: 26/03/2019
# Versao: 1.0
###################################################################################################################################

dataprocessamento=$(date "+%Y%m%d")

echo ">>>>>>>>>>>>>> Inicio do processamento da carga em $dataprocessamento $(date +"%T") >>>>>>>>>>>>>>"
caminhohdfs=/user/hive/warehouse/gdn_brazil.db

sh experian-hdp-ingestion-hdfs-gdn.sh /dados/gdn/entrada

echo ">>>>>>>>>>>>>> limpa todos os arquivos da sensibilizacao, para depois o DataStage copiar novamente e fazer uma nova carga em $dataprocessamento $(date +"%T") >>>>>>>>>>>>>>"
hdfs dfs -rm -f $caminhohdfs/stg_sensibiliza/*
hdfs dfs -rm -f  $caminhohdfs/wrk_sensibiliza/*

echo ">>>>>>>>>>>>>> executa processo spark python para calculo do ultimate parent em $dataprocessamento $(date +"%T") >>>>>>>>>>>>>>"
spark2-submit --master yarn \
--driver-memory 10G \
--executor-memory 6G \
--num-executors 40 \
--executor-cores 3 \
--name br.com.experian.hdp.gdn.UltimateParent \
ultimateparent.py


echo ">>>>>>>>>>>>>> executa processo do spark scala para carregar tabelas da stage, work e hbase $dataprocessamento $(date +"%T") >>>>>>>>>>>>>>"
spark2-submit --master yarn \
--driver-memory 15G \
--executor-memory 10G \
--num-executors 40 \
--executor-cores 3 \
--class br.com.experian.hdp.gdn.GdnBrazil \
gdn-1.0.0.0.jar


if [ $? -eq 0 ]
then
  echo "Script executado com sucesso"
  exit 0
else
  echo "Script executado com falha, verificar" >&2
  exit 1
fi

echo ">>>>>>>>>>>>>>Final do processamento da carga em $dataprocessamento $(date +"%T") >>>>>>>>>>>>>>"


