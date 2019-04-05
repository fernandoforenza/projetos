#!/bin/bash
###################################################################################################################################
# Script carga arquivos gdn
# Autor: Fernando Forenza - TRIBE CREDIT SERVICES
# Data: 19/02/2019
# Versao: 1.0
###################################################################################################################################

echo "@@@ Início da execução script experian-hdp-ingestion-hdfs-gdn.sh no diretório $1" $(date)

usage()
{
    echo "sh experian-hdp-ingestion-hdfs-gdn.sh /dados/gdn/entrada"
    exit
}

#autentincando o usuario via keytab
#kinit -kt usr_gdn_exec_hdp.keytab usr_gdn_exec_hdp@BR.EXPERIAN.LOCAL

pathHdfs=/user/hive/warehouse/gdn_brazil.db


verificaTamanhoNoHdfs()
{
    eachfile=$1
    path=$2
    existeNoHdfs=$3

    echo "Verificando se o arquivo " $eachfile " no HDFS é o mesmo no path local..."
        md5Hdfs=$(hdfs dfs -cat $existeNoHdfs | md5sum |  awk  '{print $1}')
        md5Local=$(md5sum $path/$eachfile |  awk  '{print $1}')

    if [ $md5Hdfs == $md5Local ]; then
        echo "Arquivo $eachfile é o mesmo no HDFS, removendo o arquivo local"
        rm -f $path/$eachfile
        echo "Arquivo removido com sucesso"
    else
        echo "Arquivo $eachfile já existe no HDFS mas tem tamanho diferente"
        carregaHdfs $eachfile $path $pathHdfs
        #mkdir -p $path/verificar
        #echo "Movendo o arquivo $eachfile para a pasta $path/verificar acionar equipe do projeto!!!"
        # mv $path/$eachfile $path/verificar/$eachfile

    fi
}

verificaSeExistenoHdfs()
{
    eachfile=$1
    path=$2
    pathHdfs=$3

    echo "@@@ Verificando se o arquivo " $eachfile " já existe no HDFS "
        existeNoHdfs=$(hdfs dfs -ls -R $pathHdfs | grep $eachfile | awk  '{print $8}')

    if [ "$existeNoHdfs" == "" ]; then
        echo "@@@ Arquivo não existe copiando o arquivo " $eachfile
        carregaHdfs $eachfile $path $pathHdfs
        verificaTamanhoNoHdfs $eachfile $path $existeNoHdfs
    else
        echo "@@@ Arquivo já existe no HDFS em " $existeNoHdfs
        verificaTamanhoNoHdfs $eachfile $path $existeNoHdfs
    fi
}


if hdfs dfs -test -e $pathHdfs; then
    echo "[$pathHdfs] exists on HDFS"
    #hdfs dfs -ls $pathHdfs/stg_*
fi


#config para freereport
carregaHdfs()
{
    eachfile=$1
    path=$2
    pathHdfs=$3

case $eachfile in
    ADMINIST.TXT)   hdfs dfs -put -f $path/$eachfile $pathHdfs/stg_administradores/$eachfile;;
    ANTECESS.TXT)   hdfs dfs -put -f $path/$eachfile $pathHdfs/stg_antecessoras/$eachfile;;
    CADASTPJ.TXT)   hdfs dfs -put -f $path/$eachfile $pathHdfs/stg_cadastropj/$eachfile;;
    CONSELHO.TXT)   hdfs dfs -put -f $path/$eachfile $pathHdfs/stg_conselho/$eachfile;;
    ENDERTEL.TXT)   hdfs dfs -put -f $path/$eachfile $pathHdfs/stg_enderecotelefone/$eachfile;;
    FILIAIS.TXT )   hdfs dfs -put -f $path/$eachfile $pathHdfs/stg_filiais/$eachfile;;
    HISTRISK.TXT)   hdfs dfs -put -f $path/$eachfile $pathHdfs/stg_historicoriscopj/$eachfile;;
    INSCREST.TXT)   hdfs dfs -put -f $path/$eachfile $pathHdfs/stg_inscricaoestadual/$eachfile;;
    PARTICIP.TXT)   hdfs dfs -put -f $path/$eachfile $pathHdfs/stg_participacoes/$eachfile;;
    QUADRSOC.TXT)   hdfs dfs -put -f $path/$eachfile $pathHdfs/stg_quadrosocietario/$eachfile;;
    sensibiliza.txt)   hdfs dfs -put -f $path/$eachfile $pathHdfs/stg_sensibiliza/$eachfile;;
*)  echo "@@@ Nome do arquivo não está no padrão para processamento " ;;
esac

}

if [ -n "$1" ]; then
  echo "@@@ Executando o script no diretório $1"
  path=$1
  if [ -d "$path" ]; then
    arquivos=$(find $path -type f -printf "%f\n")
    echo "@@@ Processando a lista de arquivos - >" $arquivos

    for eachfile in $arquivos
    do
         printf "\n\n"
        verificaSeExistenoHdfs $eachfile $path $pathHdfs
    done
  fi
else
  echo "@@@ Informe o diretório que deseja copiar os arquivos para o hdfs. Exemplo:"
  usage
fi

echo "@@@ Término da execução " $(date)


