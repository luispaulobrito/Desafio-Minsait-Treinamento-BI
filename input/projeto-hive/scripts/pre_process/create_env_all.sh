DADOS=("CLIENTES" "DIVISAO" "ENDERECO" "REGIAO" "VENDAS")

for i in "${DADOS[@]}"
do
    echo "Criando estrutura da pasta $i"
    mkdir ../../raw/$i
    chmod 777 ../../raw/$i
    cd ../../raw/$i
    curl -O https://github.com/caiuafranca/desafio_bigdata_final/blob/main/dados/dados_entrada/$i.csv
    hdfs dfs -mkdir /datalake/raw/$i
    hdfs dfs -chmod 777 /datalake/raw/$i
    hdfs dfs -copyFromLocal $i.csv /datalake/raw/$i
done