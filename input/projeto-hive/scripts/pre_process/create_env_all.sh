DADOS=("CLIENTES" "DIVISAO" "ENDERECO" "REGIAO" "VENDAS")

for i in "${DADOS[@]}"
do
    echo "Criando estrutura da pasta $i"
    mkdir ../../raw/$i
    chmod 777 ../../raw/$i
    cd ../../raw/$i
    curl -O https://raw.githubusercontent.com/caiuafranca/desafio_bigdata_final/main/dados/dados_entrada/$i.csv
    hdfs dfs -mkdir /datalake/raw/$i
    hdfs dfs -chmod 777 /datalake/raw/$i
    hdfs dfs -copyFromLocal $i.csv /datalake/raw/$i
    beeline -u jdbc:hive2://localhost:10000 -f ../../scripts/hql/create_table_$i.hql
done