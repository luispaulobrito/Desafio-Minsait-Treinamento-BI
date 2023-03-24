DADOS=("clientes" "divisao" "endereco" "regiao" "vendas")

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
    beeline -u jdbc:hive2://localhost:10000 -f ../../scripts/hql/create_table_$i.hql
    beeline -u jdbc:hive2://localhost:10000 -e "SELECT count(*) as quantidade from desafio.${i};"
done