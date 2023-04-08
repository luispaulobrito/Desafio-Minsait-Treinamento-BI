from pyspark.sql import SparkSession, dataframe
from pyspark.sql.functions import when, col, sum, count, isnan, round
from pyspark.sql.functions import regexp_replace, concat_ws, sha2, rtrim, substring
from pyspark.sql.functions import unix_timestamp, from_unixtime, to_date
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import DoubleType, IntegerType, StringType
from pyspark.sql import HiveContext
from pyspark.sql.functions import year, month, dayofmonth, quarter
from pyspark.sql.types import DecimalType
from pyspark.sql.functions import trim, regexp_replace, when, col
from pyspark.sql.functions import regexp_replace

import os
import re

from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import when

spark = SparkSession.builder.master("local[*]")\
    .enableHiveSupport()\
    .getOrCreate()

# Criando dataframes diretamente do Hive
df_endereco = spark.read.table("desafio_curso.endereco")
rdd = df_endereco.rdd.zipWithIndex().filter(lambda x: x[1] > 0).map(lambda x: x[0])
df_endereco = rdd.toDF(df_endereco.schema)

df_clientes = spark.read.table("desafio_curso.clientes")
rdd = df_clientes.rdd.zipWithIndex().filter(lambda x: x[1] > 0).map(lambda x: x[0])
df_clientes = rdd.toDF(df_clientes.schema)

df_divisao = spark.read.table("desafio_curso.divisao")
rdd = df_divisao.rdd.zipWithIndex().filter(lambda x: x[1] > 0).map(lambda x: x[0])
df_divisao = rdd.toDF(df_divisao.schema)

df_regiao = spark.read.table("desafio_curso.regiao")
rdd = df_regiao.rdd.zipWithIndex().filter(lambda x: x[1] > 0).map(lambda x: x[0])
df_regiao = rdd.toDF(df_regiao.schema)

df_vendas = spark.read.table("desafio_curso.vendas")
rdd = df_vendas.rdd.zipWithIndex().filter(lambda x: x[1] > 0).map(lambda x: x[0])
df_vendas = rdd.toDF(df_vendas.schema)

# Espaço para tratar e juntar os campos e a criação do modelo dimensional
#limpar linhas duplicadas em clientes e endereço
df_clientes = df_clientes.dropDuplicates(['customer_key'])
df_endereco = df_endereco.dropDuplicates(['address_number'])

#Junção das tabelas

df_vendas = df_vendas.withColumnRenamed("customer_key", "customer_key_vendas")
df_endereco = df_endereco.withColumnRenamed("address_number", "address_number_endereco")
df_divisao = df_divisao.withColumnRenamed("division", "division_divisao")
df_regiao = df_regiao.withColumnRenamed("region_code", "region_code_regiao")

df_stage = df_vendas.join(df_clientes,df_vendas.customer_key_vendas == df_clientes.customer_key,"left")
df_stage = df_stage.join(df_endereco,df_stage.address_number == df_endereco.address_number_endereco,"left")
df_stage = df_stage.join(df_divisao,df_stage.division == df_divisao.division_divisao,"left")
df_stage = df_stage.join(df_regiao,df_stage.region_code == df_regiao.region_code_regiao,"left")

#Adicionar colunas Ano, Mês, Dia, Trimestre tomando como base a coluna invoice_date

df_stage = df_stage.withColumn('invoice_date', to_date(col('invoice_date'), 'dd/MM/yyyy'))

df_stage = df_stage \
    .withColumn('Ano', year('invoice_date')) \
    .withColumn('Mes', month('invoice_date')) \
    .withColumn('Dia', dayofmonth('invoice_date')) \
    .withColumn('Trimestre', quarter('invoice_date'))  

#Campos decimais ou inteiros nulos ou vazios, sendo preenchidos por 0.
cols_to_check = ['item_number', 'discount_amount', 'list_price', 'sales_amount', 'sales_amount_based_on_list_price', 'sales_cost_amount', 'sales_margin_amount', 'sales_price', 'line_number', 'sales_quantity']

for col_name in cols_to_check:
     df_stage = df_stage.withColumn(col_name, when(col(col_name) == '', 0).otherwise(col(col_name)))
#Campos strings vazios preenchidos com 'Não informado'
all_columns = df_stage.columns

for column in all_columns:
   df_stage = df_stage.withColumn(column, 
                                  when(trim(regexp_replace(col(column), '\n', 'null')) == "", "Nao Informado")
                                  .otherwise(col(column)))
#Campos strings nulos preenchidos com 'Não informado'
df_stage = df_stage.fillna("Nao Informado")

#Adicionar chaves estrangeiras
df_stage = df_stage.withColumn('PK_TEMPO', sha2(concat_ws("",df_stage.invoice_date, df_stage.Ano,df_stage.Mes,df_stage.Dia,df_stage.Trimestre), 256))

df_stage = df_stage.withColumn('PK_CLIENTES', sha2(concat_ws("",df_stage.customer_key,df_stage.customer,df_stage.business_family_name,df_stage.business_unit,df_stage.customer_type,df_stage.division,df_stage.line_of_business,df_stage.phone,df_stage.region_code,df_stage.regional_sales_mgr,df_stage.search_type), 256))

df_stage = df_stage.withColumn('PK_LOCALIDADE', sha2(concat_ws("",df_stage.address_number,df_stage.city,df_stage.country,df_stage.state,df_stage.zip_code,df_stage.division,df_stage.division_name,df_stage.region_code,df_stage.region_name,df_stage.customer_address_1,df_stage.customer_address_2,df_stage.customer_address_3,df_stage.customer_address_4), 256))

# criando o fato
df_stage.createOrReplaceTempView("stage")

ft_vendas = spark.sql("SELECT PK_CLIENTES, PK_TEMPO, PK_LOCALIDADE, actual_delivery_date, date_key, discount_amount, invoice_date, invoice_number, item_class, item_number, item, line_number, list_price, order_number, promised_delivery_date, sales_amount AS valor_de_venda, sales_amount_based_on_list_price, sales_cost_amount, sales_margin_amount, sales_price, sales_quantity AS quantidade, sales_rep, u_m FROM stage")

#criando as dimensões
df_tempo = spark.sql("SELECT DISTINCT PK_TEMPO, invoice_date, Ano, Mes, Dia, Trimestre FROM stage") 
df_clientes = spark.sql("SELECT DISTINCT PK_CLIENTES, customer_key, customer, business_family_name, business_unit, customer_type, division, line_of_business, phone, region_code, regional_sales_mgr, search_type FROM stage")
df_localidade = spark.sql("SELECT DISTINCT PK_LOCALIDADE, address_number, city, country, state, zip_code, division, division_name, region_code, region_name, customer_address_1, customer_address_2, customer_address_3, customer_address_4 FROM stage")

# função para salvar os dados
def salvar_df(df, file):
    output = "input/projeto-hive/gold/" + file
    erase = "hdfs dfs -rm " + output + "/*"
    rename = "hdfs dfs -get /datalake/gold/"+file+"/part-* input/projeto-hive/gold/"+file+".csv"
    
    print(rename)    
    
    df.coalesce(1).write\
        .format("csv")\
        .option("header", True)\
        .option("delimiter", ";")\
        .mode("overwrite")\
        .save("/datalake/gold/"+file+"/")
    
    os.system(erase)
    os.system(rename)

#Exportar dataframes como tabelas csv
salvar_df(df_tempo, 'dim_tempo')
salvar_df(df_clientes, 'dim_clientes')
salvar_df(df_localidade, 'dim_localidade')
salvar_df(ft_vendas, 'ft_vendas')


#TESTES

# 1 - soma de sales_amount
print("Total de valor de vendas:")
ft_vendas = ft_vendas.withColumn("valor_de_venda", regexp_replace("valor_de_venda", ",", "."))
resultado = ft_vendas.agg({"valor_de_venda": "sum"}).withColumnRenamed("sum(valor_de_venda)", "total_vendas")
resultado_decimal = resultado.select(resultado["total_vendas"].cast(DecimalType(18, 2)).alias("total_vendas_decimal"))
resultado_decimal.show()

# 2 - soma de sales_quantity
print("Total produtos vendidos:")
total_vendas = ft_vendas.agg(sum('quantidade').alias('total_vendas'))
total_vendas.show()


# 3 - produto mais vendido
print("Produto mais vendido:")
agrupado_por_item = ft_vendas.groupBy('item').agg(sum('quantidade').alias('quantidade_total'))
produto_mais_vendido = agrupado_por_item.orderBy('quantidade_total', ascending=False).limit(1)
produto_mais_vendido.show()

# 4 - 5 produtos mais sales_quantity
print("5 Produtos mais vendidos:")
resultado = ft_vendas.groupBy('item').agg(sum('quantidade').alias('quantidade_total')) \
            .orderBy('quantidade_total', ascending=False) \
            .limit(5)
resultado.show()

# 5 - 5 produtos mais sales_amount
print("5 Produtos com maior valor de venda:")
resultado = ft_vendas.groupBy('item').agg(sum('valor_de_venda').alias('valor_total_vendas')) \
            .orderBy('valor_total_vendas', ascending=False) \
            .limit(5) \
            .withColumn('valor_total_vendas', col('valor_total_vendas').cast(DecimalType(18, 2)))

resultado.show()

# 6 - sales_quantity por mes e  sales_amount por mes
print("Valor de venda e quantidade de vendas por mês:")
resultado = ft_vendas.join(df_tempo, ft_vendas.PK_TEMPO == df_tempo.PK_TEMPO) \
            .groupBy('mes') \
            .agg(sum('quantidade').alias('quantidade_total'), sum('valor_de_venda').alias('valor_total_vendas')) \
            .orderBy('mes')\
            .withColumn('valor_total_vendas', col('valor_total_vendas').cast(DecimalType(18, 2)))
resultado.show()

# 7 - sales quantity por country - mostrar porcentagem
print("Quantidade de venda em moeda e quantidade de venda em porcentagem por país:")
from pyspark.sql.functions import sum, format_string
total_vendas = ft_vendas.agg({"valor_de_venda": "sum"}).collect()[0][0]

resultado = ft_vendas.join(df_localidade, ft_vendas.PK_LOCALIDADE == df_localidade.PK_LOCALIDADE) \
            .groupBy('country') \
            .agg(sum('valor_de_venda').alias('valor_total_vendas'), (sum('valor_de_venda')/total_vendas*100).alias('porcentagem')) \
            .orderBy('porcentagem', ascending=False)\
            .withColumn('porcentagem', col('porcentagem').cast(DecimalType(18, 2)))\
            .withColumn('valor_total_vendas', col('valor_total_vendas').cast(DecimalType(18, 2)))
resultado.show()

# 8 - sales_amount por ano
print("Valor de venda por ano:")
resultado = ft_vendas.join(df_tempo, ft_vendas.PK_TEMPO == df_tempo.PK_TEMPO) \
            .groupBy('ano') \
            .agg(sum('valor_de_venda').alias('valor_total_vendas')) \
            .orderBy('ano')\
            .withColumn('valor_total_vendas', col('valor_total_vendas').cast(DecimalType(18, 2)))
resultado.show()

# 9 - tabela com customer, sales_amount, country. Ordenar melhores clientes pelo valor de vendas 10 melhores
# e mostrar o total de valor de venda dos 10 melhores
resultado = ft_vendas.join(df_clientes, ft_vendas.PK_CLIENTES == df_clientes.PK_CLIENTES) \
            .join(df_localidade, ft_vendas.PK_LOCALIDADE == df_localidade.PK_LOCALIDADE) \
            .groupBy('customer', 'country') \
            .agg(sum('valor_de_venda').alias('valor_total_vendas')) \
            .orderBy('valor_total_vendas', ascending=False) \
            .withColumn('valor_total_vendas', col('valor_total_vendas').cast(DecimalType(18, 2)))\
            .limit(10)
resultado.show()
total_vendas = resultado.agg({"valor_total_vendas": "sum"}).collect()[0][0]
print("Total de vendas dos 10 melhores clientes: ", total_vendas)