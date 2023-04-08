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

# Carregar tabelas endereco e remover a primeira linha do cabeçalho

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

# 1 - soma de sales_amount
print("Total de valor de vendas:")
df_vendas = df_vendas.withColumn("sales_amount", regexp_replace("sales_amount", ",", "."))
resultado = df_vendas.agg({"sales_amount": "sum"}).withColumnRenamed("sum(sales_amount)", "total_vendas")
resultado_decimal = resultado.select(resultado["total_vendas"].cast(DecimalType(18, 2)).alias("total_vendas_decimal"))
resultado_decimal.show()

# 2 - soma de sales_quantity
print("Total produtos vendidos:")
total_vendas = df_vendas.agg(sum('sales_quantity').alias('total_vendas'))
total_vendas.show()

# 3 - produto mais vendido
print("Produto mais vendido:")
agrupado_por_item = df_vendas.groupBy('item').agg(sum('sales_quantity').alias('quantidade_total'))
produto_mais_vendido = agrupado_por_item.orderBy('quantidade_total', ascending=False).limit(1)
produto_mais_vendido.show()

# 4 - 5 produtos mais sales_quantity
print("5 Produtos mais vendidos:")
resultado = df_vendas.groupBy('item').agg(sum('sales_quantity').alias('quantidade_total')) \
            .orderBy('quantidade_total', ascending=False) \
            .limit(5)
resultado.show()

# 5 - 5 produtos mais sales_amount
print("5 Produtos com maior valor de venda:")
resultado = df_vendas.groupBy('item').agg(sum('sales_amount').alias('valor_total_vendas')) \
            .orderBy('valor_total_vendas', ascending=False) \
            .limit(5) \
            .withColumn('valor_total_vendas', col('valor_total_vendas').cast(DecimalType(18, 2)))

resultado.show()

# 6 - sales_quantity por mes e  sales_amount por mes
print("Valor de venda e quantidade de vendas por mês:")

from pyspark.sql.functions import col, sum, to_date, date_format

df_vendas = df_vendas.withColumn("month", date_format(to_date(col("invoice_date"), "dd/MM/yyyy"), "MM"))
vendas_por_mes = df_vendas.groupBy("month")\
                    .agg(sum("sales_quantity").alias("quantidade_total"), sum("sales_amount").alias("valor_total_vendas"))\
                    .orderBy('month')\
                    .withColumn('valor_total_vendas', col('valor_total_vendas').cast(DecimalType(18, 2)))

vendas_por_mes.show()

# 7 - valor de venda por country - mostrar porcentagem
print("Valor de venda em moeda e em porcentagem por país:")

from pyspark.sql.functions import col, sum

df_vendas_pais = df_vendas.join(df_endereco, df_vendas.customer_key == df_endereco.address_number)\
                         .groupBy("country")\
                         .agg(sum("sales_amount").alias("valor_total_vendas"))\
                         .withColumn('valor_total_vendas', col('valor_total_vendas').cast(DecimalType(18, 2)))

total_vendas = df_vendas.agg(sum("sales_amount")).collect()[0][0]
df_vendas_pais = df_vendas_pais.withColumn("porcentagem", col("valor_total_vendas") / total_vendas * 100)\
                    .orderBy('porcentagem', ascending=False)\
                    .withColumn('porcentagem', col('porcentagem').cast(DecimalType(18, 2)))

df_vendas_pais.show()

# 8 - sales_amount por ano
print("Valor de venda por ano:")

df_vendas = df_vendas.withColumn("year", date_format(to_date(col("invoice_date"), "dd/MM/yyyy"), "yyyy"))
vendas_por_ano = df_vendas.groupBy("year")\
                    .agg(sum("sales_amount").alias("valor_total_vendas"))\
                    .orderBy('year')\
                    .withColumn('valor_total_vendas', col('valor_total_vendas').cast(DecimalType(18, 2)))

vendas_por_ano.show()

# # 9 - tabela com customer, sales_amount, country. Ordenar melhores clientes pelo valor de sales_amount 10 melhores
# # e mostrar o total de sales_amount dos 10 melhores
# from pyspark.sql.functions import sum, col


from pyspark.sql.functions import sum

df_joined = df_clientes.join(df_vendas, "customer_key")
df_joined = df_joined.join(df_endereco, df_joined.customer_key == df_endereco.address_number,"left")
df_resultado = df_joined.groupBy("customer", 'country').agg(sum("sales_amount").alias("valor_total_vendas"))
df_top10 = df_resultado.orderBy("valor_total_vendas", ascending=False).withColumn('valor_total_vendas', col('valor_total_vendas').cast(DecimalType(18, 2))).limit(10)
df_top10.show()

