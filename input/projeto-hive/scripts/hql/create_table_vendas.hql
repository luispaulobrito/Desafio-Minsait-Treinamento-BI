CREATE TABLE IF NOT EXISTS desafio.vendas ( 
    id_categoria string,
    ds_categoria string,
    perc_parceiro string
    )
COMMENT 'Tabela de vendas'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
location '/datalake/raw/vendas/'
TBLPROPERTIES ("skip.header.line.count"="1");