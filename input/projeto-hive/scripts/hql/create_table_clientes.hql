CREATE TABLE IF NOT EXISTS desafio.clientes ( 
    id_categoria string,
    ds_categoria string,
    perc_parceiro string
    )
COMMENT 'Tabela de clientes'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
location '/datalake/raw/clientes/'
TBLPROPERTIES ("skip.header.line.count"="1");