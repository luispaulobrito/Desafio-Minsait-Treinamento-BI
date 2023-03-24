CREATE TABLE IF NOT EXISTS desafio.regiao ( 
    id_categoria string,
    ds_categoria string,
    perc_parceiro string
    )
COMMENT 'Tabela de regiao'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
location '/datalake/raw/regiao/'
TBLPROPERTIES ("skip.header.line.count"="1");