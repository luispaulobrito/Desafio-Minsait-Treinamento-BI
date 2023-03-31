CREATE TABLE IF NOT EXISTS desafio_curso.divisao ( 
    id_categoria string,
    ds_categoria string,
    perc_parceiro string
    )
COMMENT 'Tabela de divisao'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
location '/datalake/raw/divisao/'
TBLPROPERTIES ("skip.header.line.count"="1");