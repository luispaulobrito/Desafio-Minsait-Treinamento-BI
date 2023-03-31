CREATE TABLE IF NOT EXISTS desafio_curso.endereco ( 
    id_categoria string,
    ds_categoria string,
    perc_parceiro string
    )
COMMENT 'Tabela de endereco'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
location '/datalake/raw/endereco/'
TBLPROPERTIES ("skip.header.line.count"="1");