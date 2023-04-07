CREATE TABLE IF NOT EXISTS desafio_curso.divisao ( 
    Division string,
    Division_Name string
)
COMMENT 'Tabela de divisao'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
LOCATION '/datalake/raw/DIVISAO/'
TBLPROPERTIES ("skip.header.line.count"="1");