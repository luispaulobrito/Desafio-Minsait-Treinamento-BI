DESAFIO BIG DATA/BI

📌 ESCOPO DO DESAFIO
Neste desafio foram feitas as ingestões dos dados que estão na pasta [/raw](https://github.com/luispaulobrito/Desafio-Minsait-Treinamento-BI/tree/main/input/projeto-hive/raw) onde há alguns arquivos .csv de um banco relacional de vendas.

 - VENDAS.CSV
 - CLIENTES.CSV
 - ENDERECO.CSV
 - REGIAO.CSV
 - DIVISAO.CSV

Foram providos dados na pasta desafio_curso/gold em .csv para ser consumido por um relatório em PowerBI que foi construído dentro da pasta 'app'.

📑 ETAPAS
Etapa 1 - Enviar os arquivos para o HDFS
    - nesta etapa foi criado um shell script para fazer o trabalho repetitivo.
    
[Script criado para automatizar envio dos arquivos para o HDFS](https://github.com/luispaulobrito/Desafio-Minsait-Treinamento-BI/blob/main/desafio_curso/scripts/pre_process/create_env_all.sh)
    

Etapa 2 - Criar o banco DESAFIO_CURSO e dentro tabelas no Hive usando o HQL e executando um script shell dentro do hive server na pasta scripts/pre_process.

    - DESAFIO_CURSO (nome do banco)
        - TBL_VENDAS
        - TBL_CLIENTES
        - TBL_ENDERECO
        - TBL_REGIAO
        - TBL_DIVISAO
        
[Scripts HQL que são executados automaticamente pelo ShellScript](https://github.com/luispaulobrito/Desafio-Minsait-Treinamento-BI/tree/main/desafio_curso/scripts/hql)

<img src="https://github.com/luispaulobrito/Desafio-Minsait-Treinamento-BI/blob/main/assets/Screenshot_1.png" width="1000px"> 
</div>

Etapa 3 - Processar os dados no Spark Efetuando suas devidas transformações criando os arquivos com a modelagem de BI.
OBS. o desenvolvimento foi feito no jupyter e no final o codigo colocado no arquivo [desafio_curso/scripts/process/process.py](https://github.com/luispaulobrito/Desafio-Minsait-Treinamento-BI/blob/main/desafio_curso/scripts/process/process.py)

[Arquivo em PySpark que executa as etapas 3, 4, 5 e apresenta os testes dos valores](https://github.com/luispaulobrito/Desafio-Minsait-Treinamento-BI/blob/main/desafio_curso/scripts/process/Process.ipynb)

Etapa 4 - Gravar as informações em tabelas dimensionais em formato cvs delimitado por ';'.

        - FT_VENDAS
        - DIM_CLIENTES
        - DIM_TEMPO
        - DIM_LOCALIDADE
        
Etapa 5 - Exportar os dados para a pasta desafio_curso/gold

[Tabelas processadas](https://github.com/luispaulobrito/Desafio-Minsait-Treinamento-BI/tree/main/desafio_curso/gold)        
<img src="https://github.com/luispaulobrito/Desafio-Minsait-Treinamento-BI/blob/main/assets/Screenshot_3.png" width="1000px"> 
</div>

Etapa 6 - Criar e editar o PowerBI com os dados que foram trabalhados.
[Arquivo Power BI](https://github.com/luispaulobrito/Desafio-Minsait-Treinamento-BI/tree/main/desafio_curso/app) 

No PowerBI criar gráficos de vendas.
## Resultado no Power BI
![Resultado](https://github.com/luispaulobrito/Desafio-Minsait-Treinamento-BI/blob/main/assets/powerbi.gif)

Etapa 7 - Criar uma documentação com os testes e etapas do projeto.
## Testes dos valores no Jupyter
[Arquivo com teste dos valores antes da etapa de transformação dos dados](https://github.com/luispaulobrito/Desafio-Minsait-Treinamento-BI/blob/main/desafio_curso/scripts/process/Testes%20Relacional.ipynb) 

![Testes](https://github.com/luispaulobrito/Desafio-Minsait-Treinamento-BI/blob/main/assets/jupyter.gif)

REGRAS
Campos strings vazios deverão ser preenchidos com 'Não informado'.
Campos decimais ou inteiros nulos ou vazios, deversão ser preenchidos por 0.
Atentem-se a modelagem de dados da tabela FATO e Dimensão.
Na tabela FATO, pelo menos a métrica <b>valor de venda</b> é um requisito obrigatório.
Nas dimensões deverá conter valores únicos, não deverá conter valores repetidos.
