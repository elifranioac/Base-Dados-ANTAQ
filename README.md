# Base-Dados-ANTAQ_ETL

Dados disponíveis 2018-2020 em: https://drive.google.com/drive/folders/1Ms-jzqsWWZSaiO06B2ObvSs_mCu2XEYx?usp=sharing
Neste repoisitório está as fontes em formato .txt extraídas da base http://web.antaq.gov.br/Anuario/ e disponível na web via aplicação de BI QLikView. 
E as tabelas Fatos: "carga_fato.csv" e "atracacao_fato.csv"  geradas e testada pelos procedimentos realizados via estrura de programação em python,
utilizando as bibliotecas PySpark para gerar aplicações e Jobs via a API do Spark com o uso do Jupyter. Presentes nos arquivos Questao_02 (7).ipynb como a estrutura nativa do Jupyter Notebook do python.
Pode-se observar e analisar o código Questao_02 (2).py sem precisar clonar (com o comando 'git clone'), basta colocar 1s depois do github, fica assim https://github1s.com/elifranioac/Base-Dados-ANTAQ_ETL/edit/main/README.md  

Deste modo pode iniciar a ver os códigos no modelo .py, como se fosse o editor de texto VsCode.

segue o link das fontes:

## Melhorar a Automatização da extração (está diponível no final da Questao_02 (2).py)
#antaq2020 = zipfile.ZipFile('http://web.antaq.gov.br/Sistemas/ArquivosAnuario/Arquivos/2020.zip');
#w = zipfile.ZipFile('http://web.antaq.gov.br/Sistemas/ArquivosAnuario/Arquivos/2020.zip') as antaq2020;
    ##print(*antaq2020.namelist(), sep="\n")
   
#with zipfile.ZipFile('http://web.antaq.gov.br/Sistemas/ArquivosAnuario/Arquivos/2019.zip') as antaq2019:
#with zipfile.ZipFile('http://web.antaq.gov.br/Sistemas/ArquivosAnuario/Arquivos/2018.zip') as antaq2018:
#with zipfile.ZipFile('http://web.antaq.gov.br/Sistemas/ArquivosAnuario/Arquivos/MetadadosFrota.zip') as meta_frota:
#with zipfile.ZipFile('http://web.antaq.gov.br/Sistemas/ArquivosAnuario/Arquivos/MetadadosAfretamento.zip') as meta_afreta:
#with zipfile.ZipFile('http://web.antaq.gov.br/Sistemas/ArquivosAnuario/Arquivos/MetadadosMovimentacao.zip') as meta_movi:

Boas fontes bibliográfcas para aprimorar: BENGFORT, Benjamin; KIM, Jenny. Analítica de dados com Hadoop: Uma introdução para cientistas de dados. Novatec Editora, 2019.
                          MISHRA, Raju Kumar; RAMAN, Sundar Rajan. PySpark SQL Recipes. Apress, 2019.
                          JURNEY, Russell. Agile data science 2.0: Building full-stack data analytics applications with spark. " O'Reilly Media, Inc.", 2017.


Livre para contribuições e melhorias.

Bons estudos!


