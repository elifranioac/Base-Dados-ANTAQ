#!/usr/bin/env python
# coding: utf-8

# <h1 p style='text-align: center;'>Base Dados ANTAQ <h1>
# <h2 p style='text-align: center;'> Gerção das Tabelas fatos: atracacao_fato e carga_fato <h2>
# <h3 p style='text-align: center;'>Extração e transformação dos dados em Dataframes<h3>
# <h3 p style='text-align: center;'>Questão 02<h3>
# <h4 p style='text-align: center;'> Autor: Elifranio Alves Cruz <h4>
# <h2 p style='text-align: left;'> Extração <h2>

# In[1]:


#definições das bibliotecas
import pyspark
from pyspark import SparkContext


# In[2]:


import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import zipfile

from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName("Collinear Points").setMaster("local[4]") # inicializar o Spark unsando o contexto de 4 núcleo de trabalhos (jobs) 
#sc = SparkContext(conf=conf)    
from pyspark.rdd import RDD


# In[3]:


sc


# In[4]:


dir(pyspark)


# In[5]:


from pyspark.sql import SparkSession


# In[6]:


# criar uma sessão usanda para DF ( antaq) exemplo
spark = SparkSession.builder.appName("ANTAQ").getOrCreate()


# In[7]:


#Pode-se alternar 2018, 2019 e 2020 de forma manual de acordo com os arquivos nos diretórios, neste caso para 
# a tabela de acordos bilaterais.Ex.  ano = 2018; ou ano = 2019 ou ano = 2020
ano = "2020"
   # verifica se o arquivo existe no dietório
if os.path.exists("D:/Aulas 2021.1/SFIEC_Selecao/Dados/"+ano+"/"+ano+"AcordosBilaterais.txt"):
        df = spark.read.csv("D:/Aulas 2021.1/SFIEC_Selecao/Dados/"+ano+"/"+ano+"AcordosBilaterais.txt", header=True, sep=';')
            


# In[8]:


type(df)


# In[10]:


df.show(5)


# In[48]:


#Pegar a primeira linha
df.first()


# In[11]:


#pegar 5 cabeçalhos
df.head(5)


# In[12]:


# checar o nome das colunas
df.columns


# In[13]:


#checar os tipos de dados das colunas
df.dtypes


# In[14]:


# pegarv o esquema do dataframe
df.printSchema()


# In[15]:


# verificar o número de linhas
df.count()


# In[16]:


# checar o número de colunas
len(df.columns)


# In[17]:


# checar a forma (linhas, colunas)
print(df.count(), len(df.columns) )


# In[18]:


#### Tabela Acordo Bilateral######
#Pode-se alternar 2018, 2019 e 2020 de forma manual de acordo com os arquivos nos diretórios, neste caso para 
# a tabela de acordos bilaterais.Ex.  ano = 2018; ou ano = 2019 ou ano = 2020
#Extraindo de forma manual no modelo de I/O do pyspark os dados da tabela de Acordo Bilateral

# criar uma sessão usanda para extração da tabela Acordo Bilateral
spark = SparkSession.builder.appName("ANTAQ.tb_ab").getOrCreate()


ano2020 = "2020"
   # verifica se o arquivo existe no dietório
if os.path.exists("D:/Aulas 2021.1/SFIEC_Selecao/Dados/"+ano2020+"/"+ano2020+"AcordosBilaterais.txt"):
        tb_ab2020 = spark.read.csv("D:/Aulas 2021.1/SFIEC_Selecao/Dados/"+ano2020+"/"+ano2020+"AcordosBilaterais.txt", header=True, sep=';')
            
ano2019 = "2019"
   # verifica se o arquivo existe no dietório
if os.path.exists("D:/Aulas 2021.1/SFIEC_Selecao/Dados/"+ano2019+"/"+ano2019+"AcordosBilaterais.txt"):
        tb_ab2019 = spark.read.csv("D:/Aulas 2021.1/SFIEC_Selecao/Dados/"+ano2019+"/"+ano2019+"AcordosBilaterais.txt", header=True, sep=';')
        
ano2018 = "2018"
   # verifica se o arquivo existe no dietório
if os.path.exists("D:/Aulas 2021.1/SFIEC_Selecao/Dados/"+ano2018+"/"+ano2018+"AcordosBilaterais.txt"):
        tb_ab2018 = spark.read.csv("D:/Aulas 2021.1/SFIEC_Selecao/Dados/"+ano2018+"/"+ano2018+"AcordosBilaterais.txt", header=True, sep=';')   
        
tb_ab = tb_ab2020.union(tb_ab2019.union(tb_ab2018))   


# In[19]:


tb_ab2020.show(5)


# In[20]:


tb_ab.show(5)


# In[21]:


tb_ab2020.count() 


# In[22]:


tb_ab2019.count()


# In[23]:


tb_ab2018.count()


# In[24]:


tb_ab.count()


# In[25]:


#### Tabela Atracação######
#Pode-se alternar 2018, 2019 e 2020 de forma manual de acordo com os arquivos nos diretórios, neste caso para 
# a tabela de atracação.Ex.  ano = 2018; ou ano = 2019 ou ano = 2020
#Extraindo de forma manual no modelo de I/O do pyspark os dados da tabela de Atracação

# criar uma sessão usanda para extração da tabela Atracacao
spark = SparkSession.builder.appName("ANTAQ.tb_atr").getOrCreate()

ano2020 = "2020"
   # verifica se o arquivo existe no dietório
if os.path.exists("D:/Aulas 2021.1/SFIEC_Selecao/Dados/"+ano2020+"/"+ano2020+"Atracacao.txt"):
        tb_atr2020 = spark.read.csv("D:/Aulas 2021.1/SFIEC_Selecao/Dados/"+ano2020+"/"+ano2020+"Atracacao.txt", header=True, sep=';')
            
ano2019 = "2019"
   # verifica se o arquivo existe no dietório
if os.path.exists("D:/Aulas 2021.1/SFIEC_Selecao/Dados/"+ano2019+"/"+ano2019+"Atracacao.txt"):
        tb_atr2019 = spark.read.csv("D:/Aulas 2021.1/SFIEC_Selecao/Dados/"+ano2019+"/"+ano2019+"Atracacao.txt", header=True, sep=';')
        
ano2018 = "2018"
   # verifica se o arquivo existe no dietório
if os.path.exists("D:/Aulas 2021.1/SFIEC_Selecao/Dados/"+ano2018+"/"+ano2018+"Atracacao.txt"):
        tb_atr2018 = spark.read.csv("D:/Aulas 2021.1/SFIEC_Selecao/Dados/"+ano2018+"/"+ano2018+"Atracacao.txt", header=True, sep=';')   
        
tb_atr = tb_atr2020.union(tb_atr2019.union(tb_atr2018))   


# In[26]:


tb_atr2018.first()


# In[27]:


tb_atr2018.head()


# In[28]:


tb_atr2018.show(5)


# In[29]:


tb_atr2020.count()


# In[30]:


tb_atr2019.count()


# In[31]:


tb_atr2018.count()


# In[32]:


tb_atr.count()


# In[33]:


#### Tabela Carga######
#Pode-se alternar 2018, 2019 e 2020 de forma manual de acordo com os arquivos nos diretórios, neste caso para 
# a tabela de Carga.Ex.  ano = 2018; ou ano = 2019 ou ano = 2020
#Extraindo de forma manual no modelo de I/O do pyspark os dados da tabela de Carga

# criar uma sessão usanda para extração da tabela Carga
spark = SparkSession.builder.appName("ANTAQ.tb_carga").getOrCreate()

ano2020 = "2020"
   # verifica se o arquivo existe no dietório
if os.path.exists("D:/Aulas 2021.1/SFIEC_Selecao/Dados/"+ano2020+"/"+ano2020+"Carga.txt"):
        tb_carga2020 = spark.read.csv("D:/Aulas 2021.1/SFIEC_Selecao/Dados/"+ano2020+"/"+ano2020+"Carga.txt", header=True, sep=';')
            
ano2019 = "2019"
   # verifica se o arquivo existe no dietório
if os.path.exists("D:/Aulas 2021.1/SFIEC_Selecao/Dados/"+ano2019+"/"+ano2019+"Carga.txt"):
        tb_carga2019 = spark.read.csv("D:/Aulas 2021.1/SFIEC_Selecao/Dados/"+ano2019+"/"+ano2019+"Carga.txt", header=True, sep=';')
        
ano2018 = "2018"
   # verifica se o arquivo existe no dietório
if os.path.exists("D:/Aulas 2021.1/SFIEC_Selecao/Dados/"+ano2018+"/"+ano2018+"Carga.txt"):
        tb_carga2018 = spark.read.csv("D:/Aulas 2021.1/SFIEC_Selecao/Dados/"+ano2018+"/"+ano2018+"Carga.txt", header=True, sep=';')   
        
tb_carga = tb_carga2020.union(tb_carga2019.union(tb_carga2018))   


# In[34]:


tb_carga2020.first()


# In[35]:


tb_carga2020.select('IDCarga','IDAtracacao', 'Origem','Destino','Tipo Navegação').show(5) #Fica fora do escopo devido a quantidade colunas ser maior que a capacidade de resolução


# In[36]:


tb_carga2020.count()


# In[37]:


tb_carga2019.count()


# In[38]:


tb_carga2018.count()


# In[39]:


tb_carga.count()


# In[40]:


#### Tabela Carga_Conteinerizada ######
#Pode-se alternar 2018, 2019 e 2020 de forma manual de acordo com os arquivos nos diretórios, neste caso para 
# a tabela de Carga_Conteinerizada.Ex.  ano = 2018; ou ano = 2019 ou ano = 2020
#Extraindo de forma manual no modelo de I/O do pyspark os dados da tabela de Carga_Conteinerizada

# criar uma sessão usanda para extração da tabela Carga_Conteinerizada
spark = SparkSession.builder.appName("ANTAQ.tb_carga_contz").getOrCreate()

ano2020 = "2020"
   # verifica se o arquivo existe no dietório
if os.path.exists("D:/Aulas 2021.1/SFIEC_Selecao/Dados/"+ano2020+"/"+ano2020+"Carga_Conteinerizada.txt"):
        tb_carga_contz2020 = spark.read.csv("D:/Aulas 2021.1/SFIEC_Selecao/Dados/"+ano2020+"/"+ano2020+"Carga_Conteinerizada.txt", header=True, sep=';')
            
ano2019 = "2019"
   # verifica se o arquivo existe no dietório
if os.path.exists("D:/Aulas 2021.1/SFIEC_Selecao/Dados/"+ano2019+"/"+ano2019+"Carga_Conteinerizada.txt"):
        tb_carga_contz2019 = spark.read.csv("D:/Aulas 2021.1/SFIEC_Selecao/Dados/"+ano2019+"/"+ano2019+"Carga_Conteinerizada.txt", header=True, sep=';')
        
ano2018 = "2018"
   # verifica se o arquivo existe no dietório
if os.path.exists("D:/Aulas 2021.1/SFIEC_Selecao/Dados/"+ano2018+"/"+ano2018+"Carga_Conteinerizada.txt"):
        tb_carga_contz2018 = spark.read.csv("D:/Aulas 2021.1/SFIEC_Selecao/Dados/"+ano2018+"/"+ano2018+"Carga_Conteinerizada.txt", header=True, sep=';')   
        
tb_carga_contz = tb_carga_contz2020.union(tb_carga_contz2019.union(tb_carga_contz2018)) 


# In[41]:


tb_carga_contz2020.show(5)


# In[43]:


tb_carga_contz.show(5)


# In[44]:


tb_carga_contz2020.count()


# In[45]:


tb_carga_contz2019.count()


# In[46]:


tb_carga_contz2018.count()


# In[47]:


tb_carga_contz.count()


# In[48]:


#### Tabela Carga_Regiao ######
#Pode-se alternar 2018, 2019 e 2020 de forma manual de acordo com os arquivos nos diretórios, neste caso para 
# a tabela de Carga_Regiao.Ex.  ano = 2018; ou ano = 2019 ou ano = 2020
#Extraindo de forma manual no modelo de I/O do pyspark os dados da tabela de Carga_Regiao

# criar uma sessão usanda para extração da tabela Carga_Regiao
spark = SparkSession.builder.appName("ANTAQ.tb_carga_reg").getOrCreate()

ano2020 = "2020"
   # verifica se o arquivo existe no dietório
if os.path.exists("D:/Aulas 2021.1/SFIEC_Selecao/Dados/"+ano2020+"/"+ano2020+"Carga_Regiao.txt"):
        tb_carga_reg2020 = spark.read.csv("D:/Aulas 2021.1/SFIEC_Selecao/Dados/"+ano2020+"/"+ano2020+"Carga_Regiao.txt", header=True, sep=';')
            
ano2019 = "2019"
   # verifica se o arquivo existe no dietório
if os.path.exists("D:/Aulas 2021.1/SFIEC_Selecao/Dados/"+ano2019+"/"+ano2019+"Carga_Regiao.txt"):
        tb_carga_reg2019 = spark.read.csv("D:/Aulas 2021.1/SFIEC_Selecao/Dados/"+ano2019+"/"+ano2019+"Carga_Regiao.txt", header=True, sep=';')
        
ano2018 = "2018"
   # verifica se o arquivo existe no dietório
if os.path.exists("D:/Aulas 2021.1/SFIEC_Selecao/Dados/"+ano2018+"/"+ano2018+"Carga_Regiao.txt"):
        tb_carga_reg2018 = spark.read.csv("D:/Aulas 2021.1/SFIEC_Selecao/Dados/"+ano2018+"/"+ano2018+"Carga_Regiao.txt", header=True, sep=';')   
        
tb_carga_reg = tb_carga_reg2020.union(tb_carga_reg2019.union(tb_carga_reg2018)) 


# In[49]:


tb_carga_reg2020.show(5)


# In[50]:


tb_carga_reg.show(5)


# In[51]:


tb_carga_reg2020.count()


# In[52]:


tb_carga_reg2019.count()


# In[53]:


tb_carga_reg2018.count()


# In[54]:


tb_carga_reg.count()


# In[55]:


#### Tabela TemposAtracacao ######
#Pode-se alternar 2018, 2019 e 2020 de forma manual de acordo com os arquivos nos diretórios, neste caso para 
# a tabela de TemposAtracacao.Ex.  ano = 2018; ou ano = 2019 ou ano = 2020
#Extraindo de forma manual no modelo de I/O do pyspark os dados da tabela de TemposAtracacao

# criar uma sessão usanda para extração da tabela TemposAtracacao
spark = SparkSession.builder.appName("ANTAQ.tb_temp_atr").getOrCreate()

ano2020 = "2020"
   # verifica se o arquivo existe no dietório
if os.path.exists("D:/Aulas 2021.1/SFIEC_Selecao/Dados/"+ano2020+"/"+ano2020+"TemposAtracacao.txt"):
        tb_temp_atr2020 = spark.read.csv("D:/Aulas 2021.1/SFIEC_Selecao/Dados/"+ano2020+"/"+ano2020+"TemposAtracacao.txt", header=True, sep=';')
            
ano2019 = "2019"
   # verifica se o arquivo existe no dietório
if os.path.exists("D:/Aulas 2021.1/SFIEC_Selecao/Dados/"+ano2019+"/"+ano2019+"TemposAtracacao.txt"):
        tb_temp_atr2019 = spark.read.csv("D:/Aulas 2021.1/SFIEC_Selecao/Dados/"+ano2019+"/"+ano2019+"TemposAtracacao.txt", header=True, sep=';')
        
ano2018 = "2018"
   # verifica se o arquivo existe no dietório
if os.path.exists("D:/Aulas 2021.1/SFIEC_Selecao/Dados/"+ano2018+"/"+ano2018+"TemposAtracacao.txt"):
        tb_temp_atr2018 = spark.read.csv("D:/Aulas 2021.1/SFIEC_Selecao/Dados/"+ano2018+"/"+ano2018+"TemposAtracacao.txt", header=True, sep=';')   
        
tb_temp_atr = tb_temp_atr2020.union(tb_temp_atr2019.union(tb_temp_atr2018))


# In[56]:


tb_temp_atr2020.show(5)


# In[57]:


tb_temp_atr.show(5)


# In[58]:


tb_temp_atr2020.count()


# In[59]:


tb_temp_atr2019.count()


# In[60]:


tb_temp_atr2018.count()


# In[61]:


tb_temp_atr.count()


# <h2>Transformação <h2>

# In[62]:


# Dicionário das tabelas Extraidas
#tb_ab  ; tb_atr ; tb_carga ; tb_carga_contz; tb_carga_reg; tb_temp_atr
###############
# tb_ab - Tabela Acordos Bilaterais
# tb_atr - Tabela Atracacao
# tb_carga - Tabela Carga
# tb_carga_contz - Tabela Carga_Conteinerizada
# tb_carga_reg - Tabela Carga_Regiao
# tb_temp_atr - Tabela TemposAtracacao
###############

# Dicionário dos atributos a serem extraidos e transformados

#At_atracacao_fato = ["IDAtracacao" , " Tipo de Navegação da Atracação" , " CDTUP" , " Nacionalidade do Armador" , 
#                     " IDBerco" , " FlagMCOperacaoAtracacao" , " Berço Terminal" , "Porto Atracação" , " Município" , 
#                     " Apelido Instalação Portuária" , " UF" , " Complexo Portuário" , " SGUF" , 
#                     " Tipo da Autoridade Portuária" , "Região Geográfica" , " Data Atracação" , " Nº da Capitania" , 
#                     " Data Chegada" , " Nº do IMO" , " Data Desatracação" , " TEsperaAtracacao" , "Data Início Operação",
#                     " TEsperaInicioOp" , " Data Término Operação" , " TOperacao" , " Ano da data de início da operação" ,
#                     " TEsperaDesatracacao" , "Mês da data de início da operação" , " TAtracado" , " Tipo de Operação" , 
#                     " TEstadia"]
##############################
# carga_fato = [IDCarga" , " FlagTransporteViaInterioir" , " IDAtracacao" , " Percurso Transporte em vias Interiores" , 
#                " Origem Percurso Transporte Interiores" , "Destino" , " STNaturezaCarga" , 
#                " CDMercadoria (Para carga conteinerizadainformar código das mercadorias dentrodo contêiner.)" , " STSH2" , 
#                " Tipo Operação da Carga" , " STSH4" , " Carga Geral Acondicionamento" , " Natureza da Carga" ,
#                " ConteinerEstado" , " Sentido" , " Tipo Navegação" , " TEU" , "FlagAutorizacao" , " QTCarga" , 
#                " FlagCabotagem" , " VLPesoCargaBruta" , " FlagCabotagemMovimentacao" , 
#                " Ano da data de início da operação da atracação" , " FlagConteinerTamanho" , 
#                " Mês da data de início da operação da atracação" , " FlagLongoCurso" , " Porto Atracação" , 
#                " FlagMCOperacaoCarga" , " SGUF" , " FlagOffshore" , 
#                "  Peso líquido da carga (Carga não conteinerizada = Peso bruto e Carga conteinerizada = Peso sem contêiner)]



# In[63]:


tb_ab.head()


# In[64]:


tb_ab.columns


# In[65]:


tb_atr.head()


# In[66]:


tb_atr.columns


# In[67]:


tb_carga.head()


# In[68]:


tb_carga.columns


# In[69]:


tb_carga_contz.head()


# In[70]:


tb_carga_contz.columns


# In[71]:


tb_carga_reg.head()


# In[72]:


tb_carga_reg.columns


# In[73]:


tb_temp_atr.head()


# In[74]:


tb_temp_atr.columns


# In[75]:


ano = ["2018","2019","2020"]
for i in ano:
    print(i)


# In[76]:


tb_carga_reg.head()


# In[77]:


tb_carga.columns


# In[78]:


tb_atr.columns


# <h1 p style='text-align: center;'> Tabela atracacao_fato <h1>

# In[79]:


######## Tabela atracacao_fato#######
#Criação da tabeça atracacao_fato
# Campo comun entre as duas tabelas (tb_atr.IDAtracacao == tb_temp_atr.IDAtracacao)
# Inner - Combina os valores comuns dos mesmos campos nas duas tabelas (Intersecção)

# criar uma sessão usanda para extração da tabela atracacao_fato
spark = SparkSession.builder.appName("ANTAQ.atracacao_fato").getOrCreate()

atracacao_fato = tb_atr.join(tb_temp_atr, tb_atr.IDAtracacao == tb_temp_atr.IDAtracacao, "inner" )
  


# In[80]:


tb_atr.count()


# In[81]:


tb_temp_atr.count()


# In[82]:


atracacao_fato.count()


# In[83]:


atracacao_fato.columns


# In[106]:


#Criação da Tabela 'atracacao_fato.csv' no diretório atual da execução do Jupyter / se desejar outro Path basta colocar na string
atracacao_fato.toPandas().to_csv('atracacao_fato.csv')


# In[84]:


tb_carga.columns


# In[85]:


tb_atr.columns


# In[86]:


tb_carga_contz.columns


# In[87]:


#teste filtro
tb_atr_filtro = tb_atr.select('IDAtracacao','Ano','Mes','Porto Atracação', 'SGUF')


# In[88]:


tb_atr_filtro.show()


# In[89]:


#Teste Carga coneinerizada para pegar o valor do conteinerizado
tb_carga_contz_filtro = tb_carga_contz.select('IDCarga','VLPesoCargaConteinerizada')


# In[90]:


tb_carga_contz_filtro.columns


# In[91]:


#Teste Carga para pegar o valor da carga bruta
tb_carga_filtro = tb_carga.select('IDCarga','VLPesoCargaBruta')


# In[92]:


# Junção dos filtros com informações de cargas bruta e conteinrizada
# tb_carga_contz_filtro.IDCarga == tb_carga_filtro.IDCarga, "inner" == ['IDCarga']
tb_carga_liqui1 = tb_carga_filtro.join(tb_carga_contz_filtro, ['IDCarga'] )


# In[93]:


tb_carga_liqui1.columns


# In[94]:


tb_carga_liqui2 = tb_carga_liqui1.withColumn('Peso Liquido',tb_carga_liqui1['VLPesoCargaBruta'] + tb_carga_liqui1['VLPesoCargaBruta'] )


# In[95]:


tb_carga_liqui2.columns


# In[96]:


# Teste para pegar a informação do peso liquido
tb_carga_liqui = tb_carga_liqui2.select('IDCarga','Peso Liquido')


# In[97]:


tb_carga_liqui.columns


# In[98]:


#teste
#Procedimentos de junções finais
# tb_carga.IDAtracacao == tb_atr_filtro.IDAtracacao, "inner" == ['IDAtracacao']
carga_fato_tmp1 = tb_carga.join(tb_atr_filtro, ['IDAtracacao'] )


# In[99]:


carga_fato_tmp1.columns


# In[100]:


# Teste Final para colocar a informação do peso liquido na tabela carga_fato
#carga_fato_tmp1.IDCarga == tb_carga_liqui.IDCarga, "inner" == ['IDCarga']
carga_fato = carga_fato_tmp1.join(tb_carga_liqui, ['IDCarga'] )


# In[104]:


carga_fato.columns


# In[107]:


#Criação da Tabela 'carga_fato.csv' no diretório atual da execução do Jupyter / se desejar outro Path basta colocar na string
atracacao_fato.toPandas().to_csv('carga_fato.csv')


# In[102]:


######## Tabela carga_fato####### Resumo das Operações
#Criação da tabeça carga_fato
# Campo comun entre as duas tabelas (tb_atr.IDAtracacao == tb_temp_atr.IDAtracacao)
# Inner - Combina os valores comuns dos mesmos campos nas duas tabelas (Intersecção)

# criar uma sessão usanda para extração da tabela carga_fato
spark = SparkSession.builder.appName("ANTAQ.carga_fato").getOrCreate()

#carga_fato = tb_atr.join(tb_temp_atr, tb_atr.IDAtracacao == tb_temp_atr.IDAtracacao, "inner" )
#Filtro da tabela de atracação e crga conteinerizada
tb_atr_filtro = tb_atr.select('IDAtracacao','Ano','Mes','Porto Atracação', 'SGUF')
tb_carga_contz_filtro = tb_carga_contz.select('IDCarga','VLPesoCargaConteinerizada')
tb_carga_filtro = tb_carga.select('IDCarga','VLPesoCargaBruta')

#Procedimento para pegar os valores de modo que : Carga não conteinerizada = Peso bruto e Carga 
#conteinerizada = Peso sem contêiner - 
tb_carga_liqui1 = tb_carga_filtro.join(tb_carga_contz_filtro, tb_carga_contz_filtro.IDCarga == tb_carga_filtro.IDCarga, "inner" )
tb_carga_liqui2 = tb_carga_liqui1.withColumn('Peso Liquido',tb_carga_liqui1['VLPesoCargaBruta'] + tb_carga_liqui1['VLPesoCargaBruta']  )
tb_carga_liqui = tb_carga_liqui2.select('IDCarga','Peso Liquido')

#Procedimentos de junções finais
# tb_carga.IDAtracacao == tb_atr_filtro.IDAtracacao, "inner" == ['IDAtracacao']
carga_fato_tmp1 = tb_carga.join(tb_atr_filtro, ['IDAtracacao'] )

#carga_fato_tmp1.IDCarga == tb_carga_liqui.IDCarga, "inner" == ['IDCarga']
carga_fato = carga_fato_tmp1.join(tb_carga_liqui, ['IDCarga'] )


# In[134]:


tb_atr.select('Ano','Mes','Porto Atracação', 'SGUF')


# In[104]:


# Automatizar a extração
# ler o arquivo .csv/.txt com o cabeçalho e esquema para os anos 2018, 2019 e 2020

import os.path
# Carga da tabela de AcordosBilaterais
def Carga_ANTAQ(tb_ab):
    ano = ["2018","2019","2020"]
    tb_ab = pyspark.sql.dataframe.DataFrame()
    for i in ano:
        # verifica se o arquivo existe no dietório
        if os.path.exists("D:/Aulas 2021.1/SFIEC_Selecao/Dados/"+i+"/"+i+"AcordosBilaterais.txt"):
            tb_ab = tb_ab.union(spark.read.csv("D:/Aulas 2021.1/SFIEC_Selecao/Dados/"+i+"/"+i+"AcordosBilaterais.txt", header=True, sep=';'))
            #rdd = tb_ab.union(rdd)
            #tb_ab = tb_ab.union(rdd)
return tb_ab


# In[45]:



tb_acbi_2018 = sc.(self, 'D:\Aulas 2021.1\SFIEC Seleção\Dados\2018\2018AcordosBilaterais.txt')  


# In[28]:


tb_acbi_2018.count()


# In[25]:


## Automatizar a extração para 2020
#antaq2020 = zipfile.ZipFile('http://web.antaq.gov.br/Sistemas/ArquivosAnuario/Arquivos/2020.zip');
#w = zipfile.ZipFile('http://web.antaq.gov.br/Sistemas/ArquivosAnuario/Arquivos/2020.zip') as antaq2020;
    ##print(*antaq2020.namelist(), sep="\n")
   
#with zipfile.ZipFile('http://web.antaq.gov.br/Sistemas/ArquivosAnuario/Arquivos/2019.zip') as antaq2019:
#with zipfile.ZipFile('http://web.antaq.gov.br/Sistemas/ArquivosAnuario/Arquivos/2018.zip') as antaq2018:
#with zipfile.ZipFile('http://web.antaq.gov.br/Sistemas/ArquivosAnuario/Arquivos/MetadadosFrota.zip') as meta_frota:
#with zipfile.ZipFile('http://web.antaq.gov.br/Sistemas/ArquivosAnuario/Arquivos/MetadadosAfretamento.zip') as meta_afreta:
#with zipfile.ZipFile('http://web.antaq.gov.br/Sistemas/ArquivosAnuario/Arquivos/MetadadosMovimentacao.zip') as meta_movi:


# In[93]:


ano = ["2018","2019","2020"]
for i in ano:
    print(i)


# In[109]:


#Testes
ano = ["2018","2019","2020"]
tb_ab = pyspark.sql.dataframe.DataFrame()
for i in ano:
  # verifica se o arquivo existe no dietório
  ##if os.path.exists("D:/Aulas 2021.1/SFIEC_Selecao/Dados/"+i+"/"+i+"AcordosBilaterais.txt"):
    tb_ab = tb_ab.union(spark.read.csv("D:/Aulas 2021.1/SFIEC_Selecao/Dados/"+i+"/"+i+"AcordosBilaterais.txt", header=True, sep=';'))


# In[ ]:




