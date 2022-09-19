#!/usr/bin/env python
# coding: utf-8

# In[1]:


import time
import pandas as pd
from sqlalchemy import create_engine
import psycopg2 


# In[2]:


#Verifica se a tabela existe para appendar ou criar automaticamente
def table_exists(conn,table_str):
    exists = False
    try:           
        result = conn.execute("select exists(select * from information_schema.tables where table_name=%s)", (table_str,))   
        exists = result.fetchone()[0]
        #print(exists)
        result.close()
    except psycopg2.Error as e:
        print(e)
    return exists 

#Remove espaço entre as strings
def trim_all_columns(df):
    trim_strings = lambda x: x.strip() if isinstance(x, str) else x
    return df.applymap(trim_strings)


# In[3]:


#String de conexão para esse teste usei linux CentOS 7, o usuario, senha e db são dos mesmos (clickSign)
#Precisei instalar o Postgresl PostgreSQL 9.2.24 on x86_64-redhat-linux-gnu, 
#compiled by gcc (GCC) 4.8.5 20150623 (Red Hat 4.8.5-44), 64-bit
conn_string = 'postgresql://clicksign:clicksign@10.0.10.178/clicksign'
conn  = create_engine(conn_string, echo=False) 
conn = conn.execution_options(autocommit=True)


# In[5]:



local_csv = '/srv/samba/arquivos/TEST/Adult.test'
#Como ainda não conheço o negócio a fundo da ClickSign trouxe todas as colunas afinal, com esses dados
#É possível vários insigths.
#Inclui o header conforme documentação
header_list = ["age",                "workclass",                "fnlwgt",                "education",                "education-num",                "marital-status",                "occupation",                "relationship",                "race",                "sex",                "capital-gain ",                "capital-loss",                "hours-per-week",                "native-country",                "class"
              ] 
#grava no dataframe incluindo o header e removendo a primeira linha e garantindo somente inserção de 1630 linhas.
#Troca o caractere inválido por nulo.
df = pd.read_csv(local_csv,header=None, names=header_list,  nrows=1630,skiprows=1, na_values=[" ?", None]) #usecols=[0,1,2,3,4,5,6,7,8,9,10,],
#Chama função para remover espaços vazios entre as strings
df = trim_all_columns(df)
df


# In[7]:


#observem que quem vai determinar a não duplicidade da informação será o código de origem, como esses dados não tem 
#chave fica difícil fazer o incremental/diferencial de dados.
#também poderiamos add um hash md5 para validar o que é dado novo...

start_time = time.time()
if(table_exists(conn,'adult')==False): 
    df.to_sql('adult', con=conn, if_exists='replace', index=False, method="multi")
else:     
    df.to_sql('adult', con=conn, if_exists='append', index=False, method="multi")
print("to_sql duration: {} seconds".format(time.time() - start_time))


# In[ ]:




