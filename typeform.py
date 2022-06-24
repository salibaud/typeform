#!/usr/bin/env python
# coding: utf-8

# In[78]:


#!/usr/bin/env python
# coding: utf-8

# In[479]:


import json
import pandas as pd
from typeform import Typeform
import boto3
import numpy as np
from datetime import datetime, timedelta
import awswrangler as wr
import requests
import boto3

my_session = boto3.Session(aws_access_key_id="AKIAT3PM4W6RC3TZ7EY",aws_secret_access_key="rbebflz+HWx/HVrrclFixdwxM5ETLNzqhf94RoAR",region_name="eu-west-1")

d = datetime.today() - timedelta(days=1)


# In[79]:




def extraccion_type_from(formulario):
    responses = Typeform('8WvDyJVzegiGBZPwzYDL56rsXJgDczZMUA4u1Agd37aQ').responses
    resultados = pd.DataFrame([])
    base_de_datos_f = pd.DataFrame([])
    id_respose = ''
    for i in range(10):
        df = pd.DataFrame([])
        result: dict = responses.list(formulario,pageSize=1000,before=id_respose)
        base_de_datos=pd.DataFrame.from_dict(result['items'])
        base_de_datos_f=base_de_datos_f.append(base_de_datos)
        if base_de_datos.shape[0] >0:
            metadato=pd.DataFrame.from_dict(list(base_de_datos['metadata']))
            answers=pd.DataFrame.from_dict(list(base_de_datos['answers']))
            hiden=pd.DataFrame.from_dict(list(base_de_datos["hidden"]))

            for i in range(answers.shape[1]):
                a=(pd.DataFrame.from_dict(dict(answers[i])).T).drop(["field","type"],axis=1).rename(columns={"number":i+1})
                df=pd.concat([a,df],axis=1)
            b=pd.DataFrame.from_dict(dict(hiden))
            df2=pd.concat([base_de_datos[["submitted_at"]],b,df],axis=1)
            resultados=resultados.append(df2)
            print(len(resultados))
            id_respose = base_de_datos["response_id"].iloc[-1]
            print(id_respose)
        else:
            pass
        
    return resultados


# In[80]:



lead_no_alcanzado = extraccion_type_from('FHZ9nb6M')


# In[81]:


resultados = pd.DataFrame([])
for i in range(len(lead_no_alcanzado)):
    a=pd.DataFrame((lead_no_alcanzado.iloc[i,6])["labels"]).T.rename(columns={0:"Preg1_A",1:"Preg1_B",2:"Preg1_C"})
    b=pd.DataFrame((lead_no_alcanzado.iloc[i,7])["labels"]).T.rename(columns={0:"Preg2_A",1:"Preg2_B",2:"Preg2_C"})
    a=pd.concat([a,b],axis=1)
    a["date"] = lead_no_alcanzado["submitted_at"].iloc[i]
    a["correo"] = lead_no_alcanzado["correo"].iloc[i]
    a["id_cliente"] = lead_no_alcanzado["id_cliente"].iloc[i]
    a["nota"] = lead_no_alcanzado[5].iloc[i]
    a["considera_mb"] = lead_no_alcanzado["boolean"].iloc[i]
    a["porque"] = lead_no_alcanzado["text"].iloc[i]
    resultados = resultados.append(a)


# In[82]:


df=resultados[["date","correo","id_cliente","nota","Preg1_A","Preg1_B","Preg1_C","Preg2_A","Preg2_B","Preg2_C","considera_mb","porque"]]


# In[83]:



wr.s3.to_parquet(df=df,
                 path="s3://kaufmann-typeform/lead_no_alcanzado/",
                 dataset=True,
                 mode = 'overwrite',
                 boto3_session=my_session)


# In[84]:


fuga_leads = extraccion_type_from('iIvW5j9T') 
fuga_leads = fuga_leads.rename(columns={2:"nota","text":"porque","submitted_at":"date"})
df2=fuga_leads[["date","correo","id_cliente","nota","porque"]]


# In[85]:


wr.s3.to_parquet(df=df2,
                 path="s3://kaufmann-typeform/fuga_leads/",
                 dataset=True,
                 mode = 'overwrite',
                 boto3_session=my_session)


# In[86]:


fuga_clientes = extraccion_type_from('nXAup5z0') 
fuga_clientes

fuga_clientes = fuga_clientes.rename(columns={3:"nota","text":"porque","submitted_at":"date","boolean":"considero"})
df3=fuga_clientes[["date","correo","id_cliente","nota","considero","porque"]]
df3


# In[87]:


wr.s3.to_parquet(df=df3,
                 path="s3://kaufmann-typeform/fuga_clientes/",
                 dataset=True,
                 mode = 'overwrite',
                 boto3_session=my_session)


# In[ ]:




