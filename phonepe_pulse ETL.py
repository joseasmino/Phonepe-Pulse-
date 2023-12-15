import pandas as pd
import os
import json
import mysql.connector

# Clone libraries
import requests
import subprocess

import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine

# cloning the repository using the url

repo_url = 'https://github.com/PhonePe/pulse.git'

#Specify the local directory path
local_dir = r"D:\New folder (2)\pulse"

# checking the directory already exists
if not os.path.exists(local_dir):
    subprocess.call(["git", "clone", repo_url, local_dir], check=True)


# Extracting aggregation transaction data and making it a df 

path1=r"D:\New folder (2)\pulse\data\aggregated\transaction\country\india\state"
agg_tra_state_list=os.listdir(path1)
agg_tra_state_list

agg_tra = {'State': [], 'Year': [], 'Quarter': [], 'Transaction_Type': [], 'Transaction_Count': [], 'Transaction_Amount': []}

for i in agg_tra_state_list:
    p_i = path1 + '/' + i 
    agg_yr = os.listdir(p_i)

    for j in agg_yr:
        p_j = p_i + '/' + j 
        agg_yr_list = os.listdir(p_j)   

        
        for k in agg_yr_list:
            p_k = p_j + '/' + k
            data = open(p_k, 'r')
            A = json.load(data)

            
            for a in A['data']['transactionData']:
                Name = a['name']
                count = a['paymentInstruments'][0]['count']
                amount = a['paymentInstruments'][0]['amount']
                agg_tra['State'].append(i)
                agg_tra['Year'].append(j)
                agg_tra['Quarter'].append(int(k.strip('.json')))
                agg_tra['Transaction_Type'].append(Name)
                agg_tra['Transaction_Count'].append(count)
                agg_tra['Transaction_Amount'].append(amount)

df_agg_tra = pd.DataFrame(agg_tra)
df_agg_tra['State'] = df_agg_tra["State"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
df_agg_tra["State"] = df_agg_tra["State"].str.replace("-"," ")
df_agg_tra["State"] = df_agg_tra["State"].str.title()
df_agg_tra['State'] = df_agg_tra['State'].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Daman and Diu")
#df_agg_tra


# Extracting aggregation user data and making it a df 

path2=r"D:\New folder (2)\pulse\data\aggregated\user\country\india\state"
agg_user_state_list=os.listdir(path2)
agg_user_state_list

agg_user = {'State': [], 'Year': [], 'Quarter': [], 'Brands': [], 'User_Count': [], 'User_Percentage': []}

for i in agg_user_state_list:
    p_i = path2 + "/" + i 
    agg_yr = os.listdir(p_i)
    #print(agg_yr)

    for j in agg_yr:
        p_j = p_i + '/' + j 
        agg_yr_list = os.listdir(p_j)
        #print(j)

        for k in agg_yr_list:
            p_k = p_j + '/' + k
            data = open(p_k, 'r')
            B = json.load(data)
            #print(B)

            try:
                for b in B['data']['usersByDevice']:
                    brand_name=b['brand']
                    count=b['count']
                    all_percentage=b['percentage']
                    agg_user['State'].append(i)
                    agg_user['Year'].append(j)
                    agg_user['Quarter'].append(int(k.strip('.json')))
                    agg_user['Brands'].append(brand_name)
                    agg_user['User_Count'].append(count)
                    agg_user['User_Percentage'].append(all_percentage*100)
            except:
                pass
            
df_agg_user=pd.DataFrame(agg_user)
df_agg_user['State'] = df_agg_user["State"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
df_agg_user["State"] = df_agg_user["State"].str.replace("-"," ")
df_agg_user["State"] = df_agg_user["State"].str.title()
df_agg_user['State'] = df_agg_user['State'].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Daman and Diu")
#df_agg_user


# Extracting map transaction data and making it a df 

path3=r"D:\New folder (2)\pulse\data\map\transaction\hover\country\india\state"
map_tra_state_list=os.listdir(path3)
map_tra_state_list

map_tra = {'State':[], 'Year':[], 'Quarter':[], 'District':[], 'Transaction_Count':[], 'Transaction_Amount':[]}

for i in map_tra_state_list:
    p_i=path3+"/"+i
    map_year=os.listdir(p_i)
    #print(map_year)

    for j in map_year:
        p_j=p_i+"/"+j
        map_year_list=os.listdir(p_j)
        #print(map_year_list)

        for k in map_year_list:
            p_k=p_j+'/'+k
            data=open(p_k,'r')
            C=json.load(data)
            #print(C)

            for c in C['data']['hoverDataList']:
                name=c["name"]
                count=c['metric'][0]['count']
                amount=c['metric'][0]['amount']
                map_tra['State'].append(i)
                map_tra['Year'].append(j)
                map_tra['Quarter'].append(int(k.strip('.json')))
                map_tra['District'].append(name)
                map_tra['Transaction_Count'].append(count)
                map_tra['Transaction_Amount'].append(amount)
df_map_tra=pd.DataFrame(map_tra)
df_map_tra['State'] = df_map_tra["State"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
df_map_tra["State"] = df_map_tra["State"].str.replace("-"," ")
df_map_tra["State"] = df_map_tra["State"].str.title()
df_map_tra['State'] = df_map_tra['State'].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Daman and Diu")
#df_map_tra

# Extracting mao user data and making it a df 

path4=r"D:\New folder (2)\pulse\data\map\user\hover\country\india\state"
map_user_state_list=os.listdir(path4)
map_user_state_list

map_user={'State':[], 'Year':[], 'Quarter':[], 'District':[], 'Registered_Users':[]}

for i in map_user_state_list:
    p_i=path4+'/'+i
    map_year=os.listdir(p_i)
    #print(map_year)

    for j in map_year:
        p_j=p_i+'/'+j
        map_year_list=os.listdir(p_j)
        #print(map_year_list)

        for k in map_year_list:
            p_k=p_j+'/'+k
            data=open(p_k,'r')
            D=json.load(data)
            #print(D)

            for d in D['data']['hoverData'].items():
                district=d[0]
                registereduser=d[1]['registeredUsers']
                map_user['State'].append(i)
                map_user['Year'].append(j)
                map_user['Quarter'].append(int(k.strip('.json')))
                map_user['District'].append(district)
                map_user['Registered_Users'].append(registereduser)

df_map_user=pd.DataFrame(map_user)
df_map_user['State'] = df_map_user["State"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
df_map_user["State"] = df_map_user["State"].str.replace("-"," ")
df_map_user["State"] = df_map_user["State"].str.title()
df_map_user['State'] = df_map_user['State'].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Daman and Diu")
#df_map_user
                

# Extracting top transaction data and making it a df 

path5=r"D:\New folder (2)\pulse\data\top\transaction\country\india\state"
top_tra_state_list=os.listdir(path5)
top_tra_state_list

top_tra={'State':[], 'Year':[], 'Quarter':[], 'District_Name':[], 'District_Pincode':[], 'Transaction_Count':[], 'Transaction_Amount':[]}

for i in top_tra_state_list:
    p_i=path5+"/"+i
    top_year=os.listdir(p_i)
    #print(top_year)

    for j in top_year:
        p_j=p_i+"/"+j
        top_year_list=os.listdir(p_j)
        #print(top_year_list)

        for k in top_year_list:
            p_k=p_j+"/"+k
            data=open(p_k,'r')
            E=json.load(data)
            #print(E)

            for e in E['data']['districts']:
                name=e['entityName']
                count=e['metric']['count']
                amount=e['metric']['amount']

                for m in E['data']['pincodes']:
                    keys=['entityName']
                    pincode=[m[i] for i in keys if i in m][0]
                    top_tra['State'].append(i)
                    top_tra['Year'].append(j)
                    top_tra['Quarter'].append(int(k.strip('.josn')))
                    top_tra['District_Name'].append(name)
                    top_tra['District_Pincode'].append(pincode)
                    top_tra['Transaction_Count'].append(count)
                    top_tra['Transaction_Amount'].append(amount)

df_top_tra=pd.DataFrame(top_tra)
df_top_tra['State'] = df_top_tra["State"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
df_top_tra["State"] = df_top_tra["State"].str.replace("-"," ")
df_top_tra["State"] = df_top_tra["State"].str.title()
df_top_tra['State'] = df_top_tra['State'].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Daman and Diu")
#df_top_tra


# Extracting top user data and making it a df 

path6=r"D:\New folder (2)\pulse\data\top\user\country\india\state"
top_user_state_list=os.listdir(path6)
top_user_state_list

top_user={'State':[], 'Year':[], 'Quarter':[], 'District_Name':[], 'District_Pincode':[], 'Registered_User':[]}

for i in top_user_state_list:
    p_i=path6+"/"+i
    top_year=os.listdir(p_i)
    #print(top_year)

    for j in top_year:
        p_j=p_i+"/"+j
        top_year_list=os.listdir(p_j)
        #print(top_year_list)

        for k in top_year_list:
            p_k=p_j+"/"+k
            data=open(p_k,'r')
            F=json.load(data)
            #print(F)
            
            for f in F['data']['districts']:
                name=f['name']
                users=f['registeredUsers']

                
                for g in F['data']['pincodes']:
                    dist_pincode=g['name']
                    
                    top_user['State'].append(i)
                    top_user['Year'].append(j)
                    top_user['Quarter'].append(int(k.strip('.json')))
                    top_user['District_Name'].append(name)
                    top_user['District_Pincode'].append(dist_pincode)
                    top_user['Registered_User'].append(users)

df_top_user=pd.DataFrame(top_user)
df_top_user['State'] = df_top_user["State"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
df_top_user["State"] = df_top_user["State"].str.replace("-"," ")
df_top_user["State"] = df_top_user["State"].str.title()
df_top_user['State'] = df_top_user['State'].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Daman and Diu")
#df_top_user


# SQL connection

mydb=mysql.connector.connect(
    host='localhost',
    user='root',
    password="",
    port='3306'
)
cursor=mydb.cursor()

cursor.execute('create database if not exists phonepe_pulse')
mydb.commit()

engine = create_engine('mysql+pymysql://root:@localhost/phonepe_pulse',echo=False)



# df's to SQL

df_agg_tra.to_sql('aggregate_transaction', engine, if_exists = 'replace', index=False,
                   dtype={'State': sqlalchemy.types.VARCHAR(length=50), 
                          'Year': sqlalchemy.types.Integer, 
                          'Quater': sqlalchemy.types.Integer, 
                          'Transaction_type': sqlalchemy.types.VARCHAR(length=50), 
                          'Transaction_count': sqlalchemy.types.Integer,
                          'Transaction_amount': sqlalchemy.types.Float(precision=5,asdecimal=False)})

df_agg_user.to_sql('aggregate_user', engine, if_exists='replace', index=False,
                   dtype={'State':sqlalchemy.types.VARCHAR(length=50),
                          'Year':sqlalchemy.types.Integer,
                          'Quarter':sqlalchemy.types.Integer,
                          'Brands':sqlalchemy.types.VARCHAR(length=100),
                          'User_Count':sqlalchemy.types.Integer,
                          'User_Percentage':sqlalchemy.types.Float(precision=5,asdecimal=False)})

df_map_tra.to_sql('map_transaction', engine, if_exists='replace', index=False,
                   dtype={'State':sqlalchemy.types.VARCHAR(length=50),
                          'Year':sqlalchemy.types.Integer,
                          'Quarter':sqlalchemy.types.Integer,
                          'District':sqlalchemy.types.VARCHAR(length=100),
                          'Transaction_Count':sqlalchemy.types.Integer,
                          'Transaction_Amount':sqlalchemy.types.Float(precision=5,asdecimal=False)})

df_map_user.to_sql('map_user', engine, if_exists='replace', index=False,
                   dtype={'State':sqlalchemy.types.VARCHAR(length=50),
                          'Year':sqlalchemy.types.Integer,
                          'Quarter':sqlalchemy.types.Integer,
                          'District':sqlalchemy.types.VARCHAR(length=100),
                          'Registered_Users':sqlalchemy.types.Integer})

df_top_tra.to_sql('top_transaction', engine, if_exists='replace', index=False,
                   dtype={'State':sqlalchemy.types.VARCHAR(length=50),
                          'Year':sqlalchemy.types.Integer,
                          'Quarter':sqlalchemy.types.Integer,
                          'District_Name':sqlalchemy.types.VARCHAR(length=100),
                          'District_Pincode':sqlalchemy.types.Integer,
                          'Transaction_Count':sqlalchemy.types.Integer,
                          'Transaction_Amount':sqlalchemy.types.Float(precision=5,asdecimal=False)})

df_top_user.to_sql('top_user', engine, if_exists='replace', index=False,
                   dtype={'State':sqlalchemy.types.VARCHAR(length=50),
                          'Year':sqlalchemy.types.Integer,
                          'Quarter':sqlalchemy.types.Integer,
                          'District_Name':sqlalchemy.types.VARCHAR(length=100),
                          'District_Pincode':sqlalchemy.types.Integer,
                          'Registered_User':sqlalchemy.types.Integer})


