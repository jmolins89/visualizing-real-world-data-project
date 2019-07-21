import adquisition
import re
import pandas as pd
import requests as req
from pymongo import MongoClient
import numpy as np


def money_standarize(df):
    '''
    Recorre la columna 'total_money_raised' del dataframe df(input) y obtiene los
    tipos de monedas que hay en el dataframe a estudiar.
    A continuación, se hace una llamada a la API exchange rates API para obtener los
    tipos de cambio en este momento. Aplica los tipos de cambio y multiplica por el
    valor de B,M,k para incluir una nueva columna con el 'total_money_raised' normalizado.
    '''
    letters=[]
    for i in df['total_money_raised']:
        if re.findall('[^0-9.,]+',i) not in letters:
            letters.append(re.findall('[^0-9.,]+',i))
    coins= ['USD','GBP','CAD','SEK']
    coindict={}
    for i in coins:
        res = req.get('https://api.exchangeratesapi.io/latest')
        data = res.json()
        coindict[i] = data['rates'][i]
    dict_quantity={
        '$': 1,
        'M': 1000000, 
        'B':1000000000, 
        'k': 1000, 
        '£':coindict['GBP']/coindict['USD'],  
        'C$':coindict['CAD']/coindict['USD'], 
        'kr':coindict['SEK']/coindict['USD'],
        '€':1/coindict['USD']
    }
    total_money_raised_normalized=[]
    for i in df['total_money_raised']:
        x = float(re.findall('[0-9.]+',str(i))[0])
        y=1
        for j in re.findall('[^0-9.]+',str(i)):
            y = y* dict_quantity[j]
        total_money_raised_normalized.append(x*y)
    df['total_money_raised_normalized_USD']= total_money_raised_normalized
    return df

def creating_rows_offices(df):
    '''
    Cuándo una empresa tiene más de una oficina, genera tantas filas cómo oficinas
    tiene la empresa. Devuelve el df con las nuevas filas.
    '''
    office_list=[]
    for i in df['offices']:
        if not isinstance(i,list):
            office_list.append(list(i))
        else: office_list.append(i)
    df['offices']=office_list
    df2 = pd.DataFrame(df.offices.tolist()).stack().reset_index().drop(columns='level_1')
    df2.columns=['index','office']
    df.reset_index(inplace=True)
    df= df2.merge(df, how='left', on='index')
    return df

def create_latlng(data):
    '''
    Devuelve diccionario con latitud, longitud y geopoint.
    '''
    data = data['office']
    #    return (len(data),data[0]['latitude'],data[0]['longitude'])

    # Only create the geoJSON object if all geodata is available
    principal = None
    if data['latitude'] and data['longitude']:
        principal = {
            "type":"Point",
            "coordinates":[data['longitude'], data['latitude']]
        }

    return {
        "lat": data['latitude'],
        "lng": data['longitude'],
        "oficina_principal": principal
    }

def add_geoinfo(df):
    '''
    Incluye al df las columnas con latitud, longitud y geopoint.
    '''
    df_sub = df[["office"]].apply(create_latlng, result_type="expand", axis=1)
    df2 = pd.concat([df,df_sub], axis=1)
    df2 = df2.drop(columns=['_id','index','alias_list','blog_feed_url','blog_url','crunchbase_url','deadpooled_url','external_links','homepage_url','image','offices','permalink','twitter_username','video_embeds'])
    return df2

def filteringmoney(df,money):
    '''
    Filtra el dataframe (df) a partir de una cantidad de dinero total_money_raised.
    '''
    df2 = df[df['total_money_raised_normalized_USD']>=money][['index', 'office', 'acquisition','category_code', 'deadpooled_year', 'founded_year','funding_rounds', 'investments', 'ipo', 'name', 'number_of_employees', 'products','total_money_raised_normalized_USD', 'lat', 'lng', 'oficina_principal']]
    df2.reset_index(inplace=True)
    return df2

def findNear(geopoint, radio_max_meters=1000):
    '''
    Devuelve las oficinas cercanas dado un geopoint y un radio.
    '''
    client = MongoClient('mongodb://localhost:27017/')
    db = client.companies
    return db.offices.find({
        "oficina_principal": {
         "$near": {
           "$geometry": geopoint,
           "$maxDistance": radio_max_meters,
         }
       }
    })

def moneydensity(df):
    '''
    Calcula la densidad monetaria que hay alrededor de cada oficina.
    '''
    near=[]
    for i in range(len(df)):
        near.append(findNear(df['oficina_principal'][i]).count())
    near_money=[]
    for i in range(len(df)):
        x= findNear(df['oficina_principal'][i])#.count()-1)
        total=0
        for j in range(x.count()):
            total+=x[j]['total_money_raised_normalized_USD']
        near_money.append(total)
    money_density=[]
    for i in range(len(near)):
        money_density.append(near_money[i]/near[i])
    df['money_density']=money_density
    df = df.sort_values(by='money_density',ascending=False).drop(['level_0','index'],axis=1)
    df.reset_index(drop=True, inplace=True)
    return df

def youngcompanies(df,criterio=2010):
    '''
    Encuentra el número de compañías jóvenes que hay alrededor de la oficina y
    los incluye en le dataframe
    '''
    young_companies=[]
    for i in range(len(df)):
        x= findNear(df['oficina_principal'][i])#.count()-1)
        young=0
        for j in range(x.count()):
            if x[j]['founded_year']>=criterio:
                young+=1
        young_companies.append(young)
    df['young_companies']=young_companies
    return df

def designcompanies(df):
    design_companies=[]
    for i in range(len(df)):
        x= findNear(df['oficina_principal'][i])#.count()-1)
        design=0
        for j in range(x.count()):
            if x[j]['category_code']=='design':
                design+=1
        design_companies.append(design)
    df['design_companies']=design_companies
    return df

def standarize(x):
    return (x - np.min(x))/(np.max(x)-np.min(x))

def standarizing(df):
    '''
    Devuelve el dataframe con columnas money density, young companies y design
    companies estandarizadas (se añaden nuevas columnas al dataframe)
    '''
    money_density_standard = standarize(df['money_density'])
    young_companies_standard = standarize(df['young_companies'])
    design_companies_standard = standarize(df['design_companies'])
    df['money_density_standard']=money_density_standard
    df['young_companies_standard']=young_companies_standard
    df['design_companies_standard']=design_companies_standard
    return df

def ranking(df,w1,w2,w3):
    '''
    Devuelve el dataframe ordenado por un ranking segun el peso que se le de a:
    w1 para densidad monetaria
    w2 para empresas jóvenes
    w3 para empresas de diseño
    '''
    ranking=[]
    for i in range(len(df)):
        ranking.append((df['money_density_standard'][i])*w1+ (df['young_companies_standard'][i])*w2+(df['design_companies'][i])*w3)
    df['ranking']=ranking
    df = df.sort_values(by=['ranking','money_density','young_companies','design_companies'],ascending=[False,False,False,False])
    return df

def selecttop(df,top):
    df = df[:top].reset_index()
    return df


