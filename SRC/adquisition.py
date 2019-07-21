import pandas as pd
import numpy as np
import requests as req
import os
from dotenv import load_dotenv
load_dotenv()
from pymongo import MongoClient
import folium
from folium import plugins
from folium.plugins import MiniMap

api_key = os.environ["KEY"] 

def mongo(path):
    '''
    Carga datos de MongoDB mediante PyMongo. Devuelve DataFrame de companies.
    '''
    client = MongoClient(path)
    db = client.companies
    df = db.companies
    return df

def adquire(df,y): 
    '''
    Query de mongo filtrando por empresas tecnológicas, con información sobre oficinas, 
    con empleados, fundadas a partir del año 'y' (input) y que no hayan quebrado. 
    Devuelve el dataframe filtrado por estas variables.
    '''   
    companies_with_coord = pd.DataFrame(df.find(
    {'$and': 
     [
         {'$or':
          [
         {'category_code':'games_video'},
         {'category_code':'web'},
         {'category_code':'software'},
         {'category_code':'mobile'},
         {'category_code':'advertising'},
         {'category_code':'ecommerce'},
         {'category_code':'design'},
         {'category_code':'biotech'},
         {'category_code':'cleantech'},
         {'category_code':'travel'}
     ]
         },
         {"number_of_employees": 
          {'$not': 
           {'$eq': None}
          }
         },
         {"offices.latitude": 
          {'$exists': True}
         },
          {"founded_year": 
          {'$gte': y}
         },
         {"offices.longitude": 
          {'$exists': True}
         },
         {"offices.latitude": 
          {'$not': 
           {'$eq': None}
          }
         },
         {"offices.longitude": 
          {'$not': 
           {'$eq': None}
          }
         },
         {'deadpooled_year': 
             {'$eq' :None}
         }
     ]
    }))
    return companies_with_coord




