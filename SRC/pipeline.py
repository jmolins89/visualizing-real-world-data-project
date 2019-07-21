import adquisition
import cleaning

def extracting(path,y):
    '''
    Carga el dataframe dado un año (y) y un path del mongo.
    '''
    df=adquisition.mongo(path)
    df = adquisition.adquire(df,y)
    return df

def transforming(df,money,year,w1,w2,w3,top):
    '''
    Transforma el dataframe incluyendo datos necesarios para poder realizar el
    estudio Devuelve el dataframe con
    esta información.
    '''
    df2 = cleaning.money_standarize(df)
    df2= cleaning.creating_rows_offices(df2)
    df2=cleaning.add_geoinfo(df2)
    df2 = cleaning.filteringmoney(df2,money)
    df2.to_json('../output/data_clean.json', orient="records")
    df2.to_csv('../output/data_clean.csv')
    df2 = cleaning.moneydensity(df2)
    df2 = cleaning.youngcompanies(df2,year)
    df2 = cleaning.designcompanies(df2)
    df2 = cleaning.standarizing(df2)
    df2 = cleaning.ranking(df,w1,w2,w3)
    df2 = cleaning.selecttop(df,top)
    return df2

def loading(df):
    return df

