import pandas as pd
from datetime import timedelta, date, datetime
import requests
from pprint import pprint
from sqlalchemy import create_engine, URL
from sqlalchemy.types import *
from db_params import params

# Info de la BD
db = params()

# EMPEZAMOS POR LA CANTIDAD DE NODOS
# el request HTTPS no permite una peticion de muchos nodos al mismo tiempo, al tener 2410 nodos en el SIN se requiere hacer request de 10 en 10
nodos = ['08CYL-115'] # Ingresar el nodo en especifico

for nodo in nodos:
  
  # Si el numero es 20 cortamos el string, quitamos la ultima coma y corremos el proceso para esos nodos

    url_pml = 'https://ws01.cenace.gob.mx:8082/SWPML/SIM/SIN' #Parametros en URL
    formato = 'JSON'
    #Ingresar el rango de fechas a extraer precios
    start_date = date(2019, 1, 1)
    end_date = date(2019, 12, 31)

    delta = end_date - start_date

    # iteramos por cada dia
    df_list = []
    for i in range(delta.days + 1):

        # el dia a hacer request
        day = start_date + timedelta(days=i)
        day = datetime.strptime(str(day),'%Y-%m-%d').strftime('%Y/%m/%d')
        fecha_ini = day
        # MDA
        fecha = []
        horas = []
        mdas = []
        clv_nodo = []
        # MTR
        mtrs = []
        # lista de nodos
        fecha_fin = day

        # MDA
        url = url_pml+'/MDA/'+nodo+'/'+fecha_ini+'/'+fecha_fin+'/'+formato
        #print(url)
        response = requests.request(
            'GET',
            url
        )
        responseJSON = response.json()

        # iteramos por cada nodo
        for i in range(len(responseJSON['Resultados'])):
            # iteramos por cada hora
            for j in range(len(responseJSON['Resultados'][i]['Valores'])):
                # Guardamos el nodo
                clv_nodo.append(responseJSON['Resultados'][i]['clv_nodo'])
                # Guardamos la fecha
                fecha.append(responseJSON['Resultados'][i]['Valores'][j]['fecha'])
                # Guardamos la hora
                horas.append(responseJSON['Resultados'][i]['Valores'][j]['hora'])
                # Guardamos el precio marginal local
                mdas.append(responseJSON['Resultados'][i]['Valores'][j]['pml'])

        print(f'Dia {day} listo!!!')
        dict_pml = {'nodo':clv_nodo, 'fecha':fecha, 'hora':horas, 'mda':mdas}   #, 'mtr':mtrs}
        df = pd.DataFrame(dict_pml)
        df_list.append(df)

        # Si se requiere ingresar a una BD, aqui esta la linea, solo editar de acuerdo a parametros de SQLAlchemy
        # engine = create_engine(f'mysql+mysqlconnector://{db.user}:{db.password}@{db.host}:{db.port}/{db.database}')
    
    big_df = pd.concat(df_list, ignore_index=True)
    big_df.to_excel(f'PATH.xlsx')
    






