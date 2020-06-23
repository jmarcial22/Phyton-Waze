#!/usr/bin/env python3.6

import json
import requests
import datetime
import time

def obtenerUpdateJson (json1): ##Recibe a Json y le agrega "doc:" al inicio
 strdoc    ='{"doc":'
 centinela = strdoc + json.dumps(json1)+'}' 
 jsonline  = json.loads(centinela)
   
 return jsonline

def obtenerRegistroIdJson (idx_name,field_name,id): ##Recibe la conexión a Elastic,Nombre del indice,campo a buscar, y id a buscar, devuelve un JSON con el resultado
 from elasticsearch import Elasticsearch
 es2 = Elasticsearch([{'host': '10.10.10.12', 'port': 9200}])
 
 search_object = {'query': {'match': {field_name: id}}}
 #ime.sleep(1)
 res = es2.search(index=idx_name, body=search_object)  

 total = res['hits']['total'] 
 
 if total> 0:
  sw=res['hits']['hits'][0]['_id']  #Return the document _id
 else:
  sw='none'                         #Return the 'none' information
 
 return sw

#Procedimiento para moler alertas de Waze
def moler_alertas(alertas,fechaDescarga,es):
  arraux = []
  #print ('iniciando')
  arraux.append('val1')
  fech = fechaDescarga
  i=0
  for alerta in data_alerts:
   i=i+1
   encontrado=0
   uuid_alerta = alerta['uuid']
   #print ('UUiid: '+uuid_alerta)
   #En este punto buscamos si ya el valor de uuid fue guardado, en caso contrario se guarda en el vector
   for uuid in arraux:
    if uuid_alerta == uuid:
     print ('encontrado repetido: '+uuid_alerta)  
     encontrado=1
     break

   if encontrado == 0:
    arraux.append(uuid_alerta)
        
   alerta['fecha_descarga'] = fech

   x= alerta['location']['x']
   y= alerta['location']['y']

   del alerta['location']
   alerta['location']= {'lat':y,'lon':x}

   #Aquí enviamos al información a ElasticSearch
   if encontrado ==0: #Aquí guardamos o actualizamos, busca no repetidos en el feed para guardarlos, si encuentra un repetido no lo procesa
    try:
     id_documento = obtenerRegistroIdJson('waze-alertss','uuid',uuid_alerta)
    except KeyError:
     id_documento='none'	 

    if ( id_documento== 'none'): #Se buscan los documentos en el indice destino, sino se encuentra se guarda
     print('Guardando dato Uuid: '+alerta['uuid'])
     es.index(index='waze-alertss',doc_type='alerts',  body=alerta)
    else:
     print('udpate: '+id_documento)
     updatestr = obtenerUpdateJson(alerta)
     es.update(index='waze-alertss',doc_type='alerts',id=id_documento,body=updatestr)  	
   

  print (i) 

#Aquí nos conectamos a ElasticSearch
#connect to our cluster
from elasticsearch import Elasticsearch
es = Elasticsearch([{'host': '10.10.10.12', 'port': 9200}]) #Aqui colocas el nombre del servidor


#Aquí se lee el JSON de los polígonos
poligonos=  json.loads(open('/etc/logstash/conf.d/scripts/sources/poligonos.json').read())  #Aqui guardas un archivo con los poligonos

for poligono in poligonos:
  #print (poligono['url']) 
  #print ("\n")

  #Aquí se obtienen los datos desde Waze*************#######
  waze_data_poligon1 = requests.get (poligono['url'])
  data = json.loads(waze_data_poligon1.content)

  #****************************#
  #Aqui se obtiene la fecha de procesamiento Waze
  fec_desc=data['startTimeMillis']     
#print 
  #Aquí se obtienen las alertas del polígono 1
  print ("procesando poligono: "+poligono['name']+' Fecha: '+time.strftime('%H:%M:%S'))
  try: 
   data_alerts = data['alerts']
   moler_alertas(data_alerts,fec_desc,es)
  except KeyError:
   print ("No hay alertas")





