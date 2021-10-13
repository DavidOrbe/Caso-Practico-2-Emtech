# -*- coding: utf-8 -*-
"""
Created on Mon Oct 11 21:15:36 2021

@author: david
"""
'''
El objetivo de este codigo es analizar los datos proporcionados por Synergy Logistics mediante herramientas esenciales 
de python para analisis de datos como  condicionales, ciclos for, estructuras de datos (conjuntos,tuplas,listas,diccionarios),comparaciones y creacion de funciones
el uso de pandas y dataframes es solamente para imprimir en consola los datos de manera mas ordenada
'''

import csv
import pandas as pd

with open('synergy_logistics_database.csv','r') as base_datos_synergy:
    lectura_datos=csv.reader(base_datos_synergy)
    descomprimir_datos=zip(*lectura_datos) #descomprimimos los datos
    datos_por_columna=list(descomprimir_datos) #Los transformamos en una lista de tuplas que contienen los datos

'''
indices:
    0-id, 1-direction, 2-origin, 3-destination, 4-year
    5-date, 6-product, 7-transport_mode, 8-company_name, 9-total_value
'''
#Al convertir en conjunto cada tupla de datos obtenemos los elementos sin repetir de los datos   
años=set(datos_por_columna[4])
años.remove('year')
años=list(años) #convertimos a lista para ordenar los años
años.sort()
transportes=set(datos_por_columna[7])
transportes.remove('transport_mode')

#Opcion 1

# Flujo y valor de importacion y exportacion en 5 años (numero de veces que se importa o exporta)

def flujo_import_export(tipo_intercambio):
    ruta_comercial=[] #Lista que contendra las rutas comerciales [ruta,valor]
    for indice in range(0,len(datos_por_columna[2])): #ciclo que recorre en todos los elementos de las tuplas que elijamos
          if datos_por_columna[1][indice]==tipo_intercambio: #condicion para saber si es exportacio o importacion
              ruta_comercial.append([datos_por_columna[2][indice]+'-'+datos_por_columna[3][indice],datos_por_columna[9][indice]])
    
    rutas=[i[0] for i in ruta_comercial] #lista con solo las rutas
    rutas=set(rutas)    #Eliminamos rutas repetidas
    flujo_rutas=[] #[ruta,valor,flujo]
    value_exp_import=0 #contador del valor de export/import
    flujo=0             #contador de numero de veces que aparece la ruta
    for ruta in rutas:
        for ruta_comer in ruta_comercial:
            if ruta==ruta_comer[0]:
                value_exp_import+=float(ruta_comer[1])
                flujo+=1
        if value_exp_import !=0:
            flujo_rutas.append([ruta,value_exp_import,flujo])
            value_exp_import=0
            flujo=0
    por_mayor_valor=sorted(flujo_rutas,reverse=True,key=lambda x:x[1])
    por_mayor_flujo=sorted(flujo_rutas,reverse=True,key=lambda x:x[2])
    
    '''
    Usamos dataframes en esta parte solo para mostrar mejor la informacion en pantalla
    '''
    por_mayor_valor=pd.DataFrame(por_mayor_valor)
    por_mayor_valor.columns=['Ruta','Valor','Flujo']
    print(tipo_intercambio+ ' Rutas')
    print('Por mayor Valor')
    print(por_mayor_valor[0:10]) #Las primeras 10 rutas
    print()
    por_mayor_flujo=pd.DataFrame(por_mayor_flujo)
    por_mayor_flujo.columns=['Ruta','Valor','Flujo']
    print(tipo_intercambio+ ' Rutas')
    print('Por mayor Flujo')
    print(por_mayor_flujo[0:10])
    print()
    return por_mayor_valor,por_mayor_flujo
    
export_valor, export_flujo =flujo_import_export('Exports')
import_valor, import_flujo =flujo_import_export('Imports')

# #opcion 2
#Medio de transporte mas utilizado (valor,flujo, analisis anual y total en 5 años)

def transportacion(tipo_intercambio,analisis):

    value_transportes=[] #Lista que contendra los valores de cada transporte [transporte,año,valor,]
    value_total_transportes=[] #Lista que contendra la suma de los valores individuales de cada transporte
    value_imp_exp_transp=0 #contador para el valor de la import/export
    flujo=0 #Contador para el numero de veces que se utiliza el transporte
    
    if analisis=='anual': #condicion para el tipo de analisis anual/total
        for indice in range(0,len(datos_por_columna[2])):
            if datos_por_columna[1][indice]==tipo_intercambio: 
                value_transportes.append([datos_por_columna[7][indice],datos_por_columna[4][indice],datos_por_columna[9][indice]])
        #tres ciclos for anidados que van comparanda el año, el tipo de transporte y el valor de import/export correspondiente    
        for año in años:
            for transporte in transportes:
                for valor_transp in value_transportes:
                    if año==valor_transp[1]:
                        if transporte==valor_transp[0]:
                            value_imp_exp_transp+=float(valor_transp[2])
                            flujo+=1
                if value_imp_exp_transp !=0:
                    value_total_transportes.append([transporte,año,value_imp_exp_transp,flujo])
                    value_imp_exp_transp=0
                    flujo=0
        value_total_transportes.sort(reverse=True,key=lambda x:x[0])
        value_total_transportes=pd.DataFrame(value_total_transportes)
        value_total_transportes.columns=['Transporte','Año','Valor','Flujo']
        print(tipo_intercambio+ ' Transporte')
        print('Anual')
        print()
    #Mismo analisis pero sin contar los años    
    if analisis=='total':
        for indice in range(0,len(datos_por_columna[2])):
            if datos_por_columna[1][indice]==tipo_intercambio: 
                value_transportes.append([datos_por_columna[7][indice],datos_por_columna[9][indice]])
        for transporte in transportes:
            for valor_transp in value_transportes:
                if transporte==valor_transp[0]:
                    value_imp_exp_transp+=float(valor_transp[1])
                    flujo+=1
            if value_imp_exp_transp !=0:
                value_total_transportes.append([transporte,value_imp_exp_transp,flujo])
                value_imp_exp_transp=0
                flujo=0
        value_total_transportes.sort(reverse=True,key=lambda x:x[1])
    
        value_total_transportes=pd.DataFrame(value_total_transportes)
        value_total_transportes.columns=['Transporte','Valor','Flujo']
        print(tipo_intercambio+' Transporte')
        print('Total')
        print()
    print(value_total_transportes)
    print()
    return value_total_transportes

#Se llama cada caso en la funcion
transport_exp_anual=transportacion('Exports','anual')
transport_exp_total=transportacion('Exports','total')
transpor_imp_anual=transportacion('Imports','anual')
transpor_imp_total=transportacion('Imports','total')   

#Opcion 3
'''
NOTA: Entendemos como el origen el pais exportador cuando se realiza una exportacion
y el destino como el pais importador cuando se realiza una importacion 
'''
def porcentaje_exp_imp(tipo_intercambio):
    conjunto_exportadores_importadores=[] #Lista que contendra los paises importadores/export
    for indice in range(0,len(datos_por_columna[2])): 
        if datos_por_columna[1][indice]==tipo_intercambio: #condicion para saber si es exportacio o importacion
            conjunto_exportadores_importadores.append([datos_por_columna[2][indice],datos_por_columna[3][indice],datos_por_columna[9][indice]])
    
    if tipo_intercambio=='Imports': #condicion para saber si exportamos/importamos
        export_import=[i[1] for i in conjunto_exportadores_importadores] #lista con solo los paises export/import
    elif tipo_intercambio=='Exports':
         export_import=[i[0] for i in conjunto_exportadores_importadores] #lista con solo los paises export/import

    export_import=set(export_import)    #Eliminamos paises repetidos
    exportaciones_importaciones=[] #[exportador/importador,valor,porcentaje]
    conjunto_total=[float(i[2]) for i in conjunto_exportadores_importadores ] #generamos una lista de los valores de export/import
    total=sum(conjunto_total) #sumamos para obtener el valor total
    valor_exp_imp=0
    
    if tipo_intercambio=='Imports': 
        #ciclos anidados que compara cada pais con el conjunto completo con la finalidad de contabilizar su valor y obtener su porcentaje
        for exportador_importador in export_import:
            for exportacion_importacion in conjunto_exportadores_importadores:
                if exportador_importador==exportacion_importacion[1]:
                    valor_exp_imp+=float(exportacion_importacion[2])
            if valor_exp_imp !=0:
                exportaciones_importaciones.append([exportador_importador,valor_exp_imp,(valor_exp_imp*100)/(total)])
                valor_exp_imp=0
                
    elif tipo_intercambio=='Exports':
        for exportador_importador in export_import:
            for exportacion_importacion in conjunto_exportadores_importadores:
                if exportador_importador==exportacion_importacion[0]:
                    valor_exp_imp+=float(exportacion_importacion[2])
            if valor_exp_imp !=0:
                exportaciones_importaciones.append([exportador_importador,valor_exp_imp,(valor_exp_imp*100)/(total)])
                valor_exp_imp=0    
            
    exportaciones_importaciones.sort(reverse=True,key=lambda x:x[2])
    exportaciones_importaciones=pd.DataFrame(exportaciones_importaciones)
    exportaciones_importaciones.columns=['Pais','Valor','Porcentaje']
    print(tipo_intercambio+' Porcentaje')
    print(exportaciones_importaciones)
    print()
    return exportaciones_importaciones

porcent_export=porcentaje_exp_imp('Exports')
porcent_import=porcentaje_exp_imp('Imports')