from selenium import webdriver
from selenium.webdriver.support.ui import Select
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.keys import Keys

# Elimina esta línea: import datetime
import os
import pandas as pd
import psycopg2
import re
from psycopg2 import extras
from datetime import datetime  # Mantén esta importación
import numpy as np
import boto3
from weasyprint import HTML, CSS
import pymupdf  # PyMuPDF
import pandas as pd
import fitz
from datetime import date
import sys
import threading
from concurrent.futures import ThreadPoolExecutor




import time
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import TimeoutException  # Importa TimeoutException
from selenium.webdriver.support.select import Select
from selenium.common.exceptions import StaleElementReferenceException
from selenium.common.exceptions import WebDriverException
from psycopg2 import sql





download_folder = "/home/kevin/Documentos/web_scrap/descargas/descargasuax"

# Configuración del User-Agent
user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.6778.85 Safari/537.36"

options = webdriver.ChromeOptions()
prefs = {
    # Para desactivar la solicitud de descarga
    "download.prompt_for_download": False,
    # Para hacer que el directorio establecido sea la ubicación de descarga predeterminada
    "download.directory_upgrade": True,
    # Para evitar que el archivo se marque como peligroso y se bloquee
    "safebrowsing.enabled": True,
    # Desactiva la protección para descargas
    "safebrowsing.disable_download_protection": True,
    # Permite descargas automáticas
    "profile.default_content_setting_values.automatic_downloads": 1,
    "download.default_directory": "/home/kevin/Documentos/web_scrap/descargas/descargasuax"
}

options.add_experimental_option("prefs", prefs)

# Aumentar el tamaño de la ventana
options.add_argument("--window-size=1920,1080")
options.add_argument('--log-level 3')
options.add_argument(f"user-agent={user_agent}")
options.add_argument("--disable-blink-features=AutomationControlled")
options.add_experimental_option("excludeSwitches", ["enable-automation"])
options.add_experimental_option("useAutomationExtension", False)
#options.add_argument("--headless")  # Opcional: Ejecutar sin interfaz gráfica
options.add_argument("--disable-gpu")  # Recomendado para headless
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")


Excepciones = " "



queue_url = 'https://sqs.us-east-1.amazonaws.com/654654188121/colajsoonverionuno_kevin'
aws_access_key = os.environ.get("AWS_ACCESS_KEY")
aws_secret_key = os.environ.get("AWS_SECRET_KEY")
aws_region = os.environ.get("AWS_REGION")
sqs = boto3.client('sqs')
receipt_handle = " "



#variables de seguimiento

rfc_seguimiento = "00000000000000"
Fecha_actual_de_intento_de_obtencion = datetime.now().date() # Tipo datetime64[ns]
status_404 = "no"
intento_login = "falso"
status_capa_uno = "falso"
status_capa_dos = "falso"
status_capa_tres = "falso"
status_finalizado = "falso"

Excepciones = " "

numero_de_intentos_login = 0

#direcion base 
direcion_base_programa = os.path.dirname(os.path.abspath(__file__))


# Datos de conexión a PostgreSQL (declarados de forma global)
"""DB_CONFIG = {
    'dbname': 'Sat_facturas_pruebas',
    'user': 'postgres',
    'password': 'Admin',
    'host': 'localhost',  # Cambiar si es un servidor remoto
    'port': 5432  
    

}"""

def Descarga_de_tabla(driver):
    lista_dfs = []
     # Localizar el contenedor principal del paginador por su ID
    contenedor_paginador = driver.find_element(By.ID, "ctl00_MainContent_pageNavPosition")
    # Contar los elementos con la clase 'pg-normal'
    elementos_pg_normal = contenedor_paginador.find_elements(By.CLASS_NAME, "pg-normal")
    elementos_pg_selected = contenedor_paginador.find_elements(By.CLASS_NAME, "pg-selected")
    total_paginador = len(elementos_pg_normal)   + len(elementos_pg_selected)
    
    print(f"Número total de elementos con la clase 'pg-normal': {total_paginador}")
    time.sleep(5)
    tabla_id = "ctl00_MainContent_tblResult"
    
    time.sleep(10)
    
    
    while(h<total_paginador):
        
        tabla_id = "ctl00_MainContent_tblResult"
        #numero_de_filas = obtencions_de_filas_by_regex(tabla_id)
        #numero_de_filas += 1
        time.sleep(5)
        #numero_de_columnas = 17
        #ii = 2
        #numero_de_columnas = 17
        #print("numero en iteracion", ii)
        #print("valor de numero de filas",numero_de_filas)
        
        
        
        
        # Extraer la tabla de la página
        table_element = driver.find_element(By.ID, tabla_id)

        if table_element:
            table_html = table_element.get_attribute("outerHTML")  # Extrae el HTML como texto

            # CSS corregido
            css = '''
            @page {
                size: 50in 16in; /* Ancho de 14 pulgadas y alto de 11 pulgadas */
                margin: 1in; /* Márgenes opcionales */
                font-size: 14px;
            }
            table {
                width: 90%;
                border-collapse: collapse;
            }
            th, td {
                border: 1px solid black;
                padding: 8px;
                text-align: left;
            }
            th {
                background-color: #f2f2f2;
            }
            '''

            # Ruta de salida
            direccion_general = '/home/kevin/Documentos/sat_facturas_tabla/Pruebas_hilos/pdfs'
            
            # Generar timestamp sin caracteres inválidos
            hora_actual = datetime.now()
            hora_formateada = hora_actual.strftime("%H_%M_%S")  # Reemplazar ':' por '_'

            # Convertir HTML a PDF
            html = HTML(string=table_html)
            output_pdf = f"{direccion_general}/output_{hora_formateada}.pdf"
            html.write_pdf(output_pdf, stylesheets=[CSS(string=css)])

            pdf = output_pdf
            print(f"PDF generado: {pdf}")
            
            try:
                print("concatenando dataframes")
                pdf_read = fitz.open(pdf)
                doc = pymupdf.open(pdf_read)
                # Load a desired page. This works via 0-based numbers
                page = doc[0]  # this is the first page
                # Look for tables on this page and display the table count
                tabs = page.find_tables()
                print(f"{len(tabs.tables)} table(s) on {page}")
                print(tabs.tables)
                # We will see a message like "1 table(s) on page 0 of input.pdf"
                # choose the second table for conversion to a DataFrame
                tab = tabs[0]
                df_aux = tab.to_pandas()
                lista_dfs.append(df_aux)
            except:
                print("fallo")
        else:
            print("⚠️ No se encontró la tabla con el ID especificado.")
        
        try:
            # Intentar encontrar el elemento <li> con el texto `pager.next();`
            #elemento_siguiente_1 = driver.find_element(By.XPATH, "//li[.//a[text()='»']]")
            #elemento_siguiente_1 = driver.find_element(By.CSS_SELECTOR, "li[onclick='pager.next()']")
            time.sleep(5)
            elemento_siguiente_1 = driver.find_element(By.XPATH, "//li[contains(@onclick, 'pager.next()')]/a")
                        
            # Si el elemento existe, presionarlo
            elemento_siguiente_1.click()
            h += 1
                
            print("Botón encontrado y presionado con éxito.")
            print("valor de h",h)
                
        except NoSuchElementException:
                # Manejo si el elemento no se encuentra
                print("Botón no encontrado.")
        
        return lista_dfs

def dividir_en_pares(inicio, fin, partes):
    paso = (fin - inicio + 1) // partes
    return [(i, i + paso - 1) for i in range(inicio, fin, paso)]


def segundo_datapicker(driver,id_data_picker_2,fecha_fin,hora_fin,hora_fin_dividida):
    print("iniciando el segundo datapicker")
    titulo = " "
    fecha_comparacion = fecha_fin[0] + " " + fecha_fin[1]
    print("fecha a comparar",fecha_comparacion )
    dia = fecha_fin[2]
    hora = hora_fin[0]
    minutos = hora_fin[1]
    segundos = hora_fin[2]
    print("iniciando data_picker 2")
    # Localiza el contenedor del datepicker
    datepicker = driver.find_element(By.ID, id_data_picker_2)
    
    # Localiza los botones para cambiar de mes/año
    prev_button = datepicker.find_element(By.XPATH, "//*[@id='datepicker']/table/tbody/tr[1]/td[1]/button")
    next_button = datepicker.find_element(By.XPATH, "//*[@id='datepicker']/table/tbody/tr[1]/td[3]/button")
    
     #titulo = datepicker.find_element(By.CLASS_NAME, "dpTitleText").text
    time.sleep(10)
    while(titulo != fecha_comparacion):
        titulo = datepicker.find_element(By.CLASS_NAME, "dpTitleText").text
        
        if(titulo == fecha_comparacion):
            break
        
        print("valor actual de la fecha",titulo)
        datepicker = driver.find_element(By.ID, id_data_picker_2)
        prev_button = datepicker.find_element(By.XPATH, "//*[@id='datepicker']/table/tbody/tr[1]/td[1]/button")
        driver.execute_script("arguments[0].click();", prev_button)
    
    
    
    time.sleep(7)
    try:
        xpath_dia = f"//td[@class='dpTD' and text()='{str(dia)}']"
        dia_element = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, xpath_dia))
            )
        dia_element.click()
    except Exception as e:
        try:
            print("intentando opcion 2")
            
            xpath_dia = f"//td[@class='dpDayHighlightTD' and contains(., '{str(dia)}')]"

            dia_element = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, xpath_dia))
            )
            dia_element.click()
        except Excepciones as e:
            print(f"fallo al entrar al presionar dia: {e}")
    
    try:
        print(f"Tipo de hora antes de conversión: {type(hora)}, valor: {hora}")
        print(f"Tipo de minutos antes de conversión: {type(minutos)}, valor: {minutos}")
        print(f"Tipo de segundos antes de conversión: {type(segundos)}, valor: {segundos}")

        hora = str(hora)  
        minutos = str(minutos)
        segundos = str(segundos)

        if not (0 <= int(hora) <= 23):
            raise ValueError("La hora tiene que estar entre 0 y 23.")
        if not (0 <= int(minutos) <= 59):
            raise ValueError("Los minutos tienen que estar entre 0 y 59.")
        if not (0 <= int (segundos) <= 59):
            raise ValueError("Los segundos tienen que estar entre 0 y 59.")
        
        if (int(hora)<10):
            hora = str(hora).zfill(2)
        if (int(minutos)<10):
            minutos = str(minutos).zfill(2)
        if (int(segundos)<10):
            segundos = str(segundos).zfill(2)

        select_box_hora = driver.find_element(By.ID, 'ctl00_MainContent_CldFechaFinal2_DdlHora')
        select_hora = Select(select_box_hora)
        select_hora.select_by_visible_text(str(hora))  
        print(f"Seleccionado: {hora}")

        time.sleep(3)

        select_box_minutos = driver.find_element(By.ID, 'ctl00_MainContent_CldFechaFinal2_DdlMinuto')
        select_minutos = Select(select_box_minutos)
        select_minutos.select_by_visible_text(str(minutos)) 
        print(f"Seleccionado: {minutos}")

        time.sleep(3)

        select_box_segundos = driver.find_element(By.ID, 'ctl00_MainContent_CldFechaFinal2_DdlSegundo')
        select_segundos = Select(select_box_segundos)
        select_segundos.select_by_visible_text(str(segundos))  
        print(f"Seleccionado: {segundos}")

        time.sleep(3)

    except Exception as e:
        print(f"fallo al entrar a horas: {e}")


def primer_datapicker(fecha_inicio, horas_inicio,driver,hora_inicio_dividida):
    print(f"primer data picker    {fecha_inicio}, {horas_inicio}, {hora_inicio_dividida}")
    title = driver.title
    print(title)
    time.sleep(5)
    #acciones con el calendario numero uno
    time.sleep(8)
    # Buscar el elemento por su ID
    try:
        calendar_button = driver.find_element(By.ID, "ctl00_MainContent_CldFechaInicial2_BtnFecha2")

        # Opcional: Asegurarte de que el botón sea visible y habilitado antes de hacer clic
        if calendar_button.is_displayed() and calendar_button.is_enabled():
            # Hacer clic en el botón
            driver.execute_script("arguments[0].click();", calendar_button)
            print("Botón presionado correctamente.")
        else:
            print("El botón no está disponible para interactuar.")
        id_data_picker = "datepicker"
    except Exception as e:
        print(f"error en el primer datapicker")
    
    titulo = " "
    fecha_comparacion = fecha_inicio[0] + " " + fecha_inicio[1]
    print("fecha a comparar",fecha_comparacion )
    dia = fecha_inicio[2]
    hora = (hora_inicio_dividida)
    print("hora pasada por parametro", hora)
    minutos = hora_inicio[1]
    segundos = hora_inicio[2]
    
    print("iniciando data_picker 1")
    # Localiza el contenedor del datepicker
    datepicker = driver.find_element(By.ID, id_data_picker)
    
    # Localiza los botones para cambiar de mes/año
    prev_button = datepicker.find_element(By.XPATH, "//*[@id='datepicker']/table/tbody/tr[1]/td[1]/button")
    next_button = datepicker.find_element(By.XPATH, "//*[@id='datepicker']/table/tbody/tr[1]/td[3]/button")
    #titulo = datepicker.find_element(By.CLASS_NAME, "dpTitleText").text
    time.sleep(10)
    while(titulo != fecha_comparacion):
        titulo = datepicker.find_element(By.CLASS_NAME, "dpTitleText").text
        
        if(titulo == fecha_comparacion):
            break
        
        print("valor actual de la fecha",titulo)
        datepicker = driver.find_element(By.ID, id_data_picker)
        prev_button = datepicker.find_element(By.XPATH, "//*[@id='datepicker']/table/tbody/tr[1]/td[1]/button")
        driver.execute_script("arguments[0].click();", prev_button)
    
    time.sleep(7)
    xpath_dia = f"//td[@class='dpTD' and text()='{str(dia)}']"
    dia_element = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, xpath_dia))
        )
    driver.execute_script("arguments[0].click();", dia_element)
    
    time.sleep(12)
    try:
        print(f"Tipo de hora antes de conversión: {type(hora)}, valor: {hora}")
        print(f"Tipo de minutos antes de conversión: {type(minutos)}, valor: {minutos}")
        print(f"Tipo de segundos antes de conversión: {type(segundos)}, valor: {segundos}")

        hora = str(hora)  
        minutos = str(minutos)
        segundos = str(segundos)

        if not (0 <= int(hora) <= 23):
            raise ValueError("La hora tiene que estar entre 0 y 23.")
        if not (0 <= int(minutos) <= 59):
            raise ValueError("Los minutos tienen que estar entre 0 y 59.")
        if not (0 <= int (segundos) <= 59):
            raise ValueError("Los segundos tienen que estar entre 0 y 59.")
        
        if (int(hora)<10):
            hora = str(hora).zfill(2)
        if (int(minutos)<10):
            minutos = str(minutos).zfill(2)
        if (int(segundos)<10):
            segundos = str(segundos).zfill(2)

        select_box_hora = driver.find_element(By.ID, 'ctl00_MainContent_CldFechaInicial2_DdlHora')
        select_hora = Select(select_box_hora)
        select_hora.select_by_visible_text(str(hora))  
        print(f"Seleccionado: {hora}")

        time.sleep(3)

        select_box_minutos = driver.find_element(By.ID, 'ctl00_MainContent_CldFechaInicial2_DdlMinuto')
        select_minutos = Select(select_box_minutos)
        select_minutos.select_by_visible_text(str(minutos)) 
        print(f"Seleccionado: {minutos}")

        time.sleep(3)

        select_box_segundos = driver.find_element(By.ID, 'ctl00_MainContent_CldFechaInicial2_DdlSegundo')
        select_segundos = Select(select_box_segundos)
        select_segundos.select_by_visible_text(str(segundos))  
        print(f"Seleccionado: {segundos}")

        time.sleep(3)

    except Exception as e:
        print(f"fallo al entrar a horas: {e}")
    

def interaccion_principal(inicio, fin, url, cert, key, pswd, fecha_inicio, fecha_fin, hora_inicio, hora_fin):
    print(f"Procesando desde {inicio} hasta {fin} en {url}, {cert}, {key}, {pswd}, {fecha_inicio}, {fecha_fin}, {hora_inicio}, {hora_inicio}, {hora_fin}")
    direccion_pdfs = "/home/kevin/Documentos/sat_facturas_tabla/Pruebas_hilos/pdfs"
    print(f"{threading.current_thread().name} ejecutando acción: {inicio}")
    time.sleep(10)  # Simula una interacción
    options = webdriver.ChromeOptions()
    driver = webdriver.Chrome(options=options)  # Instancia separada para cada hilo
    driver.get(url)
    print("inicio", inicio)
    print("fin",fin)
    time.sleep(10)
    # XPath del elemento que buscamos
    img_xpath = '//img[@src="https://framework-gb.cdn.gob.mx/landing/img/logoheader.svg"]'

    # Bucle hasta que el elemento aparezca
    
    try:
            # Intentar encontrar la imagen
            elemento = driver.find_element(By.XPATH, img_xpath)
            print(" El elemento existe.")
              # Sale del bucle si se encuentra el elemento
    except NoSuchElementException:
            print("No existe. Refrescando...")
            driver.refresh()  # Refresca la página
            time.sleep(10)  # Espera 3 segundos antes de volver a verificar
        
    # Ahora encuentra y presiona el botón
    button = driver.find_element(By.ID, "buttonFiel")
    button.click()
    time.sleep(25)
    # Obtiene el título de la página
    title = driver.title
    print(f"Título de la página: {title}")
    try:

        time.sleep(5)

        input_file = driver.find_element(By.ID, 'fileCertificate')
        input_file.send_keys(cert)

        time.sleep(5)

        input_file = driver.find_element(By.ID, 'filePrivateKey')
        input_file.send_keys(key)

        time.sleep(5)

        password_field = driver.find_element(By.ID, 'privateKeyPassword')
        if password_field.get_attribute('value') == "":
            
                    
            password_field.send_keys(pswd)
        else:
            # Si no está vacío, limpia el campo
            password_field.clear()
            print("El campo de contraseña se limpió.")
            password_field.send_keys(pswd)
                    
            time.sleep(5)

        boton = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.ID, "submit"))
        )
        boton.click()
        time.sleep(18)
        # Encuentra el elemento por su XPath
        xpath_enlace = "/html/body/form/main/div[1]/div[2]/div[1]/div/div[1]/div/nav/ul/div[1]/li/a"
        
        try:
            # Intentar encontrar la imagen
            elemento_xpath_enlace = driver.find_element(By.XPATH, xpath_enlace)
            print(" El elemento existe.")
            elemento_xpath_enlace.click()
            #break  # Sale del bucle si se encuentra el elemento
        except NoSuchElementException:
                print(" No existe. Refrescando...")
                driver.refresh()  # Refresca la página
                time.sleep(10)  # Espera 3 segundos antes de volver a verificar
        
        time.sleep(5)
        radio_button = driver.find_element(By.ID, "ctl00_MainContent_RdoFechas")

            
        driver.execute_script("arguments[0].click();", radio_button)

        print("Radio button seleccionado correctamente.")
        primer_datapicker(fecha_inicio,hora_inicio, driver,inicio)
        print("continuando con segundo data_picker")
        time.sleep(5)
        calendar_button_2 = driver.find_element(By.ID, "ctl00_MainContent_CldFechaFinal2_BtnFecha2")

        # Opcional: Asegurarte de que el botón sea visible y habilitado antes de hacer clic
        if calendar_button_2.is_displayed() and calendar_button_2.is_enabled():
            # Hacer clic en el botón
            driver.execute_script("arguments[0].click();", calendar_button_2)
            print("Botón presionado correctamente.")
        else:
            print("El botón no está disponible para interactuar.")
        id_data_picker_2 = "datepicker"
        segundo_datapicker(driver,id_data_picker_2,fecha_fin,hora_fin,fin)
        time.sleep(15)
        boton_cfdi = driver.find_element(By.ID, "ctl00_MainContent_BtnBusqueda")
        driver.execute_script("arguments[0].click();", boton_cfdi)
        time.sleep(50)
        lista_dfs = Descarga_de_tabla(driver)
        df_final = pd.concat(lista_dfs, ignore_index=False)
        print(df_final)
        print("proceso terminado") 
        datetime_str = " "
        datetime_obj = datetime.strptime(datetime_str,  
                                 "%d%b%Y%H%M%S") 
        print(datetime_obj) 
        time = datetime_obj.time() 
        df_final.to_csv(direccion_pdfs + '/prueba_de_descarga' + time + '.csv')
        
        
    except Exception as e:
        print(f"fallo al entrar a extraer datos{e}")
    
    


if __name__ == '__main__':
    
  
    print("inicio")
    # Inicializa el navegador
    
    cert = "/home/kevin/Documentos/carpeta_orsan_pruebas/ORSAN/FNE150420DQ2/Cert.cer"
    key = "/home/kevin/Documentos/carpeta_orsan_pruebas/ORSAN/FNE150420DQ2/Llave.key"
    pswd = "FNE150420DQ2"
    
    fecha_inicio = ["Diciembre", "2024", "10"]
    fecha_fin = ["Diciembre", "2024", "10"]
    hora_inicio = ["00", "00", "00"]
    hora_fin = ["23", "00", "00"]
    datos = []
    lista_dfs = []
        
    
    try:
        # Abre la página web
        #driver.get("")
        time.sleep(5)
        try:

                
                url_actual = "https://portalcfdi.facturaelectronica.sat.gob.mx/"
                print("urlactual", url_actual)
                time.sleep(10)
                limite_superior = hora_fin[0]
                limite_inferior = hora_inicio[0]
                try:
                    lock = threading.Lock()
                    diferencia = (int(limite_superior) + 1) - int(limite_inferior)
                    divisiones = dividir_en_pares(1, diferencia, 4)
                    print("diferencias",diferencia)
                    print(divisiones)

                    with ThreadPoolExecutor(max_workers=4) as executor:
                        executor.map(lambda args: interaccion_principal(*args, url_actual,cert, key, pswd, fecha_inicio, fecha_fin, hora_inicio, hora_fin), divisiones,)
                        
                    
                    time.sleep(15)
                except Exception as e:
                   print(f"fallo en el uso de hilos {e}")           
                    
        except Exception as e:
            print(f"fallo al llegar a tablas{e}") 
            
    except Exception as e:
            print(f"fallo al llegar a tablas{e}")     
              


