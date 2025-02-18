import threading
from selenium import webdriver
from selenium.webdriver.support.ui import Select
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait

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




import time
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import TimeoutException  # Importa TimeoutException
from selenium.webdriver.support.select import Select
from selenium.common.exceptions import StaleElementReferenceException
from selenium.common.exceptions import WebDriverException
from psycopg2 import sql
from concurrent.futures import ThreadPoolExecutor





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

def interact_with_page(action, direccion_actual):
    options = webdriver.ChromeOptions()
    driver = webdriver.Chrome(options=options)  # Instancia separada para cada hilo
    driver.get(direccion_actual)
    
    print(f"{threading.current_thread().name} ejecutando acción: {action}")
    time.sleep(20)  # Simula una interacción
    
    try:
        # Esperar hasta que el elemento sea clickeable
        elemento = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, action))
        )
        elemento.click()
        print(f"{threading.current_thread().name} - Clic exitoso en el elemento")
    except Exception as e:
        print(f"{threading.current_thread().name} - Error al hacer clic: {str(e)}")
    print(60)
    driver.quit()  # Cerrar WebDriver después de la ejecución


if __name__ == "__main__":
    options = webdriver.ChromeOptions()  # Configura las opciones según sea necesario
    driver = webdriver.Chrome(options=options)
    driver.get("https://www.scrapethissite.com/pages/")
    print("Página cargada en el navegador.")
    time.sleep(15)
    xpath = '//*[@id="pages"]/section/div/div/div/div[2]/h3/a'

    try:
            # Esperar hasta que el elemento sea clickeable (máximo 10 segundos)
            elemento = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, xpath))
            )
            # Hacer clic en el elemento
            driver.execute_script("arguments[0].click();", elemento)

            print("Clic exitoso en el elemento")
            
    except Exception as e:
            print(f"Error al hacer clic en el elemento: {str(e)}")


    time.sleep(15)
    url_actual = driver.current_url
    print("urlactual", url_actual)
    time.sleep(10)
    lock = threading.Lock()
   

    with ThreadPoolExecutor(max_workers=len(actions)) as executor:
         executor.map(interact_with_page, actions, [url_actual] * len(actions))  # Pasamos la misma variable a cada ejecución
    time.sleep(60)

    print("Scraping completado.")

