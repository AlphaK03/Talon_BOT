# processors.py

import os
import time
import gc
import pandas as pd
import logging
from threading import Lock
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.edge.service import Service
from selenium.webdriver.edge.options import Options as EdgeOptions
from selenium import webdriver
from selenium.webdriver.firefox.service import Service as FirefoxService
from selenium.webdriver.firefox.options import Options as FirefoxOptions
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
from selenium.common.exceptions import (
    NoSuchElementException, TimeoutException, StaleElementReferenceException
)

# Definir excepción personalizada para el caso de no encontrar filas
class NoRowsException(Exception):
    pass

class DUAProcessor:
    def __init__(self, config, names_df, lock, stop_event):
        self.config = config
        self.names_df = names_df
        self.lock = lock
        self.stop_event = stop_event
        self.driver = self.get_browser_instance()
        self.max_index_processed = -1

    def get_browser_instance(self):
        options = FirefoxOptions()

        # Ejecutar en modo sin cabeza
        options.add_argument('--headless')

        options.binary_location = r'C:\Program Files\Mozilla Firefox\firefox.exe'  # Ruta al binario de Firefox

        # Deshabilitar características innecesarias
        options.add_argument('--disable-extensions')
        options.add_argument('--disable-infobars')
        options.add_argument('--disable-popup-blocking')
        options.add_argument('--disable-notifications')
        options.add_argument('--mute-audio')
        options.add_argument('--no-first-run')
        options.add_argument('--disable-background-timer-throttling')
        options.add_argument('--disable-renderer-backgrounding')
        options.add_argument('--disable-translate')
        options.add_argument('--disable-features=NetworkService')

        # Configurar preferencias para deshabilitar imágenes pero habilitar JavaScript
        prefs = {
            "profile.managed_default_content_settings.images": 2,  # 2: Bloquea las imágenes
            "profile.managed_default_content_settings.javascript": 1  # 1: Habilita JavaScript
        }

        # Crear instancia del navegador sin usar 'execute_cdp_cmd'
        service = FirefoxService(executable_path=self.config['firefox_driver_path'])
        driver = webdriver.Firefox(service=service, options=options)

        driver.get('https://ticaconsultas.hacienda.go.cr/Tica/hcimppon.aspx')
        return driver

    def process_dua(self, index, row, progress_callback):
        try:
            if self.stop_event.is_set():
                return
            dua_number = row['NroDUA']
            logging.info(f"Procesando DUA {dua_number}...")

            max_retries = self.config['max_retries']
            retries_main = 0
            retries_impuestos = 0
            retries_data = 0

            page_stage = "main"

            while not self.stop_event.is_set():
                try:
                    if page_stage == "main":
                        if not self.process_main_page(row):
                            if retries_main < max_retries:
                                logging.warning(f"Reintentando la página principal para el DUA {dua_number}...")
                                self.driver.get('https://ticaconsultas.hacienda.go.cr/Tica/hcimppon.aspx')
                                retries_main += 1
                                continue
                            else:
                                logging.error(f"Error permanente en la página principal para el DUA {dua_number}.")
                                self.log_failed_dua(dua_number)
                                break
                        else:
                            page_stage = "impuestos"
                            retries_main = 0

                    if self.stop_event.is_set():
                        return

                    if page_stage == "impuestos":
                        if not self.process_impuestos_page():
                            if retries_impuestos < max_retries:
                                logging.warning(f"Reintentando la página 'Impuestos' para el DUA {dua_number}...")
                                self.driver.refresh()
                                retries_impuestos += 1
                                continue
                            else:
                                logging.error(f"Error permanente en la página 'Impuestos' para el DUA {dua_number}.")
                                self.log_failed_dua(dua_number)
                                break
                        else:
                            page_stage = "data"
                            retries_impuestos = 0

                    if self.stop_event.is_set():
                        return

                    if page_stage == "data":
                        if not self.process_data_extraction_page(dua_number):
                            if retries_data < max_retries:
                                logging.warning(f"Reintentando la extracción de datos para el DUA {dua_number}...")
                                self.driver.refresh()
                                retries_data += 1
                                continue
                            else:
                                logging.error(f"Error permanente en la extracción de datos para el DUA {dua_number}.")
                                self.log_failed_dua(dua_number)
                                break
                        else:
                            retries_data = 0
                            with self.lock:
                                if index > self.max_index_processed:
                                    self.max_index_processed = index
                                    progress_callback(self.max_index_processed)
                            break
                except NoRowsException as e:
                    # Manejar el caso donde no se encontraron filas
                    logging.info(f"DUA {dua_number} sin filas en la tabla de datos.")
                    self.log_dua_no_rows(dua_number)
                    with self.lock:
                        if index > self.max_index_processed:
                            self.max_index_processed = index
                            progress_callback(self.max_index_processed)
                    break  # No reintentar el DUA
                except Exception as e:
                    logging.exception(f"Error general al procesar DUA {dua_number}: {e}")
                    self.driver.refresh()

                if self.stop_event.is_set():
                    return

            self.driver.get('https://ticaconsultas.hacienda.go.cr/Tica/hcimppon.aspx')

        except Exception as e:
            logging.exception(f"Error general al procesar DUA {dua_number}: {e}")
            self.log_failed_dua(dua_number)

    def process_main_page(self, row):
        if self.stop_event.is_set():
            return False
        try:
            max_retries = 3
            retries = 0
            while retries < max_retries:
                try:
                    # Mover la llamada a get dentro del bucle
                    self.driver.get('https://ticaconsultas.hacienda.go.cr/Tica/hcimppon.aspx')

                    # Esperar a que el captcha esté presente
                    captcha_image_element = WebDriverWait(self.driver, self.config['timeout']).until(
                        EC.presence_of_element_located((By.XPATH, "//div[@id='captchaImage']/img"))
                    )
                    captcha_url = captcha_image_element.get_attribute('src')
                    captcha_word = self.names_df.loc[self.names_df['Path'] == captcha_url, 'Word'].values
                    captcha_text = captcha_word[0] if len(captcha_word) > 0 else ''

                    # Ingresar los datos
                    self.driver.find_element(By.ID, 'vVCODI_ADUA').clear()
                    self.driver.find_element(By.ID, 'vVCODI_ADUA').send_keys(row['Aduana'])
                    self.driver.find_element(By.ID, 'vVANO_PRE').clear()
                    self.driver.find_element(By.ID, 'vVANO_PRE').send_keys(row['Año'])
                    self.driver.find_element(By.ID, 'vVNUME_CORR').clear()
                    self.driver.find_element(By.ID, 'vVNUME_CORR').send_keys(row['Número'])
                    self.driver.find_element(By.ID, '_cfield').clear()
                    self.driver.find_element(By.ID, '_cfield').send_keys(captcha_text)
                    detalle_button = WebDriverWait(self.driver, self.config['timeout']).until(
                        EC.element_to_be_clickable((By.NAME, 'DETALLE'))
                    )
                    self.driver.execute_script("arguments[0].scrollIntoView(true);", detalle_button)
                    detalle_button.click()

                    # Esperar a que la página siguiente cargue
                    WebDriverWait(self.driver, self.config['timeout']).until(
                        EC.presence_of_element_located((By.NAME, 'LIQDUA'))
                    )
                    return True
                except (TimeoutException, NoSuchElementException) as e:
                    retries += 1
                    logging.warning(
                        f"Error al procesar la página principal: {e}. Reintentando ({retries}/{max_retries})..."
                    )
                    continue
                except Exception as e:
                    self.driver.save_screenshot(f'error_{row["NroDUA"]}.png')
                    logging.error(f"Error en process_main_page: {e}")
                    return False
            logging.error("No se pudo procesar la página principal después de varios intentos.")
            return False
        except Exception as e:
            logging.error(f"Error en la página principal: {e}")
            return False

    def process_impuestos_page(self):
        if self.stop_event.is_set():
            return False
        try:
            retry_count = 0
            max_retries = self.config['max_retries']

            while retry_count < max_retries and not self.stop_event.is_set():
                try:
                    # Esperar a que el botón de Impuestos esté disponible y sea clicable
                    impuestos_button = WebDriverWait(self.driver, self.config['timeout']).until(
                        EC.element_to_be_clickable((By.NAME, 'LIQDUA'))
                    )
                    # Desplazar la vista hasta el botón de Impuestos y hacer clic en él
                    self.driver.execute_script("arguments[0].scrollIntoView(true);", impuestos_button)
                    impuestos_button.click()

                    # Esperar a que la tabla de impuestos esté disponible
                    WebDriverWait(self.driver, self.config['timeout']).until(
                        EC.presence_of_element_located((By.ID, 'Sftributos1ContainerTbl'))
                    )
                    return True

                except StaleElementReferenceException:
                    retry_count += 1
                    logging.warning(f"StaleElementReferenceException: Reintentando ({retry_count}/{max_retries})...")

                except Exception as e:
                    retry_count += 1
                    logging.warning(f"Error al hacer clic en 'Impuestos': {e}. Reintentando ({retry_count}/{max_retries})...")

            logging.error(f"Error permanente en la página 'Impuestos' después de {max_retries} intentos.")
            return False

        except Exception as e:
            logging.exception(f"Error en la página 'Impuestos': {e}")
            return False

    def process_data_extraction_page(self, dua_number):
        if self.stop_event.is_set():
            return False
        try:
            # Esperar a que la tabla 'Sftributos1ContainerTbl' esté presente
            table_element = WebDriverWait(self.driver, self.config['timeout']).until(
                EC.presence_of_element_located((By.ID, 'Sftributos1ContainerTbl'))
            )
            new_data = self.extract_important_data(dua_number)
            if new_data is None:
                logging.error(f"No se pudo extraer información de la página del DUA {dua_number}")
                return False
            del new_data
            gc.collect()
            return True

        except NoRowsException as e:
            # Manejar el caso donde no se encontraron filas
            logging.info(f"DUA {dua_number} sin filas en la tabla de datos.")
            self.log_dua_no_rows(dua_number)
            return True  # Indicar que el procesamiento se completó (no reintentar)

        except Exception as e:
            logging.exception(f"Error en la página de extracción de datos: {e}")
            return False

    def extract_important_data(self, dua_number):
        if self.stop_event.is_set():
            return None
        try:
            current_page_source = self.driver.page_source
            soup = BeautifulSoup(current_page_source, 'html.parser')
            table = soup.find('table', {'id': 'Sftributos1ContainerTbl'})

            if table is None:
                logging.error(f"No se encontró la tabla 'Sftributos1ContainerTbl' en el DUA {dua_number}.")
                raise NoRowsException(f"Tabla no encontrada para el DUA {dua_number}")

            rows = table.find_all('tr')[1:]  # Omitir la primera fila que es el encabezado

            if not rows:
                logging.error(f"No se encontraron filas en la tabla para el DUA {dua_number}.")
                raise NoRowsException(f"No se encontraron filas en la tabla para el DUA {dua_number}")

            extracted_data = []

            for row in rows:
                if self.stop_event.is_set():
                    return None
                columns = row.find_all('td')
                if len(columns) < 6:  # Asegurarse de que haya al menos 6 columnas visibles
                    logging.warning(f"La fila no contiene suficientes columnas en el DUA {dua_number}.")
                    continue

                # Extraer datos de todas las columnas visibles y ocultas
                data = {
                    'Codigo_Aduana': columns[0].text.strip(),  # Código de Aduana
                    'Año': columns[1].text.strip(),            # Año
                    'Numero': columns[2].text.strip(),         # Número
                    'Tributo': columns[3].text.strip(),        # Tributo
                    'Descripcion_tributo': columns[4].text.strip(),  # Descripción del tributo
                    'Valor_MN': columns[5].text.strip(),       # Valor en Moneda Nacional (MN)
                    'numero_del_dua': dua_number               # Número del DUA
                }
                extracted_data.append(data)

            if not extracted_data:
                logging.warning(f"No se extrajo ningún dato del DUA {dua_number}.")
                raise NoRowsException(f"No se extrajo ningún dato del DUA {dua_number}")

            # Crear un DataFrame de los datos extraídos
            df = pd.DataFrame(extracted_data)

            # Guardar los datos en el archivo CSV
            output_csv_path = self.config['output_csv']
            if not os.path.exists(output_csv_path):
                df.to_csv(output_csv_path, mode='w', header=True, index=False)
            else:
                df.to_csv(output_csv_path, mode='a', header=False, index=False)

            logging.info(f"Datos del DUA {dua_number} guardados exitosamente")
            return df

        except NoRowsException:
            # Re-lanzar la excepción para manejarla en niveles superiores
            raise
        except Exception as e:
            logging.exception(f"Error al procesar la página del DUA {dua_number}: {e}")
            return None

    def log_failed_dua(self, dua_number):
        failed_duas_path = self.config['failed_duas']
        with open(failed_duas_path, 'a') as f:
            f.write(f"{dua_number}\n")

    def log_dua_no_rows(self, dua_number):
        # Obtener la ruta desde la configuración o usar una por defecto
        no_rows_duas_path = self.config.get('no_rows_duas', 'data/output/DUAs_sin_filas_agosto24.txt')
        with open(no_rows_duas_path, 'a') as f:
            f.write(f"{dua_number}\n")

    def close(self):
        self.driver.quit()
