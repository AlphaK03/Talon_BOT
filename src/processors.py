# processors.py

import os
import time
import gc
import pandas as pd
import logging
from threading import Lock
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.edge.service import Service as EdgeService
from selenium.webdriver.edge.options import Options as EdgeOptions
from selenium import webdriver
from selenium import webdriver
from selenium.webdriver.firefox.service import Service as FirefoxService
from selenium.webdriver.firefox.options import Options as FirefoxOptions
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
from selenium.common.exceptions import WebDriverException
from selenium.common.exceptions import (
    NoSuchElementException, TimeoutException, StaleElementReferenceException
)

class NoRowsException(Exception):
    pass


def set_zoom_level(driver, zoom_percentage):
    """Ajustar el nivel de zoom de la página."""
    zoom_script = f"document.body.style.zoom='{zoom_percentage}%'"
    driver.execute_script(zoom_script)


import requests

def esperar_disponibilidad_pagina(url, intervalo=10, max_intentos=0):
    intentos = 0
    while True:
        try:
            response = requests.get(url, timeout=5)
            if response.status_code == 200:
                logging.info(f"La página {url} está disponible nuevamente.")
                return
        except requests.RequestException:
            pass
        intentos += 1
        logging.warning(f"La página {url} aún no está disponible. Intento {intentos}. Reintentando en {intervalo} segundos...")
        time.sleep(intervalo)
        if max_intentos and intentos >= max_intentos:
            logging.error(f"No se pudo acceder a la página {url} después de {max_intentos} intentos.")
            break



class DUAProcessor:
    def __init__(self, config, names_df, lock, stop_event):
        self.config = config
        self.names_df = names_df
        self.lock = lock
        self.stop_event = stop_event
        self.driver = self.get_browser_instance()
        self.max_index_processed = -1

        self.screenshot_path = self.config.get('screenshot_path', 'screenshots')
        if not os.path.exists(self.screenshot_path):
            os.makedirs(self.screenshot_path)

    def get_browser_instance(self):
        options = EdgeOptions()

        options.add_argument('--headless')

        options.add_argument('--disable-extensions')
        options.add_argument('--disable-popup-blocking')
        options.add_argument('--disable-notifications')
        options.add_argument('--mute-audio')
        options.add_argument('--no-first-run')
        options.add_argument('--disable-background-timer-throttling')
        options.add_argument('--disable-renderer-backgrounding')
        options.add_argument('--disable-translate')
        options.add_argument('--disable-features=NetworkService')
        options.add_argument('--disable-blink-features=AutomationControlled')

        service = EdgeService(executable_path=self.config['edge_driver_path'])
        driver = webdriver.Edge(service=service, options=options)

        blocked_extensions = ['.png', '.jpg', '.jpeg', '.gif', '.css', '.woff', '.woff2', '.ttf', '.otf', '.svg']

        def interceptor(request):
            if any(request.path.endswith(ext) for ext in blocked_extensions):
                request.abort()

        driver.request_interceptor = interceptor

        #driver.get('https://portaltica.hacienda.go.cr/TicaExterno/hcimppon.aspx')
        return driver

    def process_dua(self, index, row, progress_callback):
        try:
            if self.stop_event.is_set():
                return
            dua_number = row['NroDUA']
            self.set_current_dua(dua_number, row)
            # logging.info(f"Procesando DUA {dua_number}...")

            page_stage = "main"

            while not self.stop_event.is_set():
                try:
                    if page_stage == "main":
                        if not self.process_main_page(row):
                            break
                        else:
                            page_stage = "impuestos"

                    if self.stop_event.is_set():
                        return

                    if page_stage == "impuestos":
                        if not self.process_impuestos_page():
                            break
                        else:
                            page_stage = "data"

                    if self.stop_event.is_set():
                        return

                    if page_stage == "data":
                        if not self.process_data_extraction_page(dua_number):
                            break
                        else:
                            with self.lock:
                                if index > self.max_index_processed:
                                    self.max_index_processed = index
                                    progress_callback(self.max_index_processed)
                            break
                except NoRowsException as e:
                    logging.info(f"DUA {dua_number} sin filas en la tabla de datos.")
                    self.log_dua_no_rows(dua_number)
                    with self.lock:
                        if index > self.max_index_processed:
                            self.max_index_processed = index
                            progress_callback(self.max_index_processed)
                    break
                except Exception as e:
                    logging.exception(f"Error general al procesar DUA {dua_number}: {e}")
                    self.driver.refresh()

                if self.stop_event.is_set():
                    return

            self.driver.get('https://portaltica.hacienda.go.cr/TicaExterno/hcimppon.aspx')

        except Exception as e:
            logging.exception(f"Error general al procesar DUA {dua_number}: {e}")

    def process_main_page(self, row):
        dua_number = row['NroDUA']
        if self.stop_event.is_set():
            return False
        try:
            max_retries = 3
            retries = 0
            while retries < max_retries and not self.stop_event.is_set():
                try:
                    self.driver.get('https://portaltica.hacienda.go.cr/TicaExterno/hcimppon.aspx')
                    set_zoom_level(self.driver, 25)

                    captcha_image_element = WebDriverWait(self.driver, self.config['timeout']).until(
                        EC.presence_of_element_located((By.XPATH, "//div[@id='captchaImage']/img"))
                    )
                    captcha_url = captcha_image_element.get_attribute('src')
                    captcha_word = self.names_df.loc[self.names_df['Path'] == captcha_url, 'Word'].values
                    captcha_text = captcha_word[0] if len(captcha_word) > 0 else ''

                    WebDriverWait(self.driver, self.config['timeout']).until(
                        EC.presence_of_element_located((By.ID, 'vVCODI_ADUA'))
                    )
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
                    try:
                        detalle_button.click()
                        time.sleep(0.7)
                    except WebDriverException:
                        logging.warning("Elemento 'DETALLE' está bloqueado, intentando forzar el clic con JavaScript.")
                        self.driver.execute_script("arguments[0].click();", detalle_button)

                    WebDriverWait(self.driver, self.config['timeout']).until(
                        EC.presence_of_element_located((By.NAME, 'LIQDUA'))
                    )
                    return True

                except WebDriverException as e:
                    if "ERR_NAME_NOT_RESOLVED" in str(e):
                        logging.warning("Error DNS: página no disponible. Esperando disponibilidad...")
                        esperar_disponibilidad_pagina("https://portaltica.hacienda.go.cr/TicaExterno/hcimppon.aspx",
                                                      intervalo=10)
                        continue
                    retries += 1
                    logging.warning(
                        f"WebDriverException para DUA {dua_number}: {e}. Reintentando ({retries}/{max_retries})...")
                    continue

                except (TimeoutException, NoSuchElementException, StaleElementReferenceException) as e:
                    retries += 1
                    logging.warning(
                        f"Error al procesar la página principal para DUA {dua_number}: {e}. Reintentando ({retries}/{max_retries})...")
                    continue

                except Exception as e:
                    self.driver.save_screenshot(f'error_{dua_number}.png')
                    logging.error(f"Error en process_main_page para DUA {dua_number}: {e}")
                    self.log_failed_dua(dua_number)
                    return False

            logging.error(f"No se pudo procesar la página principal para DUA {dua_number} después de varios intentos.")
            self.log_failed_dua(dua_number)
            return False

        except Exception as e:
            logging.error(f"Error inesperado en process_main_page para DUA {dua_number}: {e}")
            self.log_failed_dua(dua_number)
            return False

    def process_impuestos_page(self):
        if self.stop_event.is_set():
            return False
        try:
            retry_count = 0
            max_retries = self.config['max_retries']

            while retry_count < max_retries and not self.stop_event.is_set():
                try:
                    set_zoom_level(self.driver, 25)

                    if self.is_captcha_present():
                        logging.warning(f"Se detectó captcha, reintentando process_main_page...")
                        self.process_main_page(self.current_row)
                        continue

                    # Esperar a que el botón de Impuestos esté disponible
                    time.sleep(0.6)
                    impuestos_button = WebDriverWait(self.driver, self.config['timeout']).until(
                        EC.element_to_be_clickable((By.NAME, 'LIQDUA'))
                    )

                    self.driver.execute_script("arguments[0].scrollIntoView(true);", impuestos_button)

                    impuestos_button.click()

                    # Esperar a que la tabla esté disponible
                    WebDriverWait(self.driver, self.config['timeout']).until(
                        EC.presence_of_element_located((By.ID, 'Sftributos1ContainerTbl'))
                    )
                    return True

                except StaleElementReferenceException:
                    retry_count += 1
                    logging.warning(
                        f"Elemento 'Impuestos' no disponible en el DOM, reintentando ({retry_count}/{max_retries})...")
                    continue

                except WebDriverException as e:
                    retry_count += 1
                    logging.warning(
                        f"Error al hacer clic en 'Impuestos': {e}. Reintentando ({retry_count}/{max_retries})...")

                    screenshot_filename = f'error_impuestos_{self.current_dua_number}_{retry_count}.png'
                    screenshot_filepath = os.path.join(self.screenshot_path, screenshot_filename)
                    self.driver.save_screenshot(screenshot_filepath)

                    if self.is_captcha_present():
                        logging.warning(f"Se detectó captcha, reintentando process_main_page...")
                        self.process_main_page(self.current_row)
                        continue

            logging.error(f"Error permanente en la página 'Impuestos' después de {max_retries} intentos.")
            return False

        except Exception as e:
            logging.exception(f"Error en la página 'Impuestos': {e}")
            return False

    def process_data_extraction_page(self, dua_number):
        if self.stop_event.is_set():
            return False
        try:
            set_zoom_level(self.driver, 25)
            # Esperar a que la tabla 'Sftributos1ContainerTbl' esté presente
            WebDriverWait(self.driver, self.config['timeout']).until(
                EC.presence_of_element_located((By.ID, 'Sftributos1ContainerTbl'))
            )
            time.sleep(1)
            new_data = self.extract_important_data(dua_number)
            if new_data is None:
                logging.info(f"DUA {dua_number} no tiene datos en la página.")
                self.log_dua_no_rows(dua_number)
                return True
            del new_data
            gc.collect()
            return True

        except NoRowsException as e:
            logging.info(f"DUA {dua_number} sin filas en la tabla de datos.")
            self.log_dua_no_rows(dua_number)
            return True

        except Exception as e:
            logging.exception(f"Error en la página de extracción de datos para DUA {dua_number}: {e}")
            self.log_failed_dua(dua_number)
            return False

    def extract_important_data(self, dua_number):
        """
        Extrae los datos importantes de la página de detalle (después de hacer clic en DETALLE)
        y retorna un DataFrame con la información requerida. Se asegura de que el número del DUA
        (compuesto por los spans 'span_CODI_ADUAN_0001', 'span_ANO_PRESE_0001' y 'span_NUME_CORRE_0001')
        se extraiga directamente de la página.
        """
        if self.stop_event.is_set():
            return None
        try:
            WebDriverWait(self.driver, self.config['timeout']).until(
                EC.presence_of_element_located((By.ID, 'span_CODI_ADUAN_0001'))
            )
            WebDriverWait(self.driver, self.config['timeout']).until(
                EC.presence_of_element_located((By.ID, 'span_ANO_PRESE_0001'))
            )
            WebDriverWait(self.driver, self.config['timeout']).until(
                EC.presence_of_element_located((By.ID, 'span_NUME_CORRE_0001'))
            )

            current_page_source = self.driver.page_source
            soup = BeautifulSoup(current_page_source, 'html.parser')

            # Extraer el número del DUA desde la página
            codigo_elem = soup.find('span', id='span_CODI_ADUAN_0001')
            ano_elem = soup.find('span', id='span_ANO_PRESE_0001')
            numero_elem = soup.find('span', id='span_NUME_CORRE_0001')

            if codigo_elem is None or ano_elem is None or numero_elem is None:
                logging.error("No se pudo extraer el número del DUA de la página, a pesar de haber esperado.")
                raise NoRowsException("Número del DUA no encontrado en la página.")

            numero_del_dua = f"{codigo_elem.get_text(strip=True)}-{ano_elem.get_text(strip=True)}-{numero_elem.get_text(strip=True)}"
            logging.info(f"Número del DUA extraído: {numero_del_dua}")

            # Extraer la tabla de impuestos
            table = soup.find('table', {'id': 'Sftributos1ContainerTbl'})
            if table is None:
                logging.error(f"No se encontró la tabla 'Sftributos1ContainerTbl' en el DUA {dua_number}.")
                self.log_failed_dua(dua_number)
                raise NoRowsException(f"Tabla no encontrada para el DUA {dua_number}")

            rows = table.find_all('tr')[1:]  # Omitir el encabezado
            if not rows:
                logging.warning(f"No se encontraron filas en la tabla para el DUA {dua_number}.")
                raise NoRowsException(f"No se extrajo ningún dato del DUA {dua_number}")

            extracted_data = []
            for row in rows:
                if self.stop_event.is_set():
                    return None
                columns = row.find_all('td')
                if len(columns) < 6:
                    logging.warning(f"La fila no contiene suficientes columnas en el DUA {dua_number}.")
                    continue

                data = {
                    'Codigo_Aduana': columns[0].text.strip(),
                    'Año': columns[1].text.strip(),
                    'Numero': columns[2].text.strip(),
                    'Tributo': columns[3].text.strip(),
                    'Descripcion_tributo': columns[4].text.strip(),
                    'Valor_MN': columns[5].text.strip(),
                    'numero_del_dua': numero_del_dua
                }
                extracted_data.append(data)

            if not extracted_data:
                logging.warning(f"No se extrajo ningún dato del DUA {dua_number}.")
                self.log_dua_no_rows(dua_number)
                raise NoRowsException(f"No se extrajo ningún dato del DUA {dua_number}")

            df = pd.DataFrame(extracted_data)

            # Guardar los datos en el CSV usando la codificación utf-8-sig
            output_csv_path = self.config['output_csv']
            with self.lock:
                if not os.path.exists(output_csv_path):
                    df.to_csv(output_csv_path, mode='w', header=True, index=False, encoding='utf-8-sig')
                else:
                    # Leer el CSV existente con la codificación correcta y combinar
                    existing_df = pd.read_csv(output_csv_path, encoding='utf-8-sig')
                    combined_df = pd.concat([existing_df, df], ignore_index=True)
                    combined_df.to_csv(output_csv_path, mode='w', header=True, index=False, encoding='utf-8-sig')

            logging.info(f"Datos del DUA {dua_number} guardados exitosamente")
            return df

        except NoRowsException:
            raise
        except Exception as e:
            logging.exception(f"Error al procesar la página del DUA {dua_number}: {e}")
            self.log_failed_dua(dua_number)
            return None

    def log_failed_dua(self, dua_number):
        """Registrar los DUAs fallidos (errores generales) en el archivo correspondiente."""
        failed_duas_path = self.config['failed_duas']
        with open(failed_duas_path, 'a', encoding='utf-8-sig') as f:
            f.write(f"{dua_number}\n")

    def log_dua_no_rows(self, dua_number):
        """Registrar los DUAs sin filas (tabla vacía) en el archivo correspondiente."""
        no_rows_duas_path = self.config.get('no_rows_duas', 'data/output/DUAs_sin_filas_enero25.txt')
        with open(no_rows_duas_path, 'a', encoding='utf-8-sig') as f:
            f.write(f"{dua_number}\n")

    def close(self):
        self.driver.quit()

    def is_captcha_present(self):
        try:
            captcha_image_element = self.driver.find_element(By.XPATH, "//div[@id='captchaImage']/img")
            return True
        except NoSuchElementException:
            return False

    def set_current_dua(self, dua_number, row):
        self.current_dua_number = dua_number
        self.current_row = row


    def wait_for_page_to_load(driver, timeout=10):
        try:
            WebDriverWait(driver, timeout).until(
                EC.invisibility_of_element_located((By.CSS_SELECTOR, "div[style*='block']"))
            )
        except Exception as e:
            print(f"Error al esperar la desaparición del 'velo': {e}")


