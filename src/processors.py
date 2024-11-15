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

# Definir excepción personalizada para el caso de no encontrar filas
class NoRowsException(Exception):
    pass


def set_zoom_level(driver, zoom_percentage):
    """Ajustar el nivel de zoom de la página."""
    zoom_script = f"document.body.style.zoom='{zoom_percentage}%'"
    driver.execute_script(zoom_script)


class DUAProcessor:
    def __init__(self, config, names_df, lock, stop_event):
        self.config = config
        self.names_df = names_df
        self.lock = lock
        self.stop_event = stop_event
        self.driver = self.get_browser_instance()
        self.max_index_processed = -1

        # Crear la carpeta de capturas de pantalla si no existe
        self.screenshot_path = self.config.get('screenshot_path', 'screenshots')
        if not os.path.exists(self.screenshot_path):
            os.makedirs(self.screenshot_path)
#funcional hasta aqui
    def get_browser_instance(self):
        options = EdgeOptions()

        # Ejecutar en modo con cabeza (interfaz gráfica)
        # Elimina o comenta la siguiente línea si no deseas el modo headless
        options.add_argument('--headless')

        # Deshabilitar características innecesarias
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

        # Crear instancia del navegador Edge con Selenium Wire
        service = EdgeService(executable_path=self.config['edge_driver_path'])
        driver = webdriver.Edge(service=service, options=options)

        # Definir tipos de recursos a bloquear (bloqueamos imágenes, CSS, fuentes, pero no JavaScript)
        blocked_extensions = ['.png', '.jpg', '.jpeg', '.gif', '.css', '.woff', '.woff2', '.ttf', '.otf', '.svg']

        # Función para interceptar y bloquear solicitudes
        def interceptor(request):
            if any(request.path.endswith(ext) for ext in blocked_extensions):
                request.abort()

        # Asignar el interceptor
        driver.request_interceptor = interceptor

        # Navegar a la página inicial
        driver.get('https://ticaconsultas.hacienda.go.cr/Tica/hcimppon.aspx')
        return driver

    def process_dua(self, index, row, progress_callback):
        try:
            if self.stop_event.is_set():
                return
            dua_number = row['NroDUA']
            self.set_current_dua(dua_number, row)
            # logging.info(f"Procesando DUA {dua_number}...")

            max_retries = self.config['max_retries']
            retries_main = 0
            retries_impuestos = 0
            retries_data = 0

            page_stage = "main"

            while not self.stop_event.is_set():
                try:
                    if page_stage == "main":
                        if not self.process_main_page(row):
                            break  # Salir si falla el procesamiento
                        else:
                            page_stage = "impuestos"
                            retries_main = 0

                    if self.stop_event.is_set():
                        return

                    if page_stage == "impuestos":
                        if not self.process_impuestos_page():
                            break
                        else:
                            page_stage = "data"
                            retries_impuestos = 0

                    if self.stop_event.is_set():
                        return

                    if page_stage == "data":
                        if not self.process_data_extraction_page(dua_number):
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
            # El DUA fallido ya ha sido registrado por los métodos internos

    def process_main_page(self, row):
        dua_number = row['NroDUA']
        if self.stop_event.is_set():
            return False
        try:
            max_retries = 3
            retries = 0
            while retries < max_retries and not self.stop_event.is_set():
                try:
                    # Navegar a la página principal
                    self.driver.get('https://ticaconsultas.hacienda.go.cr/Tica/hcimppon.aspx')

                    # Establecer el zoom de la página al 25% para que todo sea visible sin scroll
                    set_zoom_level(self.driver, 25)

                    # Esperar a que el captcha esté presente
                    captcha_image_element = WebDriverWait(self.driver, self.config['timeout']).until(
                        EC.presence_of_element_located((By.XPATH, "//div[@id='captchaImage']/img"))
                    )
                    captcha_url = captcha_image_element.get_attribute('src')
                    captcha_word = self.names_df.loc[self.names_df['Path'] == captcha_url, 'Word'].values
                    captcha_text = captcha_word[0] if len(captcha_word) > 0 else ''

                    # Ingresar los datos del DUA
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

                    # Intentar hacer clic en el botón 'DETALLE' sin necesidad de desplazamiento
                    detalle_button = WebDriverWait(self.driver, self.config['timeout']).until(
                        EC.element_to_be_clickable((By.NAME, 'DETALLE'))
                    )

                    try:
                        # Intentar hacer clic en el botón 'DETALLE'
                        detalle_button.click()
                    except WebDriverException:
                        # Si otro elemento está cubriendo el botón, intentamos usar JavaScript para forzar el clic
                        logging.warning(f"Elemento 'DETALLE' está bloqueado, intentando forzar el clic con JavaScript.")
                        self.driver.execute_script("arguments[0].click();", detalle_button)

                    # Esperar a que la siguiente página cargue
                    WebDriverWait(self.driver, self.config['timeout']).until(
                        EC.presence_of_element_located((By.NAME, 'LIQDUA'))
                    )
                    return True

                except (
                TimeoutException, NoSuchElementException, StaleElementReferenceException, WebDriverException) as e:
                    retries += 1
                    logging.warning(
                        f"Error al procesar la página principal para DUA {dua_number}: {e}. Reintentando ({retries}/{max_retries})..."
                    )
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
            logging.error(f"Error en la página principal para DUA {dua_number}: {e}")
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
                    # Establecer el zoom de la página al 25% para que todo sea visible sin scroll
                    set_zoom_level(self.driver, 25)

                    # Comprobar si estamos en la página principal (posiblemente debido a un captcha)
                    if self.is_captcha_present():
                        logging.warning(f"Se detectó captcha, reintentando process_main_page...")
                        self.process_main_page(self.current_row)
                        continue

                    # Esperar a que el botón de Impuestos esté disponible y sea clicable
                    impuestos_button = WebDriverWait(self.driver, self.config['timeout']).until(
                        EC.element_to_be_clickable((By.NAME, 'LIQDUA'))
                    )

                    # Desplazar la vista hasta el botón y asegurarse de que no esté cubierto
                    self.driver.execute_script("arguments[0].scrollIntoView(true);", impuestos_button)

                    # Intentar hacer clic en el botón 'Impuestos'
                    impuestos_button.click()

                    # Esperar a que la tabla de impuestos esté disponible
                    WebDriverWait(self.driver, self.config['timeout']).until(
                        EC.presence_of_element_located((By.ID, 'Sftributos1ContainerTbl'))
                    )
                    return True

                except StaleElementReferenceException:
                    retry_count += 1
                    logging.warning(
                        f"Elemento 'Impuestos' no disponible en el DOM, reintentando ({retry_count}/{max_retries})...")
                    continue  # Volver a intentar encontrar el elemento si está obsoleto

                except WebDriverException as e:
                    retry_count += 1
                    logging.warning(
                        f"Error al hacer clic en 'Impuestos': {e}. Reintentando ({retry_count}/{max_retries})...")

                    # Guardar captura de pantalla para ayudar en la depuración
                    screenshot_filename = f'error_impuestos_{self.current_dua_number}_{retry_count}.png'
                    screenshot_filepath = os.path.join(self.screenshot_path, screenshot_filename)
                    self.driver.save_screenshot(screenshot_filepath)

                    # Si estamos en la página principal, volver a intentar desde allí
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
            # Establecer el zoom de la página al 25% para que todo sea visible sin scroll
            set_zoom_level(self.driver, 25)
            # Esperar a que la tabla 'Sftributos1ContainerTbl' esté presente
            table_element = WebDriverWait(self.driver, self.config['timeout']).until(
                EC.presence_of_element_located((By.ID, 'Sftributos1ContainerTbl'))
            )
            new_data = self.extract_important_data(dua_number)
            if new_data is None:
                logging.error(f"No se pudo extraer información de la página del DUA {dua_number}")
                self.log_failed_dua(dua_number)
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
            logging.exception(f"Error en la página de extracción de datos para DUA {dua_number}: {e}")
            self.log_failed_dua(dua_number)
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
                self.log_failed_dua(dua_number)
                raise NoRowsException(f"Tabla no encontrada para el DUA {dua_number}")

            rows = table.find_all('tr')[1:]  # Omitir la primera fila que es el encabezado

            if not rows:
                logging.error(f"No se encontraron filas en la tabla para el DUA {dua_number}.")
                self.log_failed_dua(dua_number)
                raise NoRowsException(f"No se encontraron filas en la tabla para el DUA {dua_number}")

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
                    'numero_del_dua': dua_number
                }
                extracted_data.append(data)

            if not extracted_data:
                logging.warning(f"No se extrajo ningún dato del DUA {dua_number}.")
                self.log_failed_dua(dua_number)
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
            self.log_failed_dua(dua_number)
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
            # Esperar a que el 'velo' desaparezca (puedes ajustar el selector según sea necesario)
            WebDriverWait(driver, timeout).until(
                EC.invisibility_of_element_located((By.CSS_SELECTOR, "div[style*='block']"))
            )
        except Exception as e:
            print(f"Error al esperar la desaparición del 'velo': {e}")


