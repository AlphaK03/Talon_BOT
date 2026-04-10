"""processor.py — Motor genérico de procesamiento de DUAs.

Gestiona el ciclo de vida completo de cada DUA:
  - Navegación al URL de inicio del flujo
  - Resolución del CAPTCHA (por lookup en names_df)
  - Llamada al flujo específico (fill_form → wait_for_result → extract_data)
  - Detección de duplicados (set global thread-safe)
  - Escritura al CSV de salida (protegida por lock)
  - Reintentos con recuperación de errores DNS
  - Reinicio del navegador cuando se agotan los reintentos

Para soportar un flujo distinto, pasar una instancia de una subclase de BaseFlow.
"""
import os
import logging
import time
from threading import Lock, Event
from typing import Callable

import pandas as pd
import requests
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    WebDriverException, NoSuchElementException,
    TimeoutException, StaleElementReferenceException,
)
from urllib3.exceptions import MaxRetryError, NewConnectionError as Urllib3NewConnectionError

from core.browser import create_edge_driver
from flows.base_flow import BaseFlow
from flows.talon_flow import NoRowsException

# Set global de DUAs ya procesados — compartido entre todos los workers del proceso
_PROCESSED_DUAS = set()
_PROCESSED_LOCK = Lock()


def load_processed_duas_from_log(log_path: str) -> None:
    """
    Pre-carga en el set global los DUAs ya procesados desde el archivo de log dedicado.
    Cada línea del archivo contiene un numero_del_dua.
    Llamar antes de iniciar el executor para evitar duplicados al reanudar.
    """
    if not os.path.exists(log_path):
        return
    try:
        with open(log_path, 'r', encoding='utf-8') as f:
            with _PROCESSED_LOCK:
                for line in f:
                    dua = line.strip()
                    if dua:
                        _PROCESSED_DUAS.add(dua)
        logging.info("Cargados %d DUAs previos desde %s.", len(_PROCESSED_DUAS), log_path)
    except Exception as e:
        logging.error("No se pudo cargar DUAs previos: %s", e)


def _wait_for_portal(url: str, interval: int = 10) -> None:
    """Bloquea hasta que el portal responda con HTTP 200."""
    while True:
        try:
            if requests.get(url, timeout=5).status_code == 200:
                logging.info("Portal disponible nuevamente.")
                return
        except requests.RequestException:
            pass
        logging.warning("Portal no disponible, reintentando en %ds...", interval)
        time.sleep(interval)


class DUAProcessor:
    """
    Procesa DUAs usando un flujo intercambiable (BaseFlow).
    Una instancia por hilo worker — cada instancia tiene su propio WebDriver.
    """

    def __init__(
        self,
        machine_config: dict,
        output_paths: dict,
        flow: BaseFlow,
        names_df: pd.DataFrame,
        lock: Lock,
        stop_event: Event,
    ):
        self.machine = machine_config
        self.output_paths = output_paths
        self.flow = flow
        self.names_df = names_df
        self.lock = lock
        self.stop_event = stop_event
        self._max_index = -1
        self.driver = create_edge_driver(
            machine_config['edge_driver_path'],
            headless=machine_config.get('headless', False),
        )
        os.makedirs(machine_config.get('screenshot_path', 'screenshots'), exist_ok=True)

    # ── API pública ──────────────────────────────────────────────────────────

    def process_dua(
        self,
        index: int,
        row,
        progress_callback: Callable[[int, str], None],
    ) -> None:
        """
        Procesa un DUA con reintentos.
        Llama progress_callback(index, status) donde status es:
          'success'  → DUA procesado y guardado correctamente
          'failed'   → reintentos agotados
          'skipped'  → DUA ya estaba en el CSV de salida
        """
        if self.stop_event.is_set():
            return

        dua_number = row['NroDUA']
        max_retries = self.machine['max_retries']
        timeout = self.machine['timeout']
        _last_no_rows = False  # distingue "sin filas" de error técnico real

        for attempt in range(1, max_retries + 1):
            if self.stop_event.is_set():
                return
            try:
                self._navigate_to_start()
                self.flow.on_before_form(self.driver, attempt, timeout)
                captcha_text = self._resolve_captcha(timeout)

                self.flow.fill_form(self.driver, row, captcha_text, timeout)
                self.flow.wait_for_result(self.driver, timeout)

                data = self.flow.extract_data(self.driver.page_source)
                self._validate(data, dua_number)

                extracted_dua = data.iloc[0]['numero_del_dua'].strip()

                with _PROCESSED_LOCK:
                    if extracted_dua in _PROCESSED_DUAS:
                        logging.warning("DUA %s ya procesado. Omitido.", extracted_dua)
                        progress_callback(index, 'skipped')
                        return
                    _PROCESSED_DUAS.add(extracted_dua)

                with self.lock:
                    self._write_csv(data)
                    self._log_processed(extracted_dua)
                    if index > self._max_index:
                        self._max_index = index

                logging.info("DUA %s guardado.", extracted_dua, extra={'tag': 'SUCCESS'})
                progress_callback(index, 'success')
                return  # éxito — salir del loop de reintentos


            except NoRowsException:

                _last_no_rows = True

                logging.warning("[%s] Sin filas en talones/impuestos (intento %d/%d).",

                                dua_number, attempt, max_retries)

            except (MaxRetryError, Urllib3NewConnectionError) as e:
                # El proceso del browser se cayó — la sesión WebDriver ya no existe.
                # Reiniciar inmediatamente para que los siguientes intentos tengan browser vivo.
                _last_no_rows = False
                logging.warning(
                    "[%s] Sesión WebDriver perdida (intento %d/%d). Reiniciando navegador...",
                    dua_number, attempt, max_retries,
                )
                self._restart_browser()

            except (StaleElementReferenceException, NoSuchElementException,
                    TimeoutException, WebDriverException, ValueError) as e:
                _last_no_rows = False
                logging.warning("[%s] Intento %d/%d: %s: %s",
                                dua_number, attempt, max_retries, type(e).__name__, e)
            except Exception as e:
                _last_no_rows = False
                logging.exception("[%s] Error inesperado intento %d/%d: %s",
                                  dua_number, attempt, max_retries, e)

        # Reintentos agotados
        if _last_no_rows:
            logging.warning("[%s] Sin filas en talones/impuestos tras %d intentos. Registrado.",
                            dua_number, max_retries)
            self._log_no_rows(dua_number)
        else:
            logging.error("[%s] Reintentos agotados. Reiniciando navegador.", dua_number)
            self._restart_browser()
            self._log_failed(dua_number)
        progress_callback(index, 'failed')

    def close(self) -> None:
        try:
            self.driver.quit()
        except Exception:
            pass

    # ── Internals ────────────────────────────────────────────────────────────

    def _navigate_to_start(self) -> None:
        try:
            self.driver.get(self.flow.start_url)
        except WebDriverException as e:
            if 'ERR_NAME_NOT_RESOLVED' in str(e):
                logging.warning("Error DNS. Esperando disponibilidad del portal...")
                _wait_for_portal(self.flow.start_url)
                self.driver.get(self.flow.start_url)
            else:
                raise

    def _resolve_captcha(self, timeout: int) -> str:
        el = WebDriverWait(self.driver, timeout).until(
            EC.presence_of_element_located(
                (By.XPATH, "//div[@id='captchaImage']/img")
            )
        )
        url = el.get_attribute('src')
        matches = self.names_df.loc[self.names_df['Path'] == url, 'Word'].values
        return str(matches[0]) if len(matches) > 0 else ''

    def _validate(self, data: pd.DataFrame, dua_number: str) -> None:
        if (data is None or data.empty
                or 'numero_del_dua' not in data.columns
                or data['numero_del_dua'].iloc[0].strip() == ''):
            raise ValueError(f"Datos vacíos para DUA {dua_number}")

    def _write_csv(self, data: pd.DataFrame) -> None:
        path = self.output_paths['output_csv']
        header = not os.path.exists(path)
        data.to_csv(path, mode='a', header=header, index=False, encoding='utf-8-sig')

    def _restart_browser(self) -> None:
        try:
            self.driver.quit()
        except Exception:
            pass
        self.driver = create_edge_driver(
            self.machine['edge_driver_path'],
            headless=self.machine.get('headless', False),
        )

    def _log_processed(self, dua_number: str) -> None:
        with open(self.output_paths['processed_log'], 'a', encoding='utf-8') as f:
            f.write(f"{dua_number}\n")

    def _log_no_rows(self, dua_number: str) -> None:
        with open(self.output_paths['no_rows_duas'], 'a', encoding='utf-8') as f:
            f.write(f"{dua_number}\n")

    def _log_failed(self, dua_number: str) -> None:
        with open(self.output_paths['failed_duas'], 'a', encoding='utf-8') as f:
            f.write(f"{dua_number}\n")
