"""talon_flow.py — Flujo para consultar los MOVIMIENTOS de una DUA en el portal TICA.

Implementa BaseFlow para la URL:
    https://portaltica.hacienda.go.cr/TicaExterno/hcimppon.aspx

Navegación:
  1. Formulario principal (Aduana / Año / Número / CAPTCHA) → click MANAERMAR
  2. Espera la tabla SubstkContainerTbl (resultados de movimientos)
  3. Extrae las filas: Deposito, Año, Movimiento, Tipo_de_Movimiento,
     Cantidad_de_Bultos_asociada — agrupa y suma por combinación única.

Diferencias respecto a DetallesFlow:
  - Botón: MANAERMAR (no DETALLE)
  - No hay página intermedia — la tabla aparece directamente
  - Retorna múltiples filas por DUA (una por movimiento)
"""
import logging
import time

import pandas as pd
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from flows.base_flow import BaseFlow


class NoRowsException(Exception):
    """DUA válido pero sin filas en la tabla de talones/impuestos."""



class XTalonesFlow(BaseFlow):

    _START_URL = 'https://portaltica.hacienda.go.cr/TicaExterno/hcimppon.aspx'

    @staticmethod
    def _clean(text: str) -> str:
        """Elimina caracteres que rompen el CSV: saltos de línea, tabulaciones, comas y punto y coma."""
        import re
        text = re.sub(r'[\n\r\t]+', ' ', text)
        text = text.replace(',', ' ')
        text = text.replace(';', ' ')
        text = re.sub(r' {2,}', ' ', text)
        return text.strip()

    @property
    def start_url(self) -> str:
        return self._START_URL

    def fill_form(self, driver, row, captcha_text: str, timeout: int) -> None:
        driver.execute_script("document.body.style.zoom='25%'")

        WebDriverWait(driver, timeout).until(
            EC.presence_of_element_located((By.ID, 'vVCODI_ADUA'))
        )

        for field_id, value in [
            ('vVCODI_ADUA', str(row['Aduana'])),
            ('vVANO_PRE', str(row['Año'])),
            ('vVNUME_CORR', str(row['Número'])),
            ('_cfield', captcha_text),
        ]:
            el = driver.find_element(By.ID, field_id)
            el.clear()
            el.send_keys(value)

        btn = WebDriverWait(driver, timeout).until(
            EC.element_to_be_clickable((By.NAME, 'DETALLE'))
        )
        try:
            btn.click()
        except Exception:
            driver.execute_script("arguments[0].click();", btn)
        time.sleep(0.7)

    def wait_for_result(self, driver, timeout: int) -> None:
        time.sleep(0.6)
        WebDriverWait(driver, timeout).until(
            EC.presence_of_element_located((By.NAME, 'LIQDUA'))
        )

        btn = WebDriverWait(driver, timeout).until(
            EC.element_to_be_clickable((By.NAME, 'LIQDUA'))
        )
        try:
            btn.click()
        except Exception:
            driver.execute_script("arguments[0].click();", btn)

        WebDriverWait(driver, timeout).until(
            EC.presence_of_element_located((By.ID, 'Sftributos1ContainerTbl'))
        )
        time.sleep(1)

        WebDriverWait(driver, timeout).until(
            EC.presence_of_element_located((By.ID, 'span_CODI_ADUAN_0001'))
        )
        WebDriverWait(driver, timeout).until(
            EC.presence_of_element_located((By.ID, 'span_ANO_PRESE_0001'))
        )
        WebDriverWait(driver, timeout).until(
            EC.presence_of_element_located((By.ID, 'span_NUME_CORRE_0001'))
        )

    def extract_data(self, page_source: str) -> pd.DataFrame:
        soup = BeautifulSoup(page_source, 'html.parser')

        codigo_el = soup.find('span', id='span_CODI_ADUAN_0001')
        ano_el = soup.find('span', id='span_ANO_PRESE_0001')
        numero_el = soup.find('span', id='span_NUME_CORRE_0001')

        if not all([codigo_el, ano_el, numero_el]):
            raise ValueError("No se pudo extraer el número del DUA de la página.")

        numero_del_dua = (
            f"{codigo_el.get_text(strip=True)}-"
            f"{ano_el.get_text(strip=True)}-"
            f"{numero_el.get_text(strip=True)}"
        )

        table = soup.find('table', {'id': 'Sftributos1ContainerTbl'})
        if table is None:
            raise ValueError(
                f"Tabla 'Sftributos1ContainerTbl' no encontrada para DUA {numero_del_dua}."
            )

        rows = table.find_all('tr')[1:]
        if not rows:
            raise NoRowsException(numero_del_dua)

        extracted = []
        for tr in rows:
            cols = tr.find_all('td')
            if len(cols) < 6:
                logging.warning(
                    "Fila con menos de 6 columnas omitida en DUA %s.",
                    numero_del_dua
                )
                continue

            extracted.append({
                'Codigo_Aduana':      self._clean(cols[0].get_text(strip=True)),
                'Año':                self._clean(cols[1].get_text(strip=True)),
                'Numero':             self._clean(cols[2].get_text(strip=True)),
                'Tributo':            self._clean(cols[3].get_text(strip=True)),
                'Descripcion_tributo': self._clean(cols[4].get_text(strip=True)),
                'Valor_MN':           self._clean(cols[5].get_text(strip=True)),
                'numero_del_dua':     numero_del_dua,
            })

        if not extracted:
            raise ValueError(
                f"DUA {numero_del_dua}: no se extrajeron filas válidas de la tabla."
            )

        return pd.DataFrame(extracted)
