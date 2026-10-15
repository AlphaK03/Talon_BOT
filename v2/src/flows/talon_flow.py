"""talon_flow.py — Flujo para consultar los TALONES (tributos) de una DUA en el portal TICA.

Implementa BaseFlow para la URL:
    https://ticaconsultas.hacienda.go.cr/Tica/hcimppon.aspx

Navegación (manejada por PlaywrightDUAProcessor):
  1. Formulario principal (Aduana / Año / Número / CAPTCHA) → click DETALLE
  2. Espera botón LIQDUA → click LIQDUA
  3. Espera tabla Sftributos1ContainerTbl + span_CODI_ADUAN_0001
  4. extract_data() parsea el resultado

Retorna múltiples filas por DUA (una por tributo):
  Codigo_Aduana, Año, Numero, Tributo, Descripcion_tributo, Valor_MN
"""
import logging

import pandas as pd
from bs4 import BeautifulSoup

from flows.base_flow import BaseFlow


class NoRowsException(Exception):
    """DUA válido pero sin filas en la tabla de talones/impuestos."""



class XTalonesFlow(BaseFlow):

    _START_URL = 'https://ticaconsultas.hacienda.go.cr/Tica/hcimppon.aspx'

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
