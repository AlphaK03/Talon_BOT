"""base_flow.py — Contrato abstracto que cada bot implementa.

Métodos que PlaywrightDUAProcessor usa directamente:
  - start_url      → URL donde empieza la navegación
  - extract_data   → parsea el HTML y retorna los datos

Métodos opcionales (no-op por defecto):
  - fill_form / wait_for_result → usados por el processor Selenium legacy;
    en Playwright el processor maneja la navegación internamente.

Para crear un nuevo bot: heredar de BaseFlow, implementar start_url y
extract_data. El resto del sistema funciona sin modificaciones.
"""
from abc import ABC, abstractmethod
import pandas as pd


class BaseFlow(ABC):

    @property
    @abstractmethod
    def start_url(self) -> str:
        """URL donde empieza cada intento de navegación."""

    def fill_form(self, driver, row, captcha_text: str, timeout: int) -> None:
        """Llena y envía el formulario. Solo usado por el processor Selenium."""

    def wait_for_result(self, driver, timeout: int) -> None:
        """Espera a que la página de resultado cargue. Solo usado por Selenium."""

    @abstractmethod
    def extract_data(self, page_source: str) -> pd.DataFrame:
        """
        Parsea `page_source` y retorna un DataFrame con una o más filas.
        La columna 'numero_del_dua' es obligatoria (clave de deduplicación).
        """
