"""base_flow.py — Contrato abstracto que cada bot implementa.

El motor genérico (core/processor.py) llama estos métodos en el siguiente orden
para cada DUA:

  1. start_url           → URL donde empieza la navegación
  2. on_before_form      → hook opcional antes de llenar el formulario
  3. fill_form           → llena y envía el formulario
  4. wait_for_result     → espera a que la página de resultado esté lista
  5. extract_data        → parsea el HTML y retorna los datos

Para crear un nuevo bot: heredar de BaseFlow e implementar los métodos abstractos.
El resto del sistema (reintentos, CAPTCHA, CSV, progreso) funciona sin modificaciones.
"""
from abc import ABC, abstractmethod
import pandas as pd


class BaseFlow(ABC):

    @property
    @abstractmethod
    def start_url(self) -> str:
        """URL donde empieza cada intento de navegación."""

    def on_before_form(self, driver, attempt: int, timeout: int) -> None:
        """
        Hook llamado antes de fill_form en cada intento.
        Útil para descartar modales o manejar estados inesperados.
        La implementación base no hace nada.
        """

    @abstractmethod
    def fill_form(self, driver, row, captcha_text: str, timeout: int) -> None:
        """
        Llena el formulario con los datos de `row` y el texto del CAPTCHA,
        luego hace submit. Lanza excepción si no puede completar la operación.

        Parameters
        ----------
        driver      : WebDriver activo
        row         : fila de pandas con los datos del DUA
        captcha_text: texto del CAPTCHA resuelto por lookup
        timeout     : segundos de espera para Selenium
        """

    @abstractmethod
    def wait_for_result(self, driver, timeout: int) -> None:
        """
        Espera a que la página de resultado con datos esté completamente cargada.
        Lanza TimeoutException u otra si no carga en el tiempo esperado.
        """

    @abstractmethod
    def extract_data(self, page_source: str) -> pd.DataFrame:
        """
        Parsea `page_source` y retorna un DataFrame con una o más filas.
        La columna 'numero_del_dua' es obligatoria (se usa como clave de duplicados).
        """
