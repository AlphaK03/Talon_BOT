"""
driver_updater.py
─────────────────
Funciones para detectar la versión de Edge instalada y descargar/actualizar
msedgedriver.exe desde el almacenamiento oficial de Microsoft.

Usado por main.py para el botón "Actualizar WebDriver" de la GUI.
"""

import os
import re
import shutil
import subprocess
import tempfile
import urllib.error
import urllib.request
import winreg
import zipfile
from typing import Callable

CDN_URL   = "https://msedgewebdriverstorage.blob.core.windows.net/edgewebdriver/{version}/edgedriver_win64.zip"
DEST_DIR  = "C:/WebDriver"
DEST_FILE = "msedgedriver.exe"

_REGISTRY_LOCATIONS = [
    (winreg.HKEY_CURRENT_USER,
     r"Software\Microsoft\Edge\BLBeacon",
     "version"),
    (winreg.HKEY_LOCAL_MACHINE,
     r"SOFTWARE\WOW6432Node\Microsoft\EdgeUpdate\Clients\{56EB18F8-B008-4CBD-B6D2-8C97FE7E9062}",
     "pv"),
    (winreg.HKEY_LOCAL_MACHINE,
     r"SOFTWARE\Microsoft\EdgeUpdate\Clients\{56EB18F8-B008-4CBD-B6D2-8C97FE7E9062}",
     "pv"),
]


def get_edge_version() -> str:
    """
    Lee la versión de Edge instalada desde el registro de Windows.
    Retorna '134.0.3124.93' o lanza RuntimeError.
    """
    for hive, subkey, value_name in _REGISTRY_LOCATIONS:
        try:
            key = winreg.OpenKey(hive, subkey)
            version, _ = winreg.QueryValueEx(key, value_name)
            winreg.CloseKey(key)
            version = version.strip()
            if version and version != "0.0.0.0":
                return version
        except OSError:
            continue
    raise RuntimeError(
        "No se encontro la version de Microsoft Edge en el registro de Windows.\n"
        "Asegurese de tener Edge instalado."
    )


def get_current_driver_version(driver_path: str) -> str | None:
    """
    Retorna la version del msedgedriver.exe en `driver_path`, o None si no existe / falla.
    """
    if not os.path.exists(driver_path):
        return None
    try:
        result = subprocess.run(
            [driver_path, "--version"],
            capture_output=True, text=True, timeout=5,
        )
        match = re.search(r"(\d+\.\d+\.\d+\.\d+)", result.stdout)
        if match:
            return match.group(1)
    except Exception:
        pass
    return None


def download_driver(
    version: str,
    dest_dir: str,
    progress_callback: Callable[[float, int], None] | None = None,
) -> str:
    """
    Descarga edgedriver_win64.zip para `version` y extrae msedgedriver.exe en `dest_dir`.

    progress_callback(pct: float, kb: int) se llama durante la descarga con:
      - pct  : porcentaje completado (0.0 – 100.0)
      - kb   : kilobytes descargados hasta el momento

    Retorna la ruta completa del ejecutable.
    Lanza RuntimeError con mensaje legible en caso de error.
    """
    url      = CDN_URL.format(version=version)
    dest_exe = os.path.join(dest_dir, DEST_FILE)
    os.makedirs(dest_dir, exist_ok=True)

    tmp_zip = os.path.join(tempfile.gettempdir(), f"edgedriver_{version}.zip")

    def _reporthook(block_count, block_size, total_size):
        if progress_callback and total_size > 0:
            downloaded = block_count * block_size
            pct = min(downloaded / total_size * 100, 100.0)
            progress_callback(pct, downloaded // 1024)

    try:
        urllib.request.urlretrieve(url, tmp_zip, reporthook=_reporthook)
    except urllib.error.HTTPError as e:
        raise RuntimeError(
            f"Error HTTP {e.code} al descargar el driver.\n"
            f"URL: {url}\n"
            "Verifique que la version de Edge este publicada en el servidor de Microsoft."
        ) from e
    except urllib.error.URLError as e:
        raise RuntimeError(
            f"No se pudo conectar al servidor de descarga.\n"
            f"URL: {url}\n"
            f"Detalle: {e.reason}"
        ) from e
    except OSError as e:
        raise RuntimeError(
            f"Error al guardar el archivo temporal: {tmp_zip}\n"
            f"Detalle: {e}"
        ) from e

    # Extraer msedgedriver.exe del zip
    with zipfile.ZipFile(tmp_zip, "r") as zf:
        names    = zf.namelist()
        exe_name = next((n for n in names if n.lower().endswith("msedgedriver.exe")), None)
        if not exe_name:
            raise RuntimeError(
                f"msedgedriver.exe no encontrado dentro del zip.\n"
                f"Contenido: {names}"
            )
        tmp_dir   = tempfile.mkdtemp()
        zf.extract(exe_name, tmp_dir)
        extracted = os.path.join(tmp_dir, exe_name)
        try:
            if os.path.exists(dest_exe):
                os.remove(dest_exe)
        except PermissionError:
            shutil.rmtree(tmp_dir, ignore_errors=True)
            raise RuntimeError(
                "No se puede reemplazar el driver porque esta siendo usado por otro proceso.\n"
                "Detenga la app, cierre los navegadores Edge y vuelva a intentarlo.\n"
                f"Ruta bloqueada: {dest_exe}"
            )
        shutil.move(extracted, dest_exe)
        shutil.rmtree(tmp_dir, ignore_errors=True)

    try:
        os.remove(tmp_zip)
    except OSError:
        pass

    if not os.path.exists(dest_exe):
        raise RuntimeError(
            f"La extraccion parecio completarse pero msedgedriver.exe no se encontro en: {dest_exe}"
        )

    return dest_exe
