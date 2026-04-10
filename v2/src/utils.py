"""utils.py — Configuración, sesión y utilidades compartidas."""
import os
import json
import logging
import re
import sys

if getattr(sys, 'frozen', False):
    # Corriendo como .exe: base = carpeta donde está el ejecutable
    _BASE_DIR = os.path.dirname(sys.executable)
else:
    # Corriendo como script: base = raíz del proyecto (v2/)
    _BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

_CONFIG_DIR    = os.path.join(_BASE_DIR, 'config')
_MACHINE_PATH  = os.path.join(_CONFIG_DIR, 'machine.json')
_SESSION_PATH  = os.path.join(_CONFIG_DIR, 'session.json')

def get_names_path() -> str:
    """Ruta al archivo Names_TicaPortal.xlsx — embebido en el exe o en el proyecto."""
    if getattr(sys, 'frozen', False):
        return os.path.join(sys._MEIPASS, 'Names_TicaPortal.xlsx')
    return os.path.join(_BASE_DIR, 'Names_TicaPortal.xlsx')


_DEFAULT_MACHINE = {
    "base_path":        "",
    "output_base_path": "",
    "edge_driver_path": "",
    "max_retries":      3,
    "timeout":          10,
    "cant_hilos":       1,
    "screenshot_path":  "screenshots",
    "headless":         False,
}

_DEFAULT_SESSION = {
    "sheet_name":      "",
    "input_file_path": "",
    "progress":        {},
    "totals":          {},
}


def _ensure_config() -> None:
    """Crea la carpeta config/ y los JSON por defecto si no existen."""
    os.makedirs(_CONFIG_DIR, exist_ok=True)
    if not os.path.exists(_MACHINE_PATH):
        with open(_MACHINE_PATH, 'w', encoding='utf-8') as f:
            json.dump(_DEFAULT_MACHINE, f, indent=4, ensure_ascii=False)
    if not os.path.exists(_SESSION_PATH):
        with open(_SESSION_PATH, 'w', encoding='utf-8') as f:
            json.dump(_DEFAULT_SESSION, f, indent=4, ensure_ascii=False)


# ── Config de máquina (rutas, driver, parámetros estáticos) ──────────────────

def load_machine_config() -> dict:
    _ensure_config()
    with open(_MACHINE_PATH, 'r', encoding='utf-8') as f:
        return json.load(f)


def save_machine_config(config: dict) -> None:
    os.makedirs(_CONFIG_DIR, exist_ok=True)
    with open(_MACHINE_PATH, 'w', encoding='utf-8') as f:
        json.dump(config, f, indent=4, ensure_ascii=False)


# ── Sesión (hoja activa, ruta de entrada, progreso de reanudación) ───────────

def load_session() -> dict:
    _ensure_config()
    with open(_SESSION_PATH, 'r', encoding='utf-8') as f:
        return json.load(f)


def save_session(session: dict) -> None:
    os.makedirs(_CONFIG_DIR, exist_ok=True)
    with open(_SESSION_PATH, 'w', encoding='utf-8') as f:
        json.dump(session, f, indent=4, ensure_ascii=False)


# ── Rutas derivadas (calculadas en memoria, nunca persistidas) ───────────────

def normalize_key(name: str) -> str:
    """Convierte un nombre de hoja a clave válida para JSON (sin caracteres especiales, minúsculas)."""
    return re.sub(r'\W+', '_', name.strip().lower())


def compute_output_paths(output_base: str, sheet_name: str) -> dict:
    """
    Calcula y crea (si no existe) la carpeta de salida para una hoja.
    Retorna dict con: output_csv, failed_duas, no_rows_duas.
    """
    folder = os.path.join(output_base, sheet_name)
    os.makedirs(folder, exist_ok=True)
    return {
        'output_csv':    os.path.join(folder, f'MM20YY_Talon_DUAS_{sheet_name}.csv'),
        'failed_duas':   os.path.join(folder, f'Talon_DUAS_fallidas_{sheet_name}.txt'),
        'no_rows_duas':  os.path.join(folder, f'Talon_DUAS_sin_filas_{sheet_name}.txt'),
        'processed_log': os.path.join(folder, f'Talon_DUAS_procesados_log_{sheet_name}.txt'),
    }


def get_extracted_path(sheet_name: str) -> str:
    """Ruta local donde se guarda la copia de la hoja de entrada."""
    folder = os.path.join(_BASE_DIR, 'data', 'Datos_Cargados')
    os.makedirs(folder, exist_ok=True)
    return os.path.join(folder, f'{sheet_name}.xlsx')


# ── Logging ──────────────────────────────────────────────────────────────────

def setup_logging(log_file: str = None) -> None:
    if log_file is None:
        log_file = os.path.join(_BASE_DIR, 'logs', 'debug.log')
    log_dir = os.path.dirname(log_file)
    if log_dir:
        os.makedirs(log_dir, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler(),
        ],
    )
