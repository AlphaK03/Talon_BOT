"""browser.py — Fábrica del navegador Edge."""
import atexit
import os
import shutil
import sys
import tempfile
import threading

import urllib3
from selenium import webdriver
from selenium.webdriver.edge.service import Service as EdgeService
from selenium.webdriver.edge.options import Options as EdgeOptions

# Aumenta el pool de conexiones de urllib3 (Selenium lo usa internamente).
# Por defecto maxsize=1, lo que fuerza reconexiones TCP constantes a localhost.
_defaults = urllib3.connectionpool.HTTPConnectionPool.__init__.__defaults__
urllib3.connectionpool.HTTPConnectionPool.__init__.__defaults__ = tuple(
    10 if isinstance(d, int) and d == 1 else d for d in _defaults
)

_BLOCKED_EXTENSIONS = (
    '.png', '.jpg', '.jpeg', '.gif', '.css',
    '.woff', '.woff2', '.ttf', '.otf', '.svg',
)

# Directorio base del bot: junto al .exe en producción, raíz del proyecto en desarrollo.
# Los perfiles temporales de Edge se guardan aquí en lugar de AppData/Local/Temp.
if getattr(sys, 'frozen', False):
    _BASE_DIR = os.path.dirname(sys.executable)
else:
    _BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
_BOT_TEMP_BASE = os.path.join(_BASE_DIR, 'temp', 'edge')

# Registro de sesiones activas para que el hilo de limpieza no las toque.
_active_sessions: set[str] = set()
_active_sessions_lock = threading.Lock()


def _cleanup_orphans(base: str, interval: int = 1800) -> None:
    """
    Hilo daemon: cada `interval` segundos elimina las subcarpetas de `base`
    que no correspondan a una sesión activa (huérfanas de runs crasheados).
    """
    while True:
        threading.Event().wait(interval)
        if not os.path.isdir(base):
            continue
        with _active_sessions_lock:
            active = set(_active_sessions)
        for entry in os.listdir(base):
            path = os.path.join(base, entry)
            if os.path.isdir(path) and path not in active:
                shutil.rmtree(path, ignore_errors=True)


# Inicia el hilo de limpieza una sola vez al importar el módulo.
threading.Thread(
    target=_cleanup_orphans,
    args=(_BOT_TEMP_BASE,),
    daemon=True,
    name='edge-temp-cleanup',
).start()


def create_edge_driver(driver_path: str, headless: bool = False) -> webdriver.Edge:
    """
    Crea y retorna una instancia de Edge WebDriver optimizada para scraping:
    - Perfil temporal redirigido a temp/edge/ (no ensucía AppData/Local/Temp)
    - La carpeta de sesión se elimina en cuanto el driver hace quit()
    - Un hilo daemon limpia cada 30 min carpetas huérfanas de crashes anteriores
    - Sin extensiones ni notificaciones
    - Interceptor que bloquea recursos estáticos (imágenes, fuentes, CSS)
    - headless=True oculta la ventana del navegador
    """
    os.makedirs(_BOT_TEMP_BASE, exist_ok=True)
    session_dir = tempfile.mkdtemp(dir=_BOT_TEMP_BASE)

    with _active_sessions_lock:
        _active_sessions.add(session_dir)
    atexit.register(shutil.rmtree, session_dir, True)

    options = EdgeOptions()
    options.add_argument(f'--user-data-dir={session_dir}')
    if headless:
        options.add_argument('--headless')
        options.add_argument('--disable-gpu')
    for arg in (
        '--disable-extensions',
        '--disable-popup-blocking',
        '--disable-notifications',
        '--mute-audio',
        '--no-first-run',
        '--disable-background-timer-throttling',
        '--disable-renderer-backgrounding',
        '--disable-translate',
        '--disable-features=NetworkService',
        '--disable-blink-features=AutomationControlled',
    ):
        options.add_argument(arg)

    service = EdgeService(executable_path=driver_path)
    driver = webdriver.Edge(service=service, options=options)

    # Intercepta quit() para limpiar la carpeta de sesión de inmediato.
    _original_quit = driver.quit

    def _quit_and_cleanup():
        _original_quit()
        with _active_sessions_lock:
            _active_sessions.discard(session_dir)
        shutil.rmtree(session_dir, ignore_errors=True)

    driver.quit = _quit_and_cleanup

    def _interceptor(request):
        if any(request.path.endswith(ext) for ext in _BLOCKED_EXTENSIONS):
            request.abort()

    driver.request_interceptor = _interceptor
    return driver
