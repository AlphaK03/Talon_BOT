"""main.py — Punto de entrada del bot XMovimientos v2.

Uso:
    python v2/src/main.py
"""
import os
import sys

# Agregar src/ al path para que los imports de subpaquetes funcionen
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from ui.app import App


def main():
    App().mainloop()


if __name__ == '__main__':
    main()
