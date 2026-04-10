"""app.py — Ventana principal del bot (CHC CONSULTORES — XMovimientos BOT v2).

Estructura de pestañas:
  - Procesamiento : selección de archivo/hoja, progreso, log, Iniciar/Detener
  - Configuración : todos los parámetros editables (hilos, timeout, reintentos,
                    rutas, driver) — sin necesidad de tocar JSON
"""
import os
import sys
import threading
import logging
from concurrent.futures import ThreadPoolExecutor
from tkinter import ttk, filedialog, messagebox
from tkinter.scrolledtext import ScrolledText
import tkinter as tk
import pandas as pd

_SRC_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _SRC_DIR not in sys.path:
    sys.path.insert(0, _SRC_DIR)

from utils import (
    load_machine_config, save_machine_config,
    load_session, save_session,
    compute_output_paths, get_extracted_path,
    normalize_key, setup_logging, get_names_path,
)
from core.processor import DUAProcessor, load_processed_duas_from_log
from flows.talon_flow import XTalonesFlow
from driver_updater import (
    get_edge_version, get_current_driver_version,
    download_driver, DEST_DIR, DEST_FILE,
)


# ── Logging handler ───────────────────────────────────────────────────────────

class _GuiLogHandler(logging.Handler):
    _FMT = logging.Formatter('%(asctime)s  %(levelname)-7s  %(message)s',
                              datefmt='%H:%M:%S')

    def __init__(self, widget: ScrolledText):
        super().__init__()
        self.widget = widget
        self.setFormatter(self._FMT)

    def emit(self, record: logging.LogRecord) -> None:
        msg = self.format(record)
        # Permite tags personalizados via extra={'tag': 'SUCCESS'}
        tag = getattr(record, 'tag', record.levelname)

        def _append():
            self.widget.config(state='normal')
            self.widget.insert('end', msg + '\n', tag)
            self.widget.see('end')
            self.widget.config(state='disabled')

        self.widget.after(0, _append)


# ── App ───────────────────────────────────────────────────────────────────────

class App(tk.Tk):

    def __init__(self):
        super().__init__()
        self.title("CHC CONSULTORES — XTalones BOT v2")
        self.resizable(False, False)

        self.machine = load_machine_config()
        self.session = load_session()

        self._names_df = None
        self._stop_event = threading.Event()
        self._lock = threading.Lock()
        self._stats = {'success': 0, 'failed': 0, 'skipped': 0, 'total': 0}
        self._last_saved_index = -1
        self._resume_from = -1

        self._build_ui()
        self._setup_logging()
        self._load_names_file()
        self._restore_session()

        self.protocol("WM_DELETE_WINDOW", self._on_close)

    # ═════════════════════════════════════════════════════════════════════════
    # Construcción de la interfaz
    # ═════════════════════════════════════════════════════════════════════════

    def _build_ui(self):
        notebook = ttk.Notebook(self)
        notebook.pack(fill='both', expand=True, padx=10, pady=(10, 0))

        tab_proc = ttk.Frame(notebook, padding=10)
        tab_cfg  = ttk.Frame(notebook, padding=10)

        notebook.add(tab_proc, text="  Procesamiento  ")
        notebook.add(tab_cfg,  text="  Configuración  ")

        self._build_tab_procesamiento(tab_proc)
        self._build_tab_configuracion(tab_cfg)
        self._build_action_bar()

    # ── Pestaña Procesamiento ─────────────────────────────────────────────────

    def _build_tab_procesamiento(self, parent):
        # Selección de archivo
        sel = ttk.LabelFrame(parent, text=" Archivo de DUAs ", padding=8)
        sel.pack(fill='x', pady=(0, 6))
        sel.columnconfigure(1, weight=1)

        ttk.Label(sel, text="Archivo:").grid(row=0, column=0, sticky='w')
        self._file_var = tk.StringVar()
        ttk.Entry(sel, textvariable=self._file_var, width=52,
                  state='readonly').grid(row=0, column=1, padx=6, sticky='ew')
        ttk.Button(sel, text="Buscar…",
                   command=self._select_file).grid(row=0, column=2)

        ttk.Label(sel, text="Hoja:").grid(row=1, column=0, sticky='w', pady=(6, 0))
        self._sheet_var = tk.StringVar(value=self.session.get('sheet_name', ''))
        self._sheet_combo = ttk.Combobox(sel, textvariable=self._sheet_var,
                                          width=32, state='disabled')
        self._sheet_combo.grid(row=1, column=1, sticky='w', padx=6, pady=(6, 0))
        self._sheet_combo.bind('<<ComboboxSelected>>', self._on_sheet_selected)

        # Progreso
        prog = ttk.LabelFrame(parent, text=" Progreso ", padding=8)
        prog.pack(fill='x', pady=(0, 6))

        # ── Contador grande X / Y ─────────────────────────────────────────
        counter_row = tk.Frame(prog, bg='#000000')
        counter_row.pack(fill='x', pady=(0, 4))

        self._counter_label = tk.Label(
            counter_row, text="0 / —",
            font=('Consolas', 22, 'bold'), fg='#FFFFFF', bg='#000000',
        )
        self._counter_label.pack(side='left', padx=4)

        self._pct_label = tk.Label(
            counter_row, text="0.0%",
            font=('Consolas', 12), fg='#888888', bg='#000000',
        )
        self._pct_label.pack(side='right', padx=8)

        # ── Barra de progreso ─────────────────────────────────────────────
        self._progress_var = tk.DoubleVar()
        ttk.Progressbar(prog, variable=self._progress_var,
                        maximum=100).pack(fill='x', pady=(0, 4))

        # ── Stats de esta sesión ──────────────────────────────────────────
        bottom_row = ttk.Frame(prog)
        bottom_row.pack(fill='x')

        self._stats_label = ttk.Label(bottom_row,
                                       text="Esta sesion:  OK 0   Error 0   Omitidos 0",
                                       foreground='gray')
        self._stats_label.pack(side='left')

        self._status_label = ttk.Label(bottom_row, text="Esperando para iniciar…",
                                        foreground='gray')
        self._status_label.pack(side='right')

        # Log / Consola
        log_frame = ttk.LabelFrame(parent, text=" Consola ", padding=4)
        log_frame.pack(fill='both', expand=True)

        self._log_text = ScrolledText(
            log_frame, height=13, state='disabled',
            font=('Consolas', 8), wrap='none',
            background='#000000', foreground='#FF8C00',
            insertbackground='white',
        )
        self._log_text.pack(fill='both', expand=True)

        self._log_text.tag_config('DEBUG',    foreground='#444444')
        self._log_text.tag_config('INFO',     foreground='#FF8C00')
        self._log_text.tag_config('WARNING',  foreground='#FFFF00')
        self._log_text.tag_config('ERROR',    foreground='#FF3333')
        self._log_text.tag_config('CRITICAL', foreground='#FF0000',
                                   font=('Consolas', 8, 'bold'))
        self._log_text.tag_config('SUCCESS',  foreground='#00FF41',
                                   font=('Consolas', 8, 'bold'))

    # ── Pestaña Configuración ─────────────────────────────────────────────────

    def _build_tab_configuracion(self, parent):

        proc_frame = ttk.LabelFrame(parent, text=" Parámetros de procesamiento ", padding=10)
        proc_frame.pack(fill='x', pady=(0, 8))
        proc_frame.columnconfigure(1, weight=1)

        params = [
            ("Cantidad de hilos:",  'cant_hilos',  1,  10, 1),
            ("Timeout (segundos):", 'timeout',     5,  120, 10),
            ("Máx. reintentos:",    'max_retries', 1,  10, 3),
        ]
        self._param_vars = {}
        for row_i, (label, key, lo, hi, default) in enumerate(params):
            ttk.Label(proc_frame, text=label).grid(
                row=row_i, column=0, sticky='w', pady=3)
            var = tk.IntVar(value=int(self.machine.get(key, default)))
            self._param_vars[key] = var
            spin = ttk.Spinbox(proc_frame, from_=lo, to=hi, textvariable=var,
                               width=8, state='readonly')
            spin.grid(row=row_i, column=1, sticky='w', padx=8, pady=3)

            descs = {
                'cant_hilos':  "Número de navegadores en paralelo. Recomendado: 1.",
                'timeout':     "Segundos de espera máxima por elemento en la página.",
                'max_retries': "Veces que se reintenta un DUA antes de marcarlo como fallido.",
            }
            ttk.Label(proc_frame, text=descs[key],
                      foreground='gray').grid(row=row_i, column=2, sticky='w', padx=8)

        next_row = len(params)
        self._headless_var = tk.BooleanVar(value=bool(self.machine.get('headless', False)))
        ttk.Checkbutton(
            proc_frame,
            text="Ocultar ventana del navegador (modo headless)",
            variable=self._headless_var,
        ).grid(row=next_row, column=0, columnspan=3, sticky='w', pady=(8, 2))
        ttk.Label(
            proc_frame,
            text="Si está activo, el navegador corre en segundo plano sin mostrarse en pantalla.",
            foreground='gray',
        ).grid(row=next_row + 1, column=0, columnspan=3, sticky='w', padx=4)

        paths_frame = ttk.LabelFrame(parent, text=" Rutas ", padding=10)
        paths_frame.pack(fill='x', pady=(0, 8))
        paths_frame.columnconfigure(1, weight=1)

        path_rows = [
            ("Carpeta de salida:",    'output_base_path', 'folder',
             "Donde se guardan los CSV y archivos de DUAs fallidas."),
            ("Ruta del WebDriver:",  'edge_driver_path', 'file',
             "Ejecutable msedgedriver.exe para controlar Edge."),
        ]
        self._path_vars = {}
        for row_i, (label, key, kind, desc) in enumerate(path_rows):
            ttk.Label(paths_frame, text=label).grid(
                row=row_i * 2, column=0, sticky='w', pady=(6, 0))
            var = tk.StringVar(value=self.machine.get(key, ''))
            self._path_vars[key] = var
            ttk.Entry(paths_frame, textvariable=var, width=52,
                      state='readonly').grid(
                row=row_i * 2, column=1, padx=6, sticky='ew', pady=(6, 0))

            browse_cmd = (
                (lambda k=key: self._browse_folder(k)) if kind == 'folder'
                else (lambda k=key: self._browse_file(k))
            )
            ttk.Button(paths_frame, text="Buscar…",
                       command=browse_cmd).grid(
                row=row_i * 2, column=2, pady=(6, 0))

            ttk.Label(paths_frame, text=desc, foreground='gray').grid(
                row=row_i * 2 + 1, column=1, columnspan=2,
                sticky='w', padx=6, pady=(0, 2))

        driver_frame = ttk.LabelFrame(parent, text=" WebDriver ", padding=10)
        driver_frame.pack(fill='x', pady=(0, 8))

        self._driver_info_var = tk.StringVar(value="Haga clic en 'Verificar' para comprobar versiones.")
        ttk.Label(driver_frame, textvariable=self._driver_info_var,
                  justify='left').pack(anchor='w')

        driver_btns = ttk.Frame(driver_frame)
        driver_btns.pack(anchor='w', pady=(6, 0))
        ttk.Button(driver_btns, text="Verificar versiones",
                   command=self._check_driver_versions).pack(side='left')
        ttk.Button(driver_btns, text="Actualizar Driver…",
                   command=self._show_driver_dialog).pack(side='left', padx=(8, 0))

        save_row = ttk.Frame(parent)
        save_row.pack(fill='x', pady=(4, 0))
        ttk.Button(save_row, text="  Guardar configuración  ",
                   command=self._save_config).pack(side='right')
        self._cfg_saved_label = ttk.Label(save_row, text="", foreground='#006400')
        self._cfg_saved_label.pack(side='right', padx=10)

    # ── Barra de acciones ─────────────────────────────────────────────────────

    def _build_action_bar(self):
        bar = ttk.Frame(self, padding=(10, 6, 10, 10))
        bar.pack(fill='x')

        self._folder_btn = ttk.Button(bar, text="Abrir carpeta resultados",
                                       command=self._open_output_folder)
        self._folder_btn.pack(side='left')

        self._stop_btn = ttk.Button(bar, text="Detener",
                                     command=self._stop_processing, state='disabled')
        self._stop_btn.pack(side='right', padx=(6, 0))

        self._start_btn = ttk.Button(bar, text="  Iniciar  ",
                                      command=self._start_processing)
        self._start_btn.pack(side='right')

        ttk.Button(bar, text="Reiniciar progreso",
                   command=self._reset_session).pack(side='right', padx=(0, 12))

    # ═════════════════════════════════════════════════════════════════════════
    # Logging
    # ═════════════════════════════════════════════════════════════════════════

    def _setup_logging(self):
        setup_logging()
        handler = _GuiLogHandler(self._log_text)
        handler.setLevel(logging.INFO)
        logging.getLogger().addHandler(handler)

    # ═════════════════════════════════════════════════════════════════════════
    # Sesión
    # ═════════════════════════════════════════════════════════════════════════

    def _restore_session(self):
        sheet_name     = self.session.get('sheet_name', '')
        extracted_path = get_extracted_path(sheet_name) if sheet_name else ''
        if extracted_path and os.path.exists(extracted_path):
            self._file_var.set(extracted_path)
            self._populate_sheets(extracted_path, sheet_name)
        else:
            path = self.session.get('input_file_path', '')
            if path and os.path.exists(path):
                self._file_var.set(path)
                self._populate_sheets(path, sheet_name)
        self._refresh_counter()

    def _refresh_counter(self):
        """Actualiza el contador X/Y leyendo el progreso guardado en sesión."""
        key   = normalize_key(self.session.get('sheet_name', ''))
        done  = self.session.get('progress', {}).get(key, -1) + 1
        total = self.session.get('totals',   {}).get(key, 0)
        if total > 0:
            pct = done / total * 100
            self._counter_label.config(text=f"{done:,} / {total:,}")
            self._pct_label.config(text=f"{pct:.1f}%")
            self._progress_var.set(pct)
        elif done > 0:
            self._counter_label.config(text=f"{done:,} / —")
            self._pct_label.config(text="—")
        else:
            self._counter_label.config(text="0 / —")
            self._pct_label.config(text="0.0%")

    # ═════════════════════════════════════════════════════════════════════════
    # Selectores — pestaña Procesamiento
    # ═════════════════════════════════════════════════════════════════════════

    def _select_file(self):
        path = filedialog.askopenfilename(
            title="Seleccionar archivo de DUAs",
            filetypes=[("Excel", "*.xlsx"), ("Todos los archivos", "*.*")],
        )
        if not path:
            return
        self._file_var.set(path)
        self._populate_sheets(path, self.session.get('sheet_name', ''))
        self.session['input_file_path'] = path
        self.session['sheet_name'] = self._sheet_var.get().strip()
        save_session(self.session)

    def _on_sheet_selected(self, event=None):
        self.session['sheet_name'] = self._sheet_var.get().strip()
        save_session(self.session)

    def _populate_sheets(self, path: str, preferred: str = '') -> None:
        try:
            sheets = pd.ExcelFile(path).sheet_names
        except Exception as e:
            messagebox.showerror("Error", f"No se pudo abrir el archivo:\n{e}")
            return
        self._sheet_combo.config(values=sheets, state='readonly')
        if preferred in sheets:
            self._sheet_var.set(preferred)
        elif sheets:
            self._sheet_var.set(sheets[0])

    def _open_output_folder(self):
        sheet_name = self._sheet_var.get().strip()
        base = self.machine.get('output_base_path', '')
        target = os.path.join(base, sheet_name) if sheet_name else base
        if not os.path.exists(target):
            target = base
        if not target or not os.path.exists(target):
            messagebox.showwarning(
                "Carpeta no encontrada",
                "La carpeta de salida no existe todavía.\n"
                "Inicie el proceso al menos una vez para crearla.",
            )
            return
        os.startfile(target)

    # ═════════════════════════════════════════════════════════════════════════
    # Selectores — pestaña Configuración
    # ═════════════════════════════════════════════════════════════════════════

    def _browse_folder(self, key: str):
        folder = filedialog.askdirectory(title="Seleccionar carpeta")
        if folder:
            self._path_vars[key].set(folder)

    def _browse_file(self, key: str):
        filetypes = (
            [("Excel", "*.xlsx")] if 'names' in key
            else [("Ejecutable", "*.exe"), ("Todos", "*.*")]
        )
        path = filedialog.askopenfilename(title="Seleccionar archivo",
                                          filetypes=filetypes)
        if path:
            self._path_vars[key].set(path)

    def _save_config(self):
        for key, var in self._param_vars.items():
            try:
                self.machine[key] = int(var.get())
            except ValueError:
                messagebox.showerror("Error", f"Valor inválido para '{key}'.")
                return

        for key, var in self._path_vars.items():
            self.machine[key] = var.get().strip()

        self.machine['headless'] = bool(self._headless_var.get())

        save_machine_config(self.machine)

        self._cfg_saved_label.config(text="Guardado")
        self.after(3000, lambda: self._cfg_saved_label.config(text=""))
        logging.info("Configuracion guardada.")

    # ═════════════════════════════════════════════════════════════════════════
    # CAPTCHAs
    # ═════════════════════════════════════════════════════════════════════════

    def _load_names_file(self):
        path = get_names_path()
        if not os.path.exists(path):
            logging.error("Names_TicaPortal.xlsx no encontrado: %s", path)
            messagebox.showerror(
                "Error interno",
                "No se encontró el archivo de CAPTCHAs embebido.\n"
                "Por favor contacte al administrador.",
            )
            return
        self._names_df = pd.read_excel(path)
        logging.info("Archivo de CAPTCHAs cargado (%d entradas).", len(self._names_df))

    # ═════════════════════════════════════════════════════════════════════════
    # Inicio / detención
    # ═════════════════════════════════════════════════════════════════════════

    def _start_processing(self):
        if self._names_df is None:
            messagebox.showerror(
                "Error",
                "No se cargo el archivo de CAPTCHAs.\n"
                "Verifique la ruta en la pestana Configuracion.",
            )
            return

        file_path   = self._file_var.get()
        sheet_name  = self._sheet_var.get().strip()
        output_base = self.machine.get('output_base_path', '')

        if not file_path or not os.path.exists(file_path):
            messagebox.showwarning("Atencion", "Seleccione un archivo de DUAs.")
            return
        if not sheet_name:
            messagebox.showwarning("Atencion", "Seleccione una hoja del archivo.")
            return
        if not output_base:
            messagebox.showwarning(
                "Atencion",
                "Configure la carpeta de salida en la pestana Configuracion.",
            )
            return

        try:
            df = pd.read_excel(file_path, sheet_name=sheet_name)
            df[['Aduana', 'Año', 'Número']] = df['NroDUA'].str.split('-', expand=True)
        except Exception as e:
            messagebox.showerror("Error al cargar datos", str(e))
            return

        extracted_path = get_extracted_path(sheet_name)
        try:
            pd.read_excel(file_path, sheet_name=sheet_name).to_excel(
                extracted_path, sheet_name=sheet_name, index=False)
        except Exception as e:
            logging.warning("No se pudo guardar copia local de la hoja: %s", e)

        self.session = load_session()
        self.session['sheet_name']      = sheet_name
        self.session['input_file_path'] = file_path
        self.session.setdefault('totals', {})[normalize_key(sheet_name)] = len(df)
        save_session(self.session)

        output_paths = compute_output_paths(output_base, sheet_name)
        load_processed_duas_from_log(output_paths['processed_log'])

        key        = normalize_key(sheet_name)
        progress   = self.session.get('progress', {})
        start_from = progress.get(key, -1)
        remaining  = len(df) - (start_from + 1)

        logging.info("─── INICIO DE PROCESAMIENTO ───────────────────────────")
        logging.info("Hoja seleccionada : '%s'  →  clave: '%s'", sheet_name, key)
        logging.info("Progreso guardado : %s", progress)
        logging.info("Indice de inicio  : %d  (reanudando desde fila %d / %d)",
                     start_from, start_from + 2, len(df))
        logging.info("DUAs pendientes   : %d", max(remaining, 0))
        logging.info("───────────────────────────────────────────────────────")

        self._resume_from      = start_from
        self._last_saved_index = start_from
        self._stats = {'success': 0, 'failed': 0, 'skipped': 0, 'total': len(df)}
        self._update_stats_label()

        done_initial = start_from + 1
        total        = len(df)
        pct_initial  = done_initial / total * 100 if total > 0 else 0
        self._counter_label.config(text=f"{done_initial:,} / {total:,}")
        self._pct_label.config(text=f"{pct_initial:.1f}%")
        self._progress_var.set(pct_initial)

        resume_msg = f"Desde fila {start_from + 2}" if start_from >= 0 else "Inicio"
        self._status_label.config(
            text=f"{resume_msg} — {max(remaining, 0)} pendientes",
            foreground='#FF8C00',
        )

        self._set_controls_running(True)
        self._stop_event = threading.Event()

        threading.Thread(
            target=self._run,
            args=(df, output_paths, start_from),
            daemon=True,
        ).start()

    def _stop_processing(self):
        self._stop_event.set()
        self._status_label.config(text="Deteniendo…", foreground='#b87000')
        self._stop_btn.config(state='disabled')

    def _set_controls_running(self, running: bool):
        self._start_btn.config(state='disabled' if running else 'normal')
        self._stop_btn.config(state='normal' if running else 'disabled')
        self._sheet_combo.config(state='disabled' if running else 'readonly')

    # ═════════════════════════════════════════════════════════════════════════
    # Hilo de control
    # ═════════════════════════════════════════════════════════════════════════

    def _run(self, df: pd.DataFrame, output_paths: dict, start_from: int):
        n_workers = self.machine.get('cant_hilos', 1)
        flow = XTalonesFlow()
        processors = [
            DUAProcessor(
                self.machine, output_paths, flow,
                self._names_df, self._lock, self._stop_event,
            )
            for _ in range(n_workers)
        ]
        try:
            with ThreadPoolExecutor(max_workers=n_workers) as executor:
                futures = []
                skipped_count = 0
                for idx, row in df.iterrows():
                    if self._stop_event.is_set():
                        break
                    if idx <= start_from:
                        skipped_count += 1
                        continue
                    if skipped_count > 0:
                        logging.info("Saltadas %d filas ya procesadas (indices 0-%d).",
                                     skipped_count, start_from)
                        skipped_count = 0
                    proc = processors[idx % n_workers]
                    futures.append(
                        executor.submit(proc.process_dua, idx, row, self._on_progress)
                    )
                for f in futures:
                    f.result()
        except Exception as e:
            logging.exception("Error en la ejecucion principal: %s", e)
        finally:
            for p in processors:
                p.close()
            self.after(0, self._on_finished)

    # ═════════════════════════════════════════════════════════════════════════
    # Callbacks de progreso
    # ═════════════════════════════════════════════════════════════════════════

    def _on_progress(self, index: int, status: str) -> None:
        def _gui():
            self._stats[status] = self._stats.get(status, 0) + 1
            total = self._stats['total']

            already      = self._resume_from + 1
            done_session = (self._stats['success']
                            + self._stats['failed']
                            + self._stats['skipped'])
            done_total = already + done_session
            pct = done_total / total * 100 if total > 0 else 0

            self._counter_label.config(text=f"{done_total:,} / {total:,}")
            self._pct_label.config(text=f"{pct:.1f}%")
            self._progress_var.set(pct)
            self._update_stats_label()
            pending = total - done_total
            self._status_label.config(
                text=f"Procesando — {pending} pendientes",
                foreground='#FF8C00',
            )

            if index > self._last_saved_index:
                self._last_saved_index = index
                key = normalize_key(self.session.get('sheet_name', ''))
                self.session.setdefault('progress', {})[key] = index
                save_session(self.session)
                logging.debug("Progreso guardado → clave '%s' = indice %d", key, index)

        self.after(0, _gui)

    def _on_finished(self):
        if self._last_saved_index >= 0:
            key = normalize_key(self.session.get('sheet_name', ''))
            stored = self.session.get('progress', {}).get(key, -1)
            if self._last_saved_index > stored:
                self.session.setdefault('progress', {})[key] = self._last_saved_index
                save_session(self.session)

        s       = self._stats
        stopped = self._stop_event.is_set()
        self._set_controls_running(False)
        self._sheet_combo.config(state='readonly')
        color = '#FFFF00' if stopped else '#00FF41'
        msg   = "Detenido" if stopped else "Completado"
        self._status_label.config(text=msg, foreground=color)
        logging.info("─── %s ─── OK:%d  Error:%d  Omitidos:%d",
                     msg.upper(), s['success'], s['failed'], s['skipped'])
        messagebox.showinfo(
            "Proceso finalizado",
            f"Proceso {'detenido' if stopped else 'completado'}.\n\n"
            f"OK       {s['success']}\n"
            f"Error    {s['failed']}\n"
            f"Omitidos {s['skipped']}",
        )

    def _update_stats_label(self):
        s = self._stats
        self._stats_label.config(
            text=(f"Esta sesion:  "
                  f"OK {s['success']}   "
                  f"Error {s['failed']}   "
                  f"Omitidos {s['skipped']}")
        )

    # ═════════════════════════════════════════════════════════════════════════
    # Driver
    # ═════════════════════════════════════════════════════════════════════════

    def _check_driver_versions(self):
        driver_path = self.machine.get('edge_driver_path',
                                        os.path.join(DEST_DIR, DEST_FILE))
        try:
            edge_ver   = get_edge_version()
            driver_ver = get_current_driver_version(driver_path) or "No instalado"
            match      = (edge_ver == driver_ver)
            icon       = "OK" if match else "!!"
            status     = "Versiones coinciden." if match else "Versiones NO coinciden — se recomienda actualizar."
            self._driver_info_var.set(
                f"Edge instalado: {edge_ver}   |   Driver actual: {driver_ver}\n{icon} {status}"
            )
        except RuntimeError as e:
            self._driver_info_var.set(f"Error: {e}")

    def _show_driver_dialog(self):
        driver_path = self.machine.get('edge_driver_path',
                                       os.path.join(DEST_DIR, DEST_FILE))
        dlg = tk.Toplevel(self)
        dlg.title("Actualizar WebDriver")
        dlg.resizable(False, False)
        dlg.grab_set()

        try:
            edge_ver = get_edge_version()
        except RuntimeError as e:
            messagebox.showerror("Error", str(e), parent=dlg)
            dlg.destroy()
            return

        driver_ver = get_current_driver_version(driver_path) or "No instalado"
        match = (edge_ver == driver_ver)

        ttk.Label(
            dlg,
            text=(f"Edge instalado :  {edge_ver}\n"
                  f"Driver actual  :  {driver_ver}\n\n"
                  + ("Las versiones coinciden, no es necesario actualizar."
                     if match else
                     "Las versiones no coinciden. Se recomienda actualizar.")),
            padding=14, justify='left', font=('Segoe UI', 9),
        ).pack()

        pbar = ttk.Progressbar(dlg, length=380, maximum=100)
        pbar.pack(padx=14, pady=(0, 4))
        kb_label = ttk.Label(dlg, text="")
        kb_label.pack(pady=(0, 6))

        btn_row = ttk.Frame(dlg, padding=(14, 0, 14, 14))
        btn_row.pack()

        def _do_update():
            update_btn.config(state='disabled')
            close_btn.config(state='disabled')

            def _progress(pct: float, kb: int):
                def _upd():
                    pbar.config(value=pct)
                    kb_label.config(text=f"{kb} KB descargados ({pct:.0f}%)")

                dlg.after(0, _upd)

            def _download():
                try:
                    download_driver(edge_ver, os.path.dirname(driver_path), _progress)
                    self.machine['edge_driver_path'] = driver_path
                    save_machine_config(self.machine)
                    self._path_vars['edge_driver_path'].set(driver_path)
                    dlg.after(0, lambda: messagebox.showinfo(
                        "Listo", f"Driver actualizado a la version {edge_ver}.", parent=dlg))
                    logging.info("Driver actualizado a %s.", edge_ver)
                except RuntimeError as exc:
                    err_msg = str(exc)
                    dlg.after(0, lambda msg=err_msg: messagebox.showerror(
                        "Error al descargar", msg, parent=dlg))
                finally:
                    dlg.after(0, lambda: (
                        update_btn.config(state='normal'),
                        close_btn.config(state='normal'),
                    ))

            threading.Thread(target=_download, daemon=True).start()

        update_btn = ttk.Button(btn_row, text="Actualizar ahora", command=_do_update)
        update_btn.pack(side='left', padx=5)
        close_btn = ttk.Button(btn_row, text="Cerrar", command=dlg.destroy)
        close_btn.pack(side='left', padx=5)

    def _reset_session(self):
        if not messagebox.askyesno(
                "Reiniciar progreso",
                "¿Desea reiniciar el progreso y volver a procesar desde cero?\n\n"
                "Se limpiarán únicamente: progreso y totales guardados.\n"
                "El archivo y la hoja seleccionados se conservarán.",
        ):
            return

        self.session['progress'] = {}
        self.session['totals'] = {}
        save_session(self.session)

        self._counter_label.config(text="0 / —")
        self._pct_label.config(text="0.0%")
        self._progress_var.set(0)
        self._stats = {'success': 0, 'failed': 0, 'skipped': 0, 'total': 0}
        self._update_stats_label()
        self._status_label.config(
            text="Progreso reiniciado. Listo para iniciar desde 0.",
            foreground='gray'
        )
        self._resume_from = -1
        self._last_saved_index = -1
        logging.info("Progreso reiniciado manualmente.")
    # ═════════════════════════════════════════════════════════════════════════
    # Cierre
    # ═════════════════════════════════════════════════════════════════════════

    def _on_close(self):
        if threading.active_count() > 2:
            if not messagebox.askyesno(
                "Salir",
                "Hay un proceso en curso. Desea detenerlo y salir?",
            ):
                return
            self._stop_event.set()
        self.after(300, self.destroy)
