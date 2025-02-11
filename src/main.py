#main.py
import os
import gc
import pandas as pd
import threading
import tkinter as tk
from tkinter import ttk, filedialog
from concurrent.futures import ThreadPoolExecutor
from processors import DUAProcessor
from utils import load_config, save_config, setup_logging
import logging
import re

def normalize_sheet_name(sheet_name):
    # Reemplazar caracteres que no sean letras, números o guiones bajos por guiones bajos
    return re.sub(r'\W+', '_', sheet_name)

def update_output_paths(config):
    sheet_name = config['sheet_name']
    base_output_folder = os.path.join(config['output_base_path'], sheet_name)

    # Crear el directorio si no existe
    if not os.path.exists(base_output_folder):
        os.makedirs(base_output_folder)

    # Actualizar las rutas en el config
    config['output_csv'] = os.path.join(base_output_folder, f'Duas_procesados_{sheet_name}.csv')
    config['failed_duas'] = os.path.join(base_output_folder, f'DUAs_fallidas_{sheet_name}.txt')
    config['no_rows_duas'] = os.path.join(base_output_folder, f'DUAs_sin_filas_{sheet_name}.txt')

    # Mantener 'extracted_file_path' en su ubicación original
    extracted_data_folder = os.path.join('data', 'Datos_Cargados')
    if not os.path.exists(extracted_data_folder):
        os.makedirs(extracted_data_folder)
    config['extracted_file_path'] = os.path.join(extracted_data_folder, f'{sheet_name}.xlsx')


def main():
    # Configurar logging
    setup_logging()

    # Cargar configuración
    config = load_config()

    # Actualizar rutas de salida en config
    update_output_paths(config)

    # Normalizar el sheet_name para usarlo en las claves
    normalized_sheet_name = normalize_sheet_name(config['sheet_name'])

    # Generar la clave para max_index_processed
    max_index_key = f'max_index_processed_{normalized_sheet_name}'

    # Variables globales y estado
    lock = threading.Lock()
    stop_event = threading.Event()
    max_index_processed = config.get(max_index_key, -1)

    # Crear la aplicación Tkinter
    root = tk.Tk()
    root.title("CHC CONSULTORES BOT - Talon")

    # Crear variables de Tkinter asociadas a 'root'
    progress_var = tk.DoubleVar(master=root)
    progress_text_var = tk.StringVar(master=root)
    progress_text_var.set("Esperando para iniciar...")

    # Manejar el cierre de la ventana
    def on_closing():
        nonlocal stop_event, root
        if threading.active_count() > 1:
            stop_processing()
            # Esperar a que los hilos terminen
            root.after(100, check_threads)
        else:
            root.destroy()

    def check_threads():
        if threading.active_count() > 1:
            # Aún hay hilos activos, volver a comprobar en 100ms
            root.after(100, check_threads)
        else:
            root.destroy()

    root.protocol("WM_DELETE_WINDOW", on_closing)

    # Cargar nombres de captchas
    names_file_path = os.path.join(config['base_path'], *config['names_file'])
    if not os.path.exists(names_file_path):
        logging.error(f"No se encontró el archivo Names.xlsx en la ruta: {names_file_path}")
        progress_text_var.set(f"Error: No se encontró Names.xlsx en {names_file_path}")
        return

    names_df = pd.read_excel(names_file_path)

    # Variables para los datos
    duals_df = None
    extracted_file_path = config.get('extracted_file_path', '')

    # Si existe el archivo extraído, cargar los datos desde allí
    if extracted_file_path and os.path.exists(extracted_file_path):
        duals_df = pd.read_excel(extracted_file_path)
        duals_df[['Aduana', 'Año', 'Número']] = duals_df['NroDUA'].str.split('-', expand=True)
        progress_text_var.set(f"Archivo cargado: {extracted_file_path}")
    else:
        # Si no existe, inicializar variables
        progress_text_var.set("No se ha seleccionado ningún archivo de DUAs.")
        max_index_processed = -1

    # Funciones internas

    def select_duas_file():
        nonlocal duals_df, extracted_file_path, max_index_processed, config, max_index_key, normalized_sheet_name
        totals_file_path = filedialog.askopenfilename(filetypes=[("Excel files", "*.xlsx")])
        if totals_file_path:
            extracted_file_path = extract_and_save_sheet(totals_file_path)
            duals_df = pd.read_excel(extracted_file_path)
            duals_df[['Aduana', 'Año', 'Número']] = duals_df['NroDUA'].str.split('-', expand=True)
            progress_text_var.set(f"Archivo cargado: {totals_file_path}")

            # Actualizar el sheet_name y las claves correspondientes
            sheet_name = config['sheet_name']
            normalized_sheet_name = normalize_sheet_name(sheet_name)
            max_index_key = f'max_index_processed_{normalized_sheet_name}'

            # Obtener el max_index_processed para la nueva hoja
            max_index_processed = config.get(max_index_key, -1)

            # Actualizar la configuración y guardar
            config['extracted_file_path'] = extracted_file_path
            save_config(config)

    def extract_and_save_sheet(totals_file_path):
        sheet_name = config['sheet_name']
        extracted_data_folder = os.path.join('data', 'Datos_Cargados')

        # Crear el directorio si no existe
        if not os.path.exists(extracted_data_folder):
            os.makedirs(extracted_data_folder)

        # Usa el nombre de la hoja para el nombre del archivo
        extracted_file_name = f"{sheet_name}.xlsx"
        extracted_file_path = os.path.join(extracted_data_folder, extracted_file_name)

        # Cargar la hoja y guardarla en el archivo generado dinámicamente
        df_sheet = pd.read_excel(totals_file_path, sheet_name=sheet_name)
        df_sheet.to_excel(extracted_file_path, index=False)

        # Retornar la ruta del archivo creado
        return extracted_file_path

    def update_progress(index):
        nonlocal max_index_processed, config, root, max_index_key
        progress = (index + 1) / len(duals_df) * 100
        max_index_processed = index  # Actualizar el índice máximo procesado

        # Función para actualizar la interfaz en el hilo principal
        def gui_update():
            progress_var.set(progress)
            progress_text_var.set(f"Procesando DUAs: {index + 1}/{len(duals_df)} ({progress:.2f}%)")

        # Programar la actualización en el hilo principal
        root.after(0, gui_update)

        # Guardar max_index_processed en config.json bajo la clave correcta
        config[max_index_key] = max_index_processed
        save_config(config)

    def start_processing():
        nonlocal stop_event, duals_df
        if duals_df is None:
            progress_text_var.set("Error: No se ha cargado el archivo de DUAs.")
            return
        stop_event.clear()
        progress_text_var.set("Procesando...")
        threading.Thread(target=main_threaded_execution).start()

    def stop_processing():
        nonlocal stop_event
        stop_event.set()
        progress_text_var.set("Proceso detenido")

    def main_threaded_execution():
        nonlocal stop_event, max_index_processed, config
        processors = []
        executor = ThreadPoolExecutor(max_workers=config['cant_hilos'])
        futures = []

        for i in range(config['cant_hilos']):
            processor = DUAProcessor(config, names_df, lock, stop_event)
            processors.append(processor)

        try:
            for idx, row in duals_df.iterrows():
                if stop_event.is_set():
                    break
                if idx <= max_index_processed:
                    continue  # Saltar las DUAs ya procesadas
                processor = processors[idx % len(processors)]
                future = executor.submit(processor.process_dua, idx, row, update_progress)
                futures.append(future)

            # Esperar a que las tareas actuales terminen
            for future in futures:
                future.result()

        except Exception as e:
            logging.exception(f"Error en la ejecución principal: {e}")
        finally:
            for processor in processors:
                processor.close()
            if stop_event.is_set():
                progress_text_var.set("Proceso detenido")
            else:
                progress_text_var.set("Proceso completado")

            # Guardar max_index_processed en config.json bajo la clave correcta
            config[max_index_key] = max_index_processed
            save_config(config)

    # Interfaz gráfica
    progress_bar = ttk.Progressbar(root, orient="horizontal", length=400, mode="determinate", variable=progress_var)
    progress_bar.pack(pady=20)

    label_progress = tk.Label(root, textvariable=progress_text_var)
    label_progress.pack()

    select_file_button = tk.Button(root, text="Seleccionar Archivo DUAs", command=select_duas_file)
    select_file_button.pack(pady=10)

    start_button = tk.Button(root, text="Iniciar", command=start_processing)
    start_button.pack(side="left", padx=10)

    stop_button = tk.Button(root, text="Detener", command=stop_processing)
    stop_button.pack(side="right", padx=10)

    # Iniciar el bucle principal de Tkinter
    root.mainloop()



if __name__ == "__main__":
    main()
