import logging
import os
from logging.handlers import RotatingFileHandler

class AppLogger:
    def __init__(self, module_name="GlobalPointsApp", log_file="watcher.log"):
        """
        :param module_name: Nombre que aparecerá en el log ('DB_Client', 'GmailWatcher',...)
        :param log_file: Nombre del archivo físico.
        """
        self.logger = logging.getLogger(module_name)
        self.logger.setLevel(logging.INFO)

        # Evitar duplicar mensajes si se instancia varias veces
        if not self.logger.handlers:
            try:
                os.makedirs("logs", exist_ok=True)
                file_path = os.path.join("logs", log_file)
            except Exception:
                file_path = log_file 

            formatter = logging.Formatter(
                '%(asctime)s [%(levelname)s] [%(name)s] %(message)s', 
                datefmt='%Y-%m-%d %H:%M:%S'
            )

            # 2. Archivo con ROTACIÓN
            try:
                file_handler = RotatingFileHandler(
                    file_path, maxBytes=5*1024*1024, backupCount=5, encoding='utf-8'
                )
                file_handler.setFormatter(formatter)
                self.logger.addHandler(file_handler)
            except Exception as e:
                print(f"No se pudo crear el archivo de log: {e}")

            #En consola
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)
            self.logger.addHandler(console_handler)

    def info(self, msg):
        self.logger.info(msg)

    def error(self, msg):
        self.logger.error(msg)

    def warning(self, msg):
        self.logger.warning(msg)