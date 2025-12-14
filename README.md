# GlobalPointsWatcher - Sistema de Puntos Telegram

Este sistema consta de tres componentes principales: la base de datos (SQL Server), el bot de Telegram y el lector de correos (Gmail Watcher).

## 1. Configuración Inicial (Antes de ejecutar)

### 1.1. Archivo .env
Debes crear un archivo llamado `.env` en el directorio principal y agregar las siguientes variables.

| Variable | Valor Requerido | Origen |
| :--- | :--- | :--- |
| `TELEGRAM_TOKEN` | El token largo generado por BotFather. | **BotFather** |
| `MSSQL_SERVER` | Dirección de tu servidor SQL (ej: `localhost` o IP). | SQL Server |
| `MSSQL_DATABASE` | `GlobalPointsWatcher` | SQL Server |
| `MSSQL_USER` | `GlobalPointsAppUser` | SQL Server |
| `MSSQL_PASSWORD` | Contraseña del usuario `GlobalPointsAppUser`. | SQL Server |

### 1.2. Habilitar correo de pruebas

Para que el sistema lea correos de prueba desde tu dirección personal (simulando ser el banco), debes agregar tu correo a la lista blanca.

1.  Abre el archivo `gmail_watcher.py`.
2.  Busca la lista `ALLOWED_SENDERS` cerca del inicio.
3.  Reemplaza `correo_personal@gmail.com` por tu dirección real.

## 2. Orden de Ejecución

Los dos scripts de Python deben ejecutarse de forma concurrente en dos terminales separadas.

### Terminal 1: Iniciar el Bot de Telegram

```bash
python telegram_bot.py

### Terminal 2: Iniciar el lector de correo gmail

```bash
python gmail_watcher.py

## 3. Flujo de Pruebas
Registro: Abre Telegram, busca el bot e ingresa el comando /registro. Sigue los pasos para vincular tu dirección de Gmail y obtener la Contraseña de Aplicación de Google.

Prueba de Detección: Después de registrarte, envíate un correo a ti mismo desde tu cuenta de prueba, usando el asunto "CONFIRMACION DE TRANSACCION" y un cuerpo que contenga:

El nombre de un comercio (ej: NETFLIX.COM).

La palabra terminación seguida de 4 dígitos (ej: 9999).

El símbolo de dólar y un monto (ej: $19.99).

Confirmación: El bot en Telegram debería notificarte la compra y guiarte a través de los pasos secuenciales: primero seleccionar el multiplicador y luego la categoría.

Otros comandos:

/recientes: Ver las últimas transacciones y editarlas.

/tarjetas: Listar las tarjetas registradas.

/resumen: Ver el resumen de puntos del mes.





