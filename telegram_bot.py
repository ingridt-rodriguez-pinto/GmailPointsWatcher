import os
from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import (
    ApplicationBuilder, CommandHandler, ContextTypes, 
    ConversationHandler, MessageHandler, CallbackQueryHandler, filters
)
from dotenv import load_dotenv
from db_client import GlobalPointsDB
from logger_helper import AppLogger

# Cargar configuración
load_dotenv()
TOKEN = os.getenv("TELEGRAM_TOKEN")
log = AppLogger("TelegramBot")

# Estados de la conversación de registro
ASK_EMAIL, ASK_PASSWORD = range(2)

# --- COMANDOS BÁSICOS ---
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Mensaje de bienvenida."""
    user_name = update.effective_user.first_name
    msg = (
        f"¡Hola {user_name}! Soy tu Asistente de Puntos.\n\n"
        "**Comandos disponibles:**\n"
        "🔹 /registro - Vincula tu correo (Gmail) para leer compras.\n"
        "🔹 /recientes - Ver últimas 5 compras (y editarlas).\n"
        "🔹 /tarjetas - Listar tus tarjetas registradas.\n"
        "🔹 /resumen - Ver estadísticas del mes actual.\n\n"
        "Yo vigilaré tus correos y te avisaré de cada compra nueva."
    )
    await update.message.reply_text(msg)

# --- FLUJO DE REGISTRO ---
async def registro_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("📧 Por favor, escribe tu dirección de correo (Gmail):")
    return ASK_EMAIL

async def receive_email(update: Update, context: ContextTypes.DEFAULT_TYPE):
    email = update.message.text.strip()
    if "@" not in email:
        await update.message.reply_text("Correo inválido. Intenta de nuevo:")
        return ASK_EMAIL
    
    context.user_data['email'] = email

    msg_explicativo = (
        "✅ Correo recibido.\n\n"
        "🔐 **Paso Final: Contraseña de Aplicación**\n\n"
        "Google no permite usar tu contraseña normal por seguridad. "
        "Necesitas generar una contraseña especial de 16 letras para este Bot.\n\n"
        "👇 **Sigue estos pasos exactos:**\n\n"
        "1. Asegúrate de tener la **Verificación en 2 Pasos** activada.\n"
        "2. Entra a este enlace directo:\n"
        "[Google App Passwords](https://myaccount.google.com/apppasswords)\n\n"
        "3. Escribe el nombre `BotPuntos` y dale a **Crear**.\n"
        "4. Te saldrá un código de 16 letras (ej: `abcd efgh ijkl mnop`).\n\n"
        "**Copia ese código y pégalo aquí abajo:**"
    )

    await update.message.reply_text(
        msg_explicativo, parse_mode="Markdown", disable_web_page_preview=True
    )
    return ASK_PASSWORD

async def receive_password(update: Update, context: ContextTypes.DEFAULT_TYPE):
    password = update.message.text.strip()
    email = context.user_data['email']
    chat_id = update.effective_chat.id

    await update.message.reply_text("💾 Guardando credenciales...")

    db = GlobalPointsDB()
    if db.register_user_credentials(chat_id, email, password):
        await update.message.reply_text("**¡Registro Exitoso!** Ya puedo leer tus correos.", parse_mode="Markdown")
    else:
        await update.message.reply_text("❌ Error al guardar en BD. Intenta /registro de nuevo.")

    context.user_data.clear()
    return ConversationHandler.END

# RECIENTES
async def recientes(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    db = GlobalPointsDB()
    txs = db.get_recent_transactions(chat_id,5)

    if not txs:
        await update.message.reply_text("No tienes transacciones recientes.")
        return

    await update.message.reply_text("**Últimas Transacciones:**", parse_mode="Markdown")

    for tx in txs:
        msg = (
            f"📅 `{tx['fecha']}`\n"
            f"🏪 **{tx['comercio']}**\n"
            f"💵 ${tx['monto']}  ➡️  ⭐ **{tx['puntos']} pts** (x{tx['multiplicador']})"
        )
        
        # Botón para Editar
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("✏️ Editar Puntos", callback_data=f"edit|{tx['id']}")]
        ])
        
        await update.message.reply_text(msg, reply_markup=keyboard, parse_mode="Markdown")

# Listar Tarjetas
async def tarjetas(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Lista las tarjetas del usuario."""
    chat_id = update.effective_chat.id
    db = GlobalPointsDB()
    cards = db.get_user_cards(chat_id)

    if not cards:
        await update.message.reply_text("No tienes tarjetas registradas aún.")
        return

    msg = "💳 **Mis Tarjetas Registradas**\n"
    for c in cards:
        msg += f"**{c['banco']}** ••••{c['last4']}\n     _{c['alias']}_"

    await update.message.reply_text(msg, parse_mode="Markdown")

async def resumen(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Muestra el resumen del mes actual."""
    chat_id = update.effective_chat.id
    db = GlobalPointsDB()
    data = db.get_monthly_summary(chat_id)

    if not data or data['count'] == 0:
        await update.message.reply_text("📊 Aún no hay movimientos este mes.")
        return

    msg = (
        f"📊 **Resumen de {data['month_name']}**\n"
        f"──────────────────\n"
        f"   Gastado:     **${data['total_usd']:,.2f}**\n"
        f"⭐ Puntos:      **{data['total_points']}**\n"
        f"   Movimientos: **{data['count']}**\n"
        f"   Categoría con más gastos:   **{data['top_category']}**"
    )
    await update.message.reply_text(msg, parse_mode="Markdown")

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🚫 Cancelado.")
    return ConversationHandler.END


# --- MANEJO DE BOTONES ---
async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Maneja los clics en los botones."""
    query = update.callback_query
    await query.answer()
    
    data = query.data.split("|")
    accion = data[0]

    db = GlobalPointsDB()

    # Recuperamos el texto base del mensaje (lo que no cambia, la info de la compra)
    texto_original = query.message.text_markdown
    
    # Intentamos encontrar la parte base de la transacción
    # La buscamos hasta la primera línea de confirmación o prompt.
    base_text_lines = []
    for line in texto_original.split('\n'):
        if line.startswith('✅') or line.startswith('❌') or line.startswith('👇'):
            break
        base_text_lines.append(line)
    
    base_text = '\n'.join(base_text_lines).strip()
    
    # --- A. EDICIÓN MANUAL (Botón "Editar") ---
    if accion == "edit":
        tx_id = data[1]
        keyboard = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("x1", callback_data=f"setmult|{tx_id}|1.0"),
                InlineKeyboardButton("x2", callback_data=f"setmult|{tx_id}|2.0"),
                InlineKeyboardButton("x3", callback_data=f"setmult|{tx_id}|3.0"),
            ],
            [
                InlineKeyboardButton("x4", callback_data=f"setmult|{tx_id}|4.0"),
                InlineKeyboardButton("x5", callback_data=f"setmult|{tx_id}|5.0"),
            ]
        ])
        await query.edit_message_text(
            text=f"{texto_original}\n\n👇 **Selecciona el nuevo multiplicador:**",
            reply_markup=keyboard,
            parse_mode="Markdown"
        )

    # --- B. LÓGICA SECUENCIAL DE CONFIGURACIÓN ---
    
    # Paso 1: GUARDAR MULTIPLICADOR y PREGUNTAR CATEGORÍA
    elif accion == "cfg" and data[2] == "mult":
        tx_id = int(data[1])
        valor = float(data[3])
        
        # 1. Guardar el multiplicador
        if db.complete_configuration(transaction_id=tx_id, multiplier=valor):
            confirm_mult = f"✅ Regla: **x{valor}**"
            
            # 2. Generar SOLO los botones de Categoría para el siguiente paso
            category_keyboard = InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("Comida", callback_data=f"cfg|{tx_id}|cat|Comida"),
                    InlineKeyboardButton("Transporte", callback_data=f"cfg|{tx_id}|cat|Transporte"),
                    InlineKeyboardButton("Super", callback_data=f"cfg|{tx_id}|cat|Supermercado")
                ],
                [
                    InlineKeyboardButton("Servicios", callback_data=f"cfg|{tx_id}|cat|Servicios"),
                    InlineKeyboardButton("General", callback_data=f"cfg|{tx_id}|cat|General")
                ]
            ])

            # 3. Editar el mensaje, mostrando el multiplicador y pidiendo la categoría
            final_text = f"{base_text}\n\n{confirm_mult}\n\n👇 **Ahora elige la Categoría:**"
            await query.edit_message_text(
                text=final_text,
                reply_markup=category_keyboard,
                parse_mode="Markdown"
            )
        else:
            await query.edit_message_text(text="❌ Error al guardar el multiplicador.", parse_mode="Markdown")

    # Paso 2: GUARDAR CATEGORÍA y FINALIZAR
    elif accion == "cfg" and data[2] == "cat":
        tx_id = int(data[1])
        nombre_categoria = data[3]
        
        # 1. Guardar la categoría
        if db.complete_configuration(transaction_id=tx_id, category_name=nombre_categoria):
            
            # 2. Recuperar la línea de confirmación del Multiplicador del texto anterior
            mult_confirmation_line = [line for line in texto_original.split('\n') if line.startswith('✅ Regla:')][0]
            confirm_cat = f"✅ Categoría: **{nombre_categoria}**"
            
            # 3. Crear el mensaje final con ambas confirmaciones
            final_text = f"{base_text}\n\n{mult_confirmation_line}\n{confirm_cat}"

            await query.edit_message_text(
                text=final_text,
                reply_markup=None, # Quitar botones (Configuración completa)
                parse_mode="Markdown"
            )
        else:
            await query.edit_message_text(text="❌ Error al guardar la categoría.", parse_mode="Markdown")

    # --- C. GUARDAR CAMBIOS (setmult en edición manual) ---
    elif accion == "setmult":
        tx_id = int(data[1])
        valor = float(data[2])
        if db.complete_configuration(transaction_id=tx_id, multiplier=valor):
            await query.edit_message_text(f"✅ **Actualizado:** Ahora es x{valor}", parse_mode="Markdown")
        else:
            await query.edit_message_text("❌ Error al actualizar.")

def main():
    if not TOKEN:
        log.error("No hay TELEGRAM_TOKEN en .env")
        return

    app = ApplicationBuilder().token(TOKEN).build()

    # 1. Conversation Handler
    conv_handler = ConversationHandler(
        entry_points=[CommandHandler("registro", registro_start)],
        states={
            ASK_EMAIL: [MessageHandler(filters.TEXT & ~filters.COMMAND, receive_email)],
            ASK_PASSWORD: [MessageHandler(filters.TEXT & ~filters.COMMAND, receive_password)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )

    # 2. Agregar Handlers
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("recientes", recientes))
    app.add_handler(CommandHandler("tarjetas", tarjetas))
    app.add_handler(CommandHandler("resumen", resumen))
    
    app.add_handler(conv_handler)
    app.add_handler(CallbackQueryHandler(button_callback)) # Botones

    log.info("Bot de Telegram iniciado y escuchando...")
    app.run_polling()

if __name__ == "__main__":
    main()