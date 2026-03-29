import os
import logging
import html
import traceback
from datetime import datetime
from dotenv import load_dotenv
from telegram import Update, ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, filters, ContextTypes, ConversationHandler
from telegram.request import HTTPXRequest
from sqlalchemy.exc import IntegrityError
from db_models import Usuario, Producto, Key, Compra, get_session, inicializar_db 

# =================================================================
# 1. Configuración Inicial (Lectura de Variables de Entorno)
# =================================================================

load_dotenv() 
ADMIN_TOKEN_STR = os.getenv('BOT_ADMIN_TOKEN')
if not ADMIN_TOKEN_STR:
    raise ValueError("Error: BOT_ADMIN_TOKEN no encontrado. Verifica las variables de entorno.")

# Inicializa la base de datos (se hace después de cargar ENV)
inicializar_db() 

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Estados para ConversationHandlers ---
ADJUST_USER_ID, ADJUST_AMOUNT = range(2)
ADD_KEYS_PRODUCT, ADD_KEYS_LICENSES = range(2, 4)
CREATE_USER_NAME, CREATE_USER_LOGIN_KEY, CREATE_USER_SALDO, CREATE_USER_ADMIN, CREATE_USER_PLAN = range(4, 9)
CREATE_PRODUCT_NAME, CREATE_PRODUCT_CATEGORY, CREATE_PRODUCT_PRICE, CREATE_PRODUCT_DESC = range(9, 13)
DELETE_PRODUCT_ID = 13
SALE_USER_ID, SALE_CREDITS, SALE_VENDEDOR, SALE_TIPO, SALE_PLAN, SALE_ESTADO, SALE_DETALLE = range(14, 21)

PLAN_OPTIONS = ["FREE", "STANDAR", "VIP", "GOLD", "DIAMOND"]
SALE_CREDIT_OPTIONS = ["5", "10", "20", "50", "100", "200"]
SALE_VENDEDOR_OPTIONS = ["Torres", "Admin", "Soporte"]
SALE_TIPO_OPTIONS = ["CRÉDITOS", "PLAN", "BONO"]
SALE_ESTADO_OPTIONS = ["APROBADO", "PENDIENTE", "ANULADO"]
SALE_DETALLE_OPTIONS = ["Recarga manual", "Compra Telegram", "Sin detalle"]
ADJUST_AMOUNT_OPTIONS = ["+5", "+10", "+20", "+50", "-5", "-10", "-20", "-50"]
CREATE_USER_SALDO_OPTIONS = ["0", "5", "10", "20", "50", "100"]
MAX_SOCIOS_LIST_ROWS = 200


# =================================================================
# 2. Seguridad y Login de Administradores
# =================================================================

def check_admin(update: Update) -> bool:
    """Verifica si el usuario está logueado y tiene permisos de administrador."""
    if not update.effective_user:
        return False
    try:
        user_id_telegram = update.effective_user.id
        with get_session() as session_db:
            usuario = session_db.query(Usuario).filter_by(
                telegram_id=user_id_telegram,
                es_admin=True
            ).first()
        return usuario is not None
    except Exception as e:
        logger.error(f"Error en check_admin: {e}")
        return False

async def admin_login_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Permite a un usuario administrador loguearse en el bot."""
    
    text = update.message.text
    parts = text.split()
    
    if len(parts) != 3 or parts[0].lower() != '/login':
        await update.message.reply_text(
            "❌ Formato incorrecto. Uso: `/login USUARIO CLAVE`",
            parse_mode='Markdown'
        )
        return ConversationHandler.END

    username, login_key_input = parts[1], parts[2]
    user_id_telegram = update.effective_user.id

    session_db = get_session()
    try:
        usuario = session_db.query(Usuario).filter_by(
            username=username, 
            login_key=login_key_input, 
            es_admin=True
        ).first()

        if usuario:
            existing_user_with_id = session_db.query(Usuario).filter(
                Usuario.telegram_id == user_id_telegram, 
                Usuario.id != usuario.id
            ).first()
            
            if existing_user_with_id:
                await update.message.reply_text(
                    f"❌ Error: Tu ID de Telegram ya está asociada a la cuenta '{existing_user_with_id.username}'. Desloguea esa cuenta primero si es necesario."
                )
                return ConversationHandler.END

            usuario.telegram_id = user_id_telegram
            session_db.commit()

            await update.message.reply_text(
                f"✅ **¡Bienvenido, {usuario.username}!** Eres administrador.\n"
                "Usa /start para acceder al panel.",
                parse_mode='Markdown',
                reply_markup=get_admin_keyboard()
            )
            return ConversationHandler.END
        else:
            await update.message.reply_text(
                "❌ Login fallido. Credenciales incorrectas o el usuario no es administrador."
            )
            return ConversationHandler.END
    except Exception as e:
        logger.error(f"Error en login de administrador: {e}")
        session_db.rollback()
        await update.message.reply_text("Ha ocurrido un error inesperado durante el login.")
        return ConversationHandler.END
    finally:
        session_db.close()

def get_admin_keyboard():
    """Genera el teclado principal de administración."""
    keyboard = [
        [KeyboardButton("💰 Ajustar Saldo"), KeyboardButton("🧾 Registrar Compra"), KeyboardButton("👤 Listar Socios")]
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True, one_time_keyboard=False)


def _generate_sale_number() -> str:
    return f"VTA-{datetime.now().strftime('%Y%m%d%H%M%S%f')}"


def _sale_inline_keyboard_users(usuarios: list[Usuario]) -> InlineKeyboardMarkup:
    rows = []
    for usuario in usuarios:
        rows.append([InlineKeyboardButton(f"ID {usuario.id} | {usuario.username}", callback_data=f"sale:user:{usuario.id}")])
    rows.append([InlineKeyboardButton("Cancelar", callback_data="sale:cancel")])
    return InlineKeyboardMarkup(rows)


def _sale_inline_cancel_only() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("Cancelar", callback_data="sale:cancel")]])


async def _sale_edit_prompt(context: ContextTypes.DEFAULT_TYPE, text: str, reply_markup: InlineKeyboardMarkup | None) -> None:
    chat_id = context.user_data.get('sale_flow_chat_id')
    message_id = context.user_data.get('sale_flow_message_id')
    if not chat_id or not message_id:
        return
    await context.bot.edit_message_text(
        chat_id=chat_id,
        message_id=message_id,
        text=text,
        parse_mode='Markdown',
        reply_markup=reply_markup,
    )


async def sale_callback_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    return await cancel_conversation(update, context)


def _sale_inline_keyboard_options(prefix: str, options: list[str]) -> InlineKeyboardMarkup:
    rows = []
    for idx, option in enumerate(options):
        rows.append([InlineKeyboardButton(option, callback_data=f"sale:{prefix}:{idx}")])
    rows.append([InlineKeyboardButton("Cancelar", callback_data="sale:cancel")])
    return InlineKeyboardMarkup(rows)

async def cancel_conversation(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Cancela el flujo actual y vuelve al menú principal."""
    if not check_admin(update): return ConversationHandler.END
    target_message = update.message or (update.callback_query.message if update.callback_query else None)
    if target_message:
        await target_message.reply_text("Operación cancelada. Volviendo al menú principal.", reply_markup=get_admin_keyboard())
    context.user_data.clear()
    return ConversationHandler.END


async def prompt_register_sale(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not check_admin(update):
        return ConversationHandler.END

    context.user_data['sale_user_id'] = None
    context.user_data['sale_credits'] = None
    context.user_data['sale_vendedor'] = None
    context.user_data['sale_tipo'] = None
    context.user_data['sale_plan'] = None
    context.user_data['sale_estado'] = None

    await update.message.reply_text(
        "🧾 *Registrar Compra*\n\nEscribe el *ID* del usuario:",
        parse_mode='Markdown',
        reply_markup=ReplyKeyboardMarkup([[KeyboardButton("Cancelar")]], resize_keyboard=True)
    )
    return SALE_USER_ID


async def sale_select_user(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    text = (update.message.text or "").strip()
    if text.lower() == "cancelar":
        return await cancel_conversation(update, context)

    try:
        user_id = int(text)
    except ValueError:
        await update.message.reply_text("❌ Escribe solo el número del ID (ej: `12`).", parse_mode='Markdown')
        return SALE_USER_ID

    with get_session() as session_db:
        usuario = (
            session_db.query(Usuario).filter_by(id=user_id).first()
            or session_db.query(Usuario).filter_by(telegram_id=user_id).first()
        )

    if not usuario:
        await update.message.reply_text(f"❌ No existe ningún usuario con ID `{user_id}`.\nPuedes usar el ID interno (ej: 3) o el Telegram ID.", parse_mode='Markdown')
        return SALE_USER_ID

    context.user_data['sale_user_id'] = usuario.id

    flow_message = await update.message.reply_text(
        f"✅ Usuario encontrado:\n"
        f"• ID: `{usuario.id}`\n"
        f"• Usuario: *{usuario.username}*\n"
        f"• Saldo actual: `${usuario.saldo:.2f}`\n"
        f"• Plan: `{(usuario.plan or 'FREE').upper()}`\n\n"
        "Escribe la *cantidad de créditos* a agregar (ej: 50):",
        parse_mode='Markdown',
        reply_markup=ReplyKeyboardMarkup([[KeyboardButton("Cancelar")]], resize_keyboard=True)
    )
    context.user_data['sale_flow_chat_id'] = flow_message.chat_id
    context.user_data['sale_flow_message_id'] = flow_message.message_id
    return SALE_CREDITS


async def sale_get_credits(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    text = (update.message.text or "").strip()
    if text == "Cancelar":
        return await cancel_conversation(update, context)

    try:
        credits = float(text)
        if credits <= 0:
            raise ValueError
    except ValueError:
        await update.message.reply_text("❌ Cantidad no válida. Escribe un número mayor a 0 (ej: 50).")
        return SALE_CREDITS

    context.user_data['sale_credits'] = credits
    await update.message.reply_text(
        "Selecciona el *vendedor*:",
        parse_mode='Markdown',
        reply_markup=_sale_inline_keyboard_options("vend", SALE_VENDEDOR_OPTIONS)
    )
    return SALE_VENDEDOR


async def sale_get_vendedor(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()

    data = query.data or ""
    if data == "sale:cancel":
        return await cancel_conversation(update, context)

    try:
        idx = int(data.split(":")[-1])
        vendedor = SALE_VENDEDOR_OPTIONS[idx]
    except Exception:
        await query.message.reply_text("❌ Vendedor no válido. Intenta nuevamente.")
        return SALE_VENDEDOR

    context.user_data['sale_vendedor'] = vendedor
    await query.message.reply_text(
        "Selecciona el *tipo*:",
        parse_mode='Markdown',
        reply_markup=_sale_inline_keyboard_options("tipo", SALE_TIPO_OPTIONS)
    )
    return SALE_TIPO


async def sale_get_tipo(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()

    data = query.data or ""
    if data == "sale:cancel":
        return await cancel_conversation(update, context)

    try:
        idx = int(data.split(":")[-1])
        tipo = SALE_TIPO_OPTIONS[idx]
    except Exception:
        tipo = "CRÉDITOS"

    context.user_data['sale_tipo'] = tipo
    await query.message.reply_text(
        "Selecciona el *plan* agregado en esta compra:",
        parse_mode='Markdown',
        reply_markup=_sale_inline_keyboard_options("plan", PLAN_OPTIONS)
    )
    return SALE_PLAN


async def sale_get_plan(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()

    data = query.data or ""
    if data == "sale:cancel":
        return await cancel_conversation(update, context)

    try:
        idx = int(data.split(":")[-1])
        plan = PLAN_OPTIONS[idx]
    except Exception:
        plan = "FREE"

    context.user_data['sale_plan'] = plan
    await query.message.reply_text(
        "Selecciona el *estado*:",
        parse_mode='Markdown',
        reply_markup=_sale_inline_keyboard_options("estado", SALE_ESTADO_OPTIONS)
    )
    return SALE_ESTADO


async def sale_get_estado(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()

    data = query.data or ""
    if data == "sale:cancel":
        return await cancel_conversation(update, context)

    try:
        idx = int(data.split(":")[-1])
        estado = SALE_ESTADO_OPTIONS[idx]
    except Exception:
        estado = "APROBADO"

    context.user_data['sale_estado'] = estado
    await query.message.reply_text(
        "Selecciona el *detalle* de la venta:",
        parse_mode='Markdown',
        reply_markup=_sale_inline_keyboard_options("detalle", SALE_DETALLE_OPTIONS)
    )
    return SALE_DETALLE


async def sale_finish(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_id = context.user_data.get('sale_user_id')
    credits = context.user_data.get('sale_credits')
    vendedor = context.user_data.get('sale_vendedor', 'ADMIN')
    tipo = context.user_data.get('sale_tipo', 'CRÉDITOS')
    plan = (context.user_data.get('sale_plan') or 'FREE').upper()
    estado = context.user_data.get('sale_estado', 'APROBADO')
    query = update.callback_query
    await query.answer()

    data = query.data or ""
    if data == "sale:cancel":
        return await cancel_conversation(update, context)
    try:
        idx = int(data.split(":")[-1])
        detalle_raw = SALE_DETALLE_OPTIONS[idx]
    except Exception:
        detalle_raw = 'Sin detalle'

    detalle = '' if detalle_raw == '-' or detalle_raw == 'Sin detalle' else detalle_raw

    if not user_id or credits is None:
        return await cancel_conversation(update, context)

    db_session = get_session()
    try:
        usuario = db_session.query(Usuario).filter_by(id=user_id).first()
        if not usuario:
            await query.message.reply_text("❌ Usuario no encontrado al finalizar la venta.", reply_markup=get_admin_keyboard())
            return ConversationHandler.END

        nro_venta = _generate_sale_number()
        compra = Compra(
            nro_venta=nro_venta,
            usuario_id=usuario.id,
            estado=estado,
            vendedor=vendedor,
            tipo=tipo,
            plan=plan,
            cantidad=float(credits),
            recompensa='-',
            detalle=detalle,
        )
        usuario.saldo += float(credits)
        usuario.plan = plan

        db_session.add(compra)
        db_session.commit()

        await query.message.reply_text(
            "✅ Compra registrada y créditos agregados correctamente.\n"
            f"NRO VENTA: `{nro_venta}`\n"
            f"USUARIO: **{usuario.username}**\n"
            f"PLAN APLICADO: `{plan}`\n"
            f"CREDITOS AGREGADOS: `{credits:.2f}`\n"
            f"NUEVO SALDO: `{usuario.saldo:.2f}`",
            parse_mode='Markdown',
            reply_markup=get_admin_keyboard()
        )
    except Exception as e:
        logger.error(f"Error al registrar compra: {e}")
        db_session.rollback()
        await query.message.reply_text("❌ Error al registrar la compra. Usa /cancelar.", reply_markup=get_admin_keyboard())
    finally:
        db_session.close()

    context.user_data.clear()
    return ConversationHandler.END

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Muestra el menú principal si es el administrador."""
    if not check_admin(update):
        if update.message:
            await update.message.reply_text(
                "❌ Acceso denegado.\n"
                "Usa `/login USUARIO CLAVE` para iniciar sesión.",
                parse_mode='Markdown'
            )
        return ConversationHandler.END

    await update.message.reply_text(
        "👋 **Panel de Administración**\nElige una opción:",
        parse_mode='Markdown',
        reply_markup=get_admin_keyboard()
    )
    return ConversationHandler.END 


# =================================================================
# 3. Gestión de Socios
# =================================================================

async def list_users(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Muestra la lista de usuarios y su información administrativa."""
    if not check_admin(update): return

    with get_session() as session_db:
        usuarios = (
            session_db.query(Usuario)
            .order_by(Usuario.id.desc())
            .limit(MAX_SOCIOS_LIST_ROWS + 1)
            .all()
        )

    if not usuarios:
        await update.message.reply_text("No hay socios registrados.", reply_markup=get_admin_keyboard())
        return

    truncated = len(usuarios) > MAX_SOCIOS_LIST_ROWS
    usuarios = usuarios[:MAX_SOCIOS_LIST_ROWS]

    header = f"👤 <b>Socios registrados</b> (mostrando <code>{len(usuarios)}</code>)\n\n"
    if truncated:
        header += (
            f"⚠️ Lista limitada a <code>{MAX_SOCIOS_LIST_ROWS}</code> socios más recientes para evitar saturación.\n\n"
        )

    chunks: list[str] = []
    current_chunk = header
    max_chunk_len = 3600

    for u in usuarios:
        rol = "ADMIN" if u.es_admin else "CLIENTE"
        alias = u.username or "-"
        telegram_id = str(u.telegram_id) if u.telegram_id is not None else "-"
        fecha_registro = u.fecha_registro.strftime("%Y-%m-%d %H:%M:%S") if u.fecha_registro else "-"
        plan = (u.plan or "FREE").upper()
        estado = (u.estado or "ACTIVO").upper()
        username_safe = html.escape(u.username or "-")
        alias_safe = html.escape(alias)
        key_safe = html.escape(u.login_key or "-")

        row = (
            f"ID: <code>{u.id}</code>\n"
            f"USUARIO: <b>{username_safe}</b>\n"
            f"ALIAS: <code>{alias_safe}</code>\n"
            f"TELEGRAM ID: <code>{telegram_id}</code>\n"
            f"CREDITOS: <code>{u.saldo:.2f}</code>\n"
            f"PLAN: <code>{plan}</code>\n"
            f"ESTADO: <code>{estado}</code>\n"
            f"ROL: <code>{rol}</code>\n"
            f"KEY: <code>{key_safe}</code>\n"
            f"REGISTRO: <code>{fecha_registro}</code>\n"
            "----------------------------------\n"
        )

        if len(current_chunk) + len(row) > max_chunk_len:
            chunks.append(current_chunk)
            current_chunk = row
        else:
            current_chunk += row

    if current_chunk.strip():
        chunks.append(current_chunk)

    for idx, chunk in enumerate(chunks):
        await update.message.reply_text(
            chunk,
            parse_mode='HTML',
            reply_markup=get_admin_keyboard() if idx == len(chunks) - 1 else None,
        )

# Flujo: ➕ Crear Socio
async def prompt_create_user_name(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not check_admin(update): return ConversationHandler.END
    await update.message.reply_text("Ingresa el **Username** para el nuevo socio:", parse_mode='Markdown', reply_markup=ReplyKeyboardRemove())
    return CREATE_USER_NAME

async def get_create_user_name(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data['temp_username'] = update.message.text.strip()
    await update.message.reply_text("Ingresa la **Login Key/Contraseña** para el socio:", parse_mode='Markdown')
    return CREATE_USER_LOGIN_KEY

async def get_create_user_login_key(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data['temp_login_key'] = update.message.text.strip()
    saldo_rows = [
        [KeyboardButton(CREATE_USER_SALDO_OPTIONS[0]), KeyboardButton(CREATE_USER_SALDO_OPTIONS[1]), KeyboardButton(CREATE_USER_SALDO_OPTIONS[2])],
        [KeyboardButton(CREATE_USER_SALDO_OPTIONS[3]), KeyboardButton(CREATE_USER_SALDO_OPTIONS[4]), KeyboardButton(CREATE_USER_SALDO_OPTIONS[5])],
        [KeyboardButton("Cancelar")],
    ]
    await update.message.reply_text(
        "Selecciona el **Saldo Inicial ($)**:",
        parse_mode='Markdown',
        reply_markup=ReplyKeyboardMarkup(saldo_rows, resize_keyboard=True, one_time_keyboard=False)
    )
    return CREATE_USER_SALDO

async def get_create_user_saldo(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if (update.message.text or "").strip() == "Cancelar":
        return await cancel_conversation(update, context)

    try:
        saldo = float(update.message.text)
        context.user_data['temp_saldo'] = saldo
        
        keyboard = [[KeyboardButton("Sí"), KeyboardButton("No")]]
        reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)
        
        await update.message.reply_text("¿Será este socio un administrador? (Sí/No):", reply_markup=reply_markup)
        return CREATE_USER_ADMIN
    except ValueError:
        await update.message.reply_text("❌ Saldo no válido. Selecciona una opción de los botones.")
        return CREATE_USER_SALDO

async def get_create_user_admin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    is_admin = (update.message.text or "").strip().lower() == 'sí'
    context.user_data['temp_is_admin'] = is_admin

    plan_rows = [[KeyboardButton(plan)] for plan in PLAN_OPTIONS]
    plan_rows.append([KeyboardButton("Cancelar")])
    await update.message.reply_text(
        "Selecciona el **plan** del usuario:",
        parse_mode='Markdown',
        reply_markup=ReplyKeyboardMarkup(plan_rows, resize_keyboard=True, one_time_keyboard=False)
    )
    return CREATE_USER_PLAN


async def finish_create_user(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    plan = (update.message.text or "").strip().upper()
    if plan == "CANCELAR":
        return await cancel_conversation(update, context)
    if plan not in PLAN_OPTIONS:
        await update.message.reply_text("❌ Selecciona un plan válido usando los botones.")
        return CREATE_USER_PLAN

    db_session = get_session()
    try:
        nuevo_usuario = Usuario(
            username=context.user_data['temp_username'],
            login_key=context.user_data['temp_login_key'],
            saldo=context.user_data['temp_saldo'],
            es_admin=bool(context.user_data.get('temp_is_admin', False)),
            plan=plan,
            estado='ACTIVO',
        )
        db_session.add(nuevo_usuario)
        db_session.commit()
        
        await update.message.reply_text(
            f"✅ Socio **{nuevo_usuario.username}** creado exitosamente:\n"
            f"Key: `{nuevo_usuario.login_key}` | Saldo: `${nuevo_usuario.saldo:.2f}`\n"
            f"Plan: `{nuevo_usuario.plan}` | Estado: `{nuevo_usuario.estado}`", 
            parse_mode='Markdown', 
            reply_markup=get_admin_keyboard()
        )
    except IntegrityError:
        db_session.rollback()
        await update.message.reply_text("❌ Error: Ya existe un socio con ese nombre de usuario. Usa /cancelar.", reply_markup=get_admin_keyboard())
    except Exception as e:
        logger.error(f"Error al crear socio: {e}")
        db_session.rollback()
        await update.message.reply_text("❌ Error al guardar el socio. Usa /cancelar.", reply_markup=get_admin_keyboard())
    finally:
        db_session.close()
    
    context.user_data.clear()
    return ConversationHandler.END

# Flujo: 💰 Ajustar Saldo
async def prompt_adjust_saldo(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not check_admin(update): return ConversationHandler.END

    await update.message.reply_text(
        "💰 *Ajustar Saldo*\n\nEscribe el *ID* del usuario:",
        parse_mode='Markdown',
        reply_markup=ReplyKeyboardMarkup([[KeyboardButton("Cancelar")]], resize_keyboard=True)
    )
    return ADJUST_USER_ID

async def select_user_id(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    text = (update.message.text or "").strip()
    if text.lower() == "cancelar":
        return await cancel_conversation(update, context)

    try:
        user_id = int(text)
    except ValueError:
        await update.message.reply_text("❌ Escribe solo el número del ID (ej: `12`).", parse_mode='Markdown')
        return ADJUST_USER_ID

    with get_session() as session_db:
        usuario = (
            session_db.query(Usuario).filter_by(id=user_id).first()
            or session_db.query(Usuario).filter_by(telegram_id=user_id).first()
        )

    if not usuario:
        await update.message.reply_text(f"❌ No existe ningún usuario con ID `{user_id}`.\nPuedes usar el ID interno (ej: 3) o el Telegram ID.", parse_mode='Markdown')
        return ADJUST_USER_ID

    context.user_data['user_to_adjust_id'] = usuario.id
    amount_rows = [
        [KeyboardButton(ADJUST_AMOUNT_OPTIONS[0]), KeyboardButton(ADJUST_AMOUNT_OPTIONS[1]), KeyboardButton(ADJUST_AMOUNT_OPTIONS[2]), KeyboardButton(ADJUST_AMOUNT_OPTIONS[3])],
        [KeyboardButton(ADJUST_AMOUNT_OPTIONS[4]), KeyboardButton(ADJUST_AMOUNT_OPTIONS[5]), KeyboardButton(ADJUST_AMOUNT_OPTIONS[6]), KeyboardButton(ADJUST_AMOUNT_OPTIONS[7])],
        [KeyboardButton("Cancelar")],
    ]

    await update.message.reply_text(
        f"✅ Usuario encontrado:\n"
        f"• ID: `{usuario.id}`\n"
        f"• Usuario: *{usuario.username}*\n"
        f"• Saldo actual: `${usuario.saldo:.2f}`\n"
        f"• Plan: `{(usuario.plan or 'FREE').upper()}`\n\n"
        "Selecciona el *monto a ajustar*:",
        parse_mode='Markdown',
        reply_markup=ReplyKeyboardMarkup(amount_rows, resize_keyboard=True, one_time_keyboard=False)
    )
    return ADJUST_AMOUNT

async def adjust_saldo_final(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    text = (update.message.text or "").strip()
    if text == "Cancelar":
        return await cancel_conversation(update, context)

    try:
        monto = float(text)
        user_id = context.user_data.get('user_to_adjust_id')
        
        if not user_id: return await cancel_conversation(update, context)

        with get_session() as session_db:
            usuario = session_db.query(Usuario).filter_by(id=user_id).first()
            
            if usuario:
                usuario.saldo += monto
                session_db.commit()
                
                await update.message.reply_text(
                    f"✅ Saldo de **{usuario.username}** ajustado.\n"
                    f"Monto aplicado: **${monto:.2f}**\n"
                    f"Nuevo saldo: **${usuario.saldo:.2f}**",
                    parse_mode='Markdown',
                    reply_markup=get_admin_keyboard()
                )
    
    except ValueError:
        await update.message.reply_text("❌ Monto no válido. Selecciona un valor usando los botones.")
        return ADJUST_AMOUNT
    except Exception as e:
        logger.error(f"Error al ajustar saldo: {e}")
        await update.message.reply_text("❌ Error inesperado. Usa /start para volver.", reply_markup=get_admin_keyboard())

    context.user_data.clear()
    return ConversationHandler.END


# =================================================================
# 4. Gestión de Productos/Keys
# =================================================================

async def manage_products_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Muestra la lista de productos y un menú de acciones."""
    if not check_admin(update): return

    with get_session() as session_db:
        productos = session_db.query(Producto).all()
        message = "**Catálogo de Productos (ID | Nombre | Stock):**\n\n"
        if not productos:
            message += "No hay productos registrados. Usa '➕ Crear Producto'."
        else:
            for p in productos:
                stock_available = session_db.query(Key).filter(Key.producto_id == p.id, Key.estado == 'available').count()
                message += (
                    f"ID: `{p.id}` | **{p.nombre}** (${p.precio:.2f})\n"
                    f"   Stock: **{stock_available}**\n"
                    "----------------------------------\n"
                )
    
    keyboard = [
        [KeyboardButton("➕ Crear Producto")],
        [KeyboardButton("🔑 Añadir Keys"), KeyboardButton("🗑️ Eliminar Producto")], 
        [KeyboardButton("Go back")] 
    ]
    reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True, one_time_keyboard=False)

    await update.message.reply_text(
        message + "\nElige una acción:",
        parse_mode='Markdown',
        reply_markup=reply_markup
    )
    
# Flujo: ➕ Crear Producto
async def prompt_create_product(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not check_admin(update): return ConversationHandler.END
    await update.message.reply_text("Ingresa el **Nombre del Producto**:", parse_mode='Markdown', reply_markup=ReplyKeyboardRemove())
    return CREATE_PRODUCT_NAME

async def get_product_name(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data['temp_nombre'] = update.message.text
    await update.message.reply_text("Ingresa la **Categoría**:", parse_mode='Markdown')
    return CREATE_PRODUCT_CATEGORY

async def get_product_category(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data['temp_categoria'] = update.message.text
    await update.message.reply_text("Ingresa el **Precio ($)** (ej: 10.00):", parse_mode='Markdown')
    return CREATE_PRODUCT_PRICE

async def get_product_price(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    try:
        price = float(update.message.text)
        context.user_data['temp_precio'] = price
        await update.message.reply_text("Ingresa la **Descripción** (opcional, /skip para omitir):", parse_mode='Markdown')
        return CREATE_PRODUCT_DESC
    except ValueError:
        await update.message.reply_text("❌ Precio no válido. Ingresa un número (ej: 10.00).")
        return CREATE_PRODUCT_PRICE

async def finish_create_product(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    desc = update.message.text if update.message.text and update.message.text != "/skip" else ""
    
    db_session = get_session()
    try:
        nuevo_producto = Producto(
            nombre=context.user_data['temp_nombre'],
            categoria=context.user_data['temp_categoria'],
            precio=context.user_data['temp_precio'],
            descripcion=desc
        )
        db_session.add(nuevo_producto)
        db_session.commit()
        
        await update.message.reply_text(
            f"✅ Producto **{nuevo_producto.nombre}** (ID: {nuevo_producto.id}) creado exitosamente.", 
            parse_mode='Markdown', 
            reply_markup=get_admin_keyboard()
        )
    except Exception as e:
        logger.error(f"Error al crear producto: {e}")
        db_session.rollback()
        await update.message.reply_text("❌ Error al guardar el producto en la DB. Usa /cancelar.", reply_markup=get_admin_keyboard())
    finally:
        db_session.close()
    
    context.user_data.clear()
    return ConversationHandler.END


# Flujo: 🗑️ Eliminar Producto
async def prompt_delete_product(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not check_admin(update): return ConversationHandler.END
    
    await update.message.reply_text(
        "**ADVERTENCIA:** Esto eliminará el producto y TODAS las keys asociadas.\n"
        "Ingresa el **ID** del Producto a eliminar:",
        parse_mode='Markdown',
        reply_markup=ReplyKeyboardRemove()
    )
    return DELETE_PRODUCT_ID

async def process_delete_product(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    try:
        product_id = int(update.message.text)
    except ValueError:
        await update.message.reply_text("❌ ID no válido. Ingresa el número ID del producto.")
        return DELETE_PRODUCT_ID

    db_session = get_session()
    try:
        producto = db_session.query(Producto).filter_by(id=product_id).first()
        if not producto:
            await update.message.reply_text("❌ Producto no encontrado. Ingresa un ID válido.")
            return DELETE_PRODUCT_ID

        db_session.query(Key).filter_by(producto_id=product_id).delete()
        db_session.delete(producto)
        db_session.commit()

        await update.message.reply_text(
            f"✅ Producto **{producto.nombre}** y sus keys eliminados con éxito.",
            parse_mode='Markdown',
            reply_markup=get_admin_keyboard()
        )
    except Exception as e:
        logger.error(f"Error al eliminar producto: {e}")
        db_session.rollback()
        await update.message.reply_text("❌ Error inesperado al eliminar. Usa /cancelar.", reply_markup=get_admin_keyboard())
    finally:
        db_session.close()
    
    return ConversationHandler.END

# Flujo: 🔑 Añadir Keys
async def show_key_management_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not check_admin(update): return ConversationHandler.END
    
    with get_session() as session_db:
        productos = session_db.query(Producto).all() 

    if not productos:
        await update.message.reply_text("❌ No hay productos registrados. Usa '➕ Crear Producto'.", reply_markup=get_admin_keyboard())
        return ConversationHandler.END
    
    keyboard_rows = []
    message = "**Productos disponibles para añadir Keys:**\n\n"
    for p in productos:
        with get_session() as s:
            stock = s.query(Key).filter(Key.producto_id == p.id, Key.estado == 'available').count()
        message += f"ID: `{p.id}` | **{p.nombre}** - Stock: {stock}\n"
        keyboard_rows.append([KeyboardButton(f"ID {p.id}: {p.nombre}")])

    keyboard_rows.append([KeyboardButton("Back to Admin Menu")])
    reply_markup = ReplyKeyboardMarkup(keyboard_rows, resize_keyboard=True, one_time_keyboard=False)

    await update.message.reply_text(
        f"{message}\n\nSelecciona un producto o ingresa su ID para añadir Keys:",
        parse_mode='Markdown',
        reply_markup=reply_markup
    )
    return ADD_KEYS_PRODUCT

async def select_product_for_keys(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    text = update.message.text
    
    if text == "Back to Admin Menu":
        return await start(update, context) 

    try:
        product_id = int(text.split(':')[0].replace('ID', '').strip().split()[0])
    except Exception:
        await update.message.reply_text("❌ Opción no válida. Ingresa el ID numérico del producto.")
        return ADD_KEYS_PRODUCT

    with get_session() as session_db:
        producto = session_db.query(Producto).filter_by(id=product_id).first()
        
    if not producto:
        await update.message.reply_text("❌ Producto no encontrado. Ingresa un ID válido.")
        return ADD_KEYS_PRODUCT
    
    context.user_data['product_to_add_keys_id'] = product_id
    context.user_data['product_to_add_keys_name'] = producto.nombre

    await update.message.reply_text(
        f"Producto seleccionado: **{producto.nombre}**\n\n"
        "Ahora, **pega las licencias/keys, una por línea**.",
        parse_mode='Markdown',
        reply_markup=ReplyKeyboardRemove()
    )
    return ADD_KEYS_LICENSES

async def process_add_licenses(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    licencias_raw = update.message.text
    product_id = context.user_data.get('product_to_add_keys_id')
    product_name = context.user_data.get('product_to_add_keys_name')
    
    if not product_id:
        return await cancel_conversation(update, context)

    if not licencias_raw:
        await update.message.reply_text("❌ No ingresaste ninguna key.")
        return ADD_KEYS_LICENSES

    keys_list = [lic.strip() for lic in licencias_raw.splitlines() if lic.strip()]
    if not keys_list:
        await update.message.reply_text("❌ No se detectó ninguna key válida. Intenta de nuevo.")
        return ADD_KEYS_LICENSES
        
    added_keys = 0
    db_session = get_session() 
    try:
        for lic in keys_list:
            existing_key = db_session.query(Key).filter_by(licencia=lic).first()
            if not existing_key:
                nueva_key = Key(producto_id=product_id, licencia=lic, estado='available')
                db_session.add(nueva_key)
                added_keys += 1
            else:
                logger.warning(f"Key duplicada omitida: {lic}")
        
        db_session.commit()

        await update.message.reply_text(
            f"✅ Keys agregadas a **{product_name}**:\n"
            f"Se agregaron **{added_keys}** nuevas licencias.",
            parse_mode='Markdown',
            reply_markup=get_admin_keyboard()
        )
    except Exception as e:
        logger.error(f"Error al añadir keys: {e}")
        db_session.rollback()
        await update.message.reply_text("❌ Error al guardar las keys. Usa /cancelar.", reply_markup=get_admin_keyboard())
    finally:
        db_session.close()
    
    context.user_data.clear()
    return ConversationHandler.END

async def unknown(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if check_admin(update) and update.message: 
        await update.message.reply_text("Opción no reconocida. Usa los botones o /start para volver al menú principal.", reply_markup=get_admin_keyboard())

# =================================================================
# 5. Función Principal de Ejecución del Bot Administrador
# =================================================================

def main_admin() -> None:
    """Ejecuta el bot administrador."""
    application = (
        Application.builder()
        .token(ADMIN_TOKEN_STR)
        .request(HTTPXRequest(connect_timeout=20, read_timeout=20, write_timeout=20, pool_timeout=20))
        .build()
    )
    
    # LOGIN DE ADMINISTRADORES (maneja el comando /login)
    application.add_handler(CommandHandler("login", admin_login_prompt))
    
    # Handlers para comandos y botones simples
    application.add_handler(CommandHandler("start", start))
    application.add_handler(MessageHandler(filters.Regex("^Go back$") | filters.Regex("^Back to Admin Menu$"), start))
    application.add_handler(MessageHandler(filters.Regex("^👤 Listar Socios$"), list_users))
    application.add_handler(MessageHandler(filters.Regex("^📦 Gestión Productos$"), manage_products_menu))

    # Flujo de Registro de Compra/Recarga
    sale_conv_handler = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex(r"^(?:🧾\s*)?Registrar Compra\s*$"), prompt_register_sale)],
        states={
            SALE_USER_ID: [MessageHandler(filters.TEXT & ~filters.COMMAND, sale_select_user)],
            SALE_CREDITS: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, sale_get_credits),
                CallbackQueryHandler(sale_callback_cancel, pattern=r"^sale:cancel$")
            ],
            SALE_VENDEDOR: [CallbackQueryHandler(sale_get_vendedor, pattern=r"^sale:(vend:\d+|cancel)$")],
            SALE_TIPO: [CallbackQueryHandler(sale_get_tipo, pattern=r"^sale:(tipo:\d+|cancel)$")],
            SALE_PLAN: [CallbackQueryHandler(sale_get_plan, pattern=r"^sale:(plan:\d+|cancel)$")],
            SALE_ESTADO: [CallbackQueryHandler(sale_get_estado, pattern=r"^sale:(estado:\d+|cancel)$")],
            SALE_DETALLE: [CallbackQueryHandler(sale_finish, pattern=r"^sale:(detalle:\d+|cancel)$")],
        },
        fallbacks=[CommandHandler("cancelar", cancel_conversation), CommandHandler("start", start)],
        per_user=True
    )
    application.add_handler(sale_conv_handler)

    # Flujo de Ajuste de Saldo
    saldo_conv_handler = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex("^💰 Ajustar Saldo$"), prompt_adjust_saldo)],
        states={
            ADJUST_USER_ID: [MessageHandler(filters.TEXT & ~filters.COMMAND, select_user_id)],
            ADJUST_AMOUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, adjust_saldo_final)],
        },
        fallbacks=[CommandHandler("cancelar", cancel_conversation), CommandHandler("start", start)],
        per_user=True
    )
    application.add_handler(saldo_conv_handler)
    
    # Flujo de Creación de Socio (Usuario)
    create_user_conv_handler = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex("^➕ Crear Socio$"), prompt_create_user_name)],
        states={
            CREATE_USER_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_create_user_name)],
            CREATE_USER_LOGIN_KEY: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_create_user_login_key)],
            CREATE_USER_SALDO: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_create_user_saldo)],
            CREATE_USER_ADMIN: [MessageHandler(filters.Regex("^(Sí|No)$"), get_create_user_admin)],
            CREATE_USER_PLAN: [MessageHandler(filters.TEXT & ~filters.COMMAND, finish_create_user)],
        },
        fallbacks=[CommandHandler("cancelar", cancel_conversation), CommandHandler("start", start)],
        per_user=True
    )
    application.add_handler(create_user_conv_handler)

    # Flujo de Creación de Producto
    product_conv_handler = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex("^➕ Crear Producto$"), prompt_create_product)],
        states={
            CREATE_PRODUCT_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_product_name)],
            CREATE_PRODUCT_CATEGORY: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_product_category)],
            CREATE_PRODUCT_PRICE: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_product_price)],
            CREATE_PRODUCT_DESC: [MessageHandler(filters.TEXT & ~filters.COMMAND, finish_create_product), CommandHandler("skip", finish_create_product)],
        },
        fallbacks=[CommandHandler("cancelar", cancel_conversation), CommandHandler("start", start)],
        per_user=True
    )
    application.add_handler(product_conv_handler)
    
    # Flujo de Eliminar Producto
    delete_product_conv_handler = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex("^🗑️ Eliminar Producto$"), prompt_delete_product)],
        states={
            DELETE_PRODUCT_ID: [MessageHandler(filters.TEXT & ~filters.COMMAND, process_delete_product)],
        },
        fallbacks=[CommandHandler("cancelar", cancel_conversation), CommandHandler("start", start)],
        per_user=True
    )
    application.add_handler(delete_product_conv_handler)
    
    # Flujo de Añadir Keys
    keys_conv_handler = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex("^🔑 Añadir Keys$"), show_key_management_menu)],
        states={
            ADD_KEYS_PRODUCT: [MessageHandler(filters.TEXT & ~filters.COMMAND, select_product_for_keys)],
            ADD_KEYS_LICENSES: [MessageHandler(filters.TEXT & ~filters.COMMAND, process_add_licenses)],
        },
        fallbacks=[CommandHandler("cancelar", cancel_conversation), CommandHandler("start", start)],
        per_user=True
    )
    application.add_handler(keys_conv_handler)
    
    # Manejador general para texto no reconocido
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, unknown))

    async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
        logger.error("Excepción no capturada (admin):", exc_info=context.error)
        tb = "".join(traceback.format_exception(None, context.error, context.error.__traceback__))
        logger.error(f"Traceback:\n{tb}")

    application.add_error_handler(error_handler)
    logger.info("El Bot ADMINISTRADOR se está iniciando...")
    application.run_polling(
        allowed_updates=Update.ALL_TYPES,
        drop_pending_updates=True,
    )


if __name__ == '__main__':
    main_admin()
