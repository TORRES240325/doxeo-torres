import os
import sys
import logging
import json
import asyncio
import re
import math
import time
import base64
import difflib
import html
import traceback
from io import BytesIO
from urllib import request, error
from telegram import Update, ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove, InlineKeyboardButton, InlineKeyboardMarkup, InputFile, InputMediaPhoto
from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, filters, ContextTypes, ConversationHandler
from telegram.error import BadRequest, Conflict
from telegram.request import HTTPXRequest
from sqlalchemy.orm.exc import NoResultFound
from db_models import Usuario, Producto, Key, Compra, inicializar_db, get_session 
from dotenv import load_dotenv

# =================================================================
# 1. Configuración Inicial (Lectura de Variables de Entorno)
# =================================================================
load_dotenv()
TOKEN = os.getenv('BOT_MAIN_TOKEN') 
if not TOKEN:
    raise ValueError("Error: BOT_MAIN_TOKEN no encontrado. Verifica las variables de entorno.")

LEDER_API_BASE_URL = os.getenv('LEDER_API_BASE_URL', 'https://leder-data-api.ngrok.dev/v1.7').rstrip('/')
LEDER_API_TOKEN = os.getenv('LEDER_API_TOKEN', '')
LEDER_API_SOURCE = os.getenv('LEDER_API_SOURCE', 'database')

inicializar_db() 

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Estados del ConversationHandler ---
BUY_CATEGORY, BUY_PRODUCT = range(2)

BUY_CREDITS_URL = os.getenv("BUY_CREDITS_URL", "https://wa.me/51921154839")
BUY_CREDITS_CONTACT = "+51 921 154 839"
BOT_IMAGES_DIR = os.path.join(os.path.dirname(__file__), "assets", "images")
COMMAND_IMAGE_BASENAME = {
    "start": "start",
    "cmds": "cmds",
    "me": "me",
    "token_api": "token_api",
}
SUPPORTED_IMAGE_EXTENSIONS = (".jpg", ".jpeg", ".png", ".webp")
BOT_RESPONSE_TAG = "#TORRES_DATA"
POSTMAN_V17_UNSUPPORTED_COMMANDS = {
    "nm",
    "c4",
    "dnivaz",
    "dnivam",
    "dnivel",
    "dniveln",
    "fa",
    "fad",
    "fadb",
    "fb",
    "fbdb",
}
POSTMAN_V17_DOCUMENTED_COMMANDS = {
    "dni", "dnif", "dnidb", "dnifdb",
    "dend", "denp",
    "sue", "tra",
    "sun", "sunr",
    "meta",
    "ag",
    "fam", "fam2", "fam3",
    "con",
    "tremp", "exd",
    "dir", "cor",
    "tel", "telp", "osiptel",
    "claro", "entel",
    "pla", "pro",
    "fis", "fishnm",
}

PLAN_RANK = {
    "FREE": 1,
    "STANDARD": 2,
    "STANDAR": 2,
    "VIP": 3,
    "GOLD": 4,
    "DIAMOND": 5,
}

PLAN_ANTISPAM_SECONDS = {
    "FREE": 20,
    "STANDARD": 20,
    "STANDAR": 20,
    "VIP": 10,
    "GOLD": 6,
    "DIAMOND": 4,
}

USER_LAST_COMMAND_TS: dict[int, float] = {}
USER_LAST_START_TS: dict[int, float] = {}
PROCESSED_COMMAND_MESSAGES: dict[tuple[int, str], float] = {}
COMMAND_DEDUP_SECONDS = 3

COMMAND_DEFINITIONS = {
    "dni": {"name": "RENIEC DNI ONLINE [FREEMIUM] [1 FOTO]", "args": "<dni>", "cost": 2, "response": "Datos e imagen [rostro]", "plan": "FREE", "implemented": True},
    "dnif": {"name": "RENIEC DNI ONLINE [FREEMIUM] [4 FOTOS]", "args": "<dni>", "cost": 3, "response": "Datos e imagenes [rostro, huellas y firma]", "plan": "FREE", "implemented": False},
    "dnidb": {"name": "RENIEC DNI DATABASE [FREEMIUM] [1 FOTO]", "args": "<dni>", "cost": 1, "response": "Datos e imagen [rostro]", "plan": "FREE", "implemented": False},
    "dnifdb": {"name": "RENIEC DNI DATABASE [FREEMIUM] [4 FOTOS]", "args": "<dni>", "cost": 2, "response": "Datos e imagenes [rostro, huellas y firma]", "plan": "FREE", "implemented": False},
    "nm": {"name": "RENIEC NOMBRES ONLINE [FREEMIUM]", "args": "<nombres,apellidos>", "cost": 2, "response": "Texto de coincidencias", "plan": "FREE", "implemented": False},
    "dnim": {"name": "RENIEC DNI METADATA [PREMIUM]", "args": "<dni>", "cost": 2, "response": "Datos en texto", "plan": "STANDARD", "implemented": False},
    "mh": {"name": "RENIEC MEJORES HUELLAS ONLINE [PREMIUM]", "args": "<dni>", "cost": 5, "response": "Datos en texto", "plan": "GOLD", "implemented": False},
    "c4": {"name": "CERTIFICADO DE INSCRIPCION (C4) [PREMIUM]", "args": "<dni>", "cost": 5, "response": "PDF generado", "plan": "GOLD", "implemented": False},
    "dnivaz": {"name": "DNI VIRTUAL AZUL [PREMIUM]", "args": "<dni>", "cost": 5, "response": "Anverso y reverso", "plan": "GOLD", "implemented": False},
    "dnivam": {"name": "DNI VIRTUAL AMARILLO [PREMIUM]", "args": "<dni>", "cost": 5, "response": "Anverso y reverso", "plan": "GOLD", "implemented": False},
    "dnivel": {"name": "DNI VIRTUAL ELECTRONICO [PREMIUM]", "args": "<dni>", "cost": 5, "response": "Anverso y reverso", "plan": "GOLD", "implemented": False},
    "dniveln": {"name": "DNI VIRTUAL ELECTRONICO NUEVO [PREMIUM]", "args": "<dni>", "cost": 5, "response": "Anverso y reverso", "plan": "GOLD", "implemented": False},
    "fa": {"name": "FICHA RENIEC AZUL ONLINE [PREMIUM]", "args": "<dni>", "cost": 5, "response": "Imagen Generada", "plan": "GOLD", "implemented": False},
    "fad": {"name": "FICHA RENIEC AZUL DATABASE [PREMIUM]", "args": "<dni>", "cost": 5, "response": "Imagen Generada", "plan": "GOLD", "implemented": False},
    "fadb": {"name": "FICHA RENIEC AZUL DATABASE [PREMIUM]", "args": "<dni>", "cost": 5, "response": "Imagen Generada", "plan": "GOLD", "implemented": False},
    "fb": {"name": "FICHA RENIEC BLANCA ONLINE [PREMIUM]", "args": "<dni>", "cost": 5, "response": "Imagen Generada", "plan": "GOLD", "implemented": False},
    "fbdb": {"name": "FICHA RENIEC BLANCA DATABASE [PREMIUM]", "args": "<dni>", "cost": 5, "response": "Imagen Generada", "plan": "GOLD", "implemented": False},
    "cnv": {"name": "CERTIFICADO NACIDO VIVO [PREMIUM]", "args": "<dni>", "cost": 15, "response": "PDF OFICIAL", "plan": "GOLD", "implemented": False},
    "cdef": {"name": "CERTIFICADO DE DEFUNCION [PREMIUM]", "args": "<dni>", "cost": 15, "response": "PDF OFICIAL", "plan": "GOLD", "implemented": False},
    "fis": {"name": "FISCALIA PERSONAS [PREMIUM]", "args": "<dni>", "cost": 25, "response": "Documento PDF", "plan": "DIAMOND", "implemented": False},
    "fisruc": {"name": "FISCALIA EMPRESAS [PREMIUM]", "args": "<ruc>", "cost": 25, "response": "Documento PDF", "plan": "DIAMOND", "implemented": False},
    "fishnm": {"name": "FISCALIA PERSONAS NOMBRES [PREMIUM]", "args": "<nombres|paterno|materno>", "cost": 25, "response": "Documento PDF", "plan": "DIAMOND", "implemented": False},
    "det": {"name": "DETENIDOS [PREMIUM]", "args": "<dni>", "cost": 15, "response": "Datos en texto", "plan": "GOLD", "implemented": False},
    "rqh": {"name": "REQUISITORIAS HISTORICAS [PREMIUM]", "args": "<dni>", "cost": 3, "response": "Datos en texto", "plan": "GOLD", "implemented": False},
    "antpenv": {"name": "ANTECEDENTES PENALES VERIFICADOR [PREMIUM]", "args": "<dni>", "cost": 2, "response": "Datos en texto", "plan": "STANDARD", "implemented": False},
    "dend": {"name": "DENUNCIAS POLICIALES [PREMIUM]", "args": "<dni>", "cost": 15, "response": "PDF GENERADO", "plan": "GOLD", "implemented": False},
    "dence": {"name": "DENUNCIAS POLICIALES [PREMIUM]", "args": "<carnet_extranjeria>", "cost": 15, "response": "PDF GENERADO", "plan": "GOLD", "implemented": False},
    "denpas": {"name": "DENUNCIAS POLICIALES [PREMIUM]", "args": "<pasaporte>", "cost": 15, "response": "PDF GENERADO", "plan": "GOLD", "implemented": False},
    "denci": {"name": "DENUNCIAS POLICIALES [PREMIUM]", "args": "<cedula_identidad>", "cost": 15, "response": "PDF GENERADO", "plan": "GOLD", "implemented": False},
    "denp": {"name": "DENUNCIAS POLICIALES [PREMIUM]", "args": "<placa>", "cost": 15, "response": "PDF GENERADO", "plan": "GOLD", "implemented": False},
    "denar": {"name": "DENUNCIAS POLICIALES [PREMIUM]", "args": "<serie_armamento>", "cost": 15, "response": "PDF GENERADO", "plan": "GOLD", "implemented": False},
    "dencd": {"name": "DENUNCIAS POLICIALES [PREMIUM]", "args": "<clave_denuncia>", "cost": 15, "response": "PDF GENERADO", "plan": "GOLD", "implemented": False},
    "sun": {"name": "SUNAT RUC [PREMIUM]", "args": "<dni_o_ruc>", "cost": 2, "response": "Datos en texto", "plan": "STANDARD", "implemented": False},
    "sunr": {"name": "SUNAT RAZON SOCIAL [PREMIUM]", "args": "<razon_social>", "cost": 1, "response": "Datos en texto", "plan": "STANDARD", "implemented": False},
    "antpen": {"name": "CERTIFICADO ANTECEDENTES PENALES [PREMIUM]", "args": "<dni>", "cost": 5, "response": "PDF Generado", "plan": "GOLD", "implemented": False},
    "antpol": {"name": "CERTIFICADO ANTECEDENTES POLICIALES [PREMIUM]", "args": "<dni>", "cost": 5, "response": "PDF Generado", "plan": "GOLD", "implemented": False},
    "antjud": {"name": "CERTIFICADO ANTECEDENTES JUDICIALES [PREMIUM]", "args": "<dni>", "cost": 5, "response": "PDF Generado", "plan": "GOLD", "implemented": False},
    "actan": {"name": "ACTA NACIMIENTO ONLINE [PREMIUM]", "args": "<dni>", "cost": 25, "response": "Datos e imagen del acta original", "plan": "GOLD", "implemented": False},
    "actam": {"name": "ACTA MATRIMONIO ONLINE [PREMIUM]", "args": "<dni>", "cost": 25, "response": "Datos e imagen del acta original", "plan": "GOLD", "implemented": False},
    "actad": {"name": "ACTA DEFUNCION ONLINE [PREMIUM]", "args": "<dni>", "cost": 25, "response": "Datos e imagen del acta original", "plan": "GOLD", "implemented": False},
    "actamdb": {"name": "ACTA MATRIMONIO DATABASE [PREMIUM]", "args": "<dni>", "cost": 25, "response": "Datos e imagen del acta original", "plan": "GOLD", "implemented": False},
    "actaddb": {"name": "ACTA DEFUNCION DATABASE [PREMIUM]", "args": "<dni>", "cost": 25, "response": "Datos e imagen del acta original", "plan": "GOLD", "implemented": False},
    "actancc": {"name": "ACTA NACIMIENTO ONLINE COPIA CERTIFICADA [PREMIUM]", "args": "<dni>", "cost": 50, "response": "Datos y documento PDF Original", "plan": "GOLD", "implemented": False},
    "actamcc": {"name": "ACTA MATRIMONIO ONLINE COPIA CERTIFICADA [PREMIUM]", "args": "<dni>", "cost": 50, "response": "Datos y documento PDF Original", "plan": "GOLD", "implemented": False},
    "actadcc": {"name": "ACTA DEFUNCION ONLINE COPIA CERTIFICADA [PREMIUM]", "args": "<dni>", "cost": 50, "response": "Datos y documento PDF Original", "plan": "GOLD", "implemented": False},
    "tel": {"name": "TELEFONOS [FREE]", "args": "<dni_o_telefono>", "cost": 15, "response": "Datos en texto", "plan": "FREE", "implemented": False},
    "telp": {"name": "TELEFONOS [PREMIUM]", "args": "<dni_o_telefono>", "cost": 5, "response": "Datos en texto", "plan": "STANDARD", "implemented": False},
    "osiptel": {"name": "OSIPTEL [PREMIUM]", "args": "<dni>", "cost": 5, "response": "Datos en texto", "plan": "GOLD", "implemented": False},
    "claro": {"name": "CLARO [PREMIUM]", "args": "<telefono>", "cost": 2, "response": "Datos en texto", "plan": "STANDARD", "implemented": False},
    "entel": {"name": "ENTEL [PREMIUM]", "args": "<telefono>", "cost": 2, "response": "Datos en texto", "plan": "STANDARD", "implemented": False},
    "ag": {"name": "ARBOL GENEALOGICO [PREMIUM]", "args": "<dni>", "cost": 6, "response": "Datos en texto", "plan": "STANDARD", "implemented": False},
    "agv": {"name": "ARBOL GENEALOGICO VISUAL [PREMIUM]", "args": "<dni>", "cost": 15, "response": "Imagen Generada", "plan": "GOLD", "implemented": False},
    "agvp": {"name": "ARBOL GENEALOGICO VISUAL PROFESIONAL [PREMIUM]", "args": "<dni>", "cost": 20, "response": "Imagen Generada", "plan": "GOLD", "implemented": False},
    "fam": {"name": "FAMILIA [PREMIUM]", "args": "<dni>", "cost": 3, "response": "Datos en texto", "plan": "STANDARD", "implemented": False},
    "fam2": {"name": "FAMILIA 2 [PREMIUM]", "args": "<dni>", "cost": 3, "response": "Datos en texto", "plan": "STANDARD", "implemented": False},
    "fam3": {"name": "FAMILIA 3 [PREMIUM]", "args": "<dni>", "cost": 3, "response": "Datos en texto", "plan": "STANDARD", "implemented": False},
    "pro": {"name": "PROPIEDADES [PREMIUM]", "args": "<dni>", "cost": 5, "response": "Datos en texto", "plan": "GOLD", "implemented": False},
    "sbs": {"name": "SBS REPORTE DEUDAS ONLINE [PREMIUM]", "args": "<dni>", "cost": 15, "response": "Reportes en PDF", "plan": "GOLD", "implemented": False},
    "sbsdb": {"name": "SBS REPORTE DEUDAS DATABASE [PREMIUM]", "args": "<dni>", "cost": 10, "response": "Datos e imagen generada", "plan": "GOLD", "implemented": False},
    "sbsrucdb": {"name": "SBS REPORTE DEUDAS DATABASE - EMPRESAS [PREMIUM]", "args": "<ruc>", "cost": 10, "response": "Datos e imagen generada", "plan": "GOLD", "implemented": False},
    "cafp": {"name": "CONSTANCIA AFILIACION AFP [PREMIUM]", "args": "<dni>", "cost": 10, "response": "Reporte en PDF", "plan": "GOLD", "implemented": False},
    "pla": {"name": "VEHICULOS [PREMIUM]", "args": "<dni o placa>", "cost": 2, "response": "Datos en texto", "plan": "STANDARD", "implemented": False},
    "tra": {"name": "TRABAJOS [PREMIUM]", "args": "<dni>", "cost": 2, "response": "Datos en texto", "plan": "STANDARD", "implemented": False},
    "tremp": {"name": "TRABAJADORES POR EMPRESA [PREMIUM]", "args": "<ruc>", "cost": 2, "response": "Datos en texto", "plan": "STANDARD", "implemented": False},
    "sue": {"name": "SUELDOS [PREMIUM]", "args": "<dni>", "cost": 3, "response": "Datos en texto", "plan": "STANDARD", "implemented": False},
    "con": {"name": "CONYUGES [PREMIUM]", "args": "<dni>", "cost": 3, "response": "Datos en texto", "plan": "STANDARD", "implemented": False},
    "exd": {"name": "EMPRESAS [PREMIUM]", "args": "<dni>", "cost": 1, "response": "Datos en texto", "plan": "STANDARD", "implemented": False},
    "meta": {"name": "METADATA COMPLETA [PREMIUM]", "args": "<dni>", "cost": 15, "response": "Datos en texto (Busqueda en 10 fuentes)", "plan": "STANDARD", "implemented": False},
    "dir": {"name": "DIRECCIONES [PREMIUM]", "args": "<dni>", "cost": 3, "response": "Datos en texto", "plan": "STANDARD", "implemented": False},
    "cor": {"name": "CORREOS [PREMIUM]", "args": "<dni_o_correo>", "cost": 3, "response": "Datos en texto", "plan": "STANDARD", "implemented": False},
    "seeker": {"name": "SEEKER [PREMIUM]", "args": "<dni>", "cost": 10, "response": "Datos en texto", "plan": "STANDARD", "implemented": False},
    "afp": {"name": "AFPS [PREMIUM]", "args": "<dni>", "cost": 2, "response": "Datos en texto", "plan": "STANDARD", "implemented": False},
    "bdir": {"name": "DIRECCION INVERSA [PREMIUM]", "args": "<direccion>", "cost": 15, "response": "Datos en texto", "plan": "STANDARD", "implemented": False},
    "ce": {"name": "CARNET EXTRANJERIA [FREE]", "args": "<carnet_extranjeria>", "cost": 0, "response": "Datos en texto", "plan": "FREE", "implemented": False},
    "nmv": {"name": "VENEZOLANOS NOMBRES [PREMIUM]", "args": "<nombres_apellidos>", "cost": 1, "response": "Datos en texto", "plan": "STANDARD", "implemented": False},
    "cedula": {"name": "VENEZOLANOS CEDULA [PREMIUM]", "args": "<cedula>", "cost": 1, "response": "Datos en texto", "plan": "STANDARD", "implemented": False},
    "pasaporte": {"name": "PASAPORTE [PREMIUM]", "args": "<pasaporte>", "cost": 10, "response": "Datos en texto", "plan": "STANDARD", "implemented": False},
}

CATEGORY_ORDER = [
    "RENIEC [🪪]",
    "JUSTICIA [⚖️]",
    "SUNAT [🏛️]",
    "CERTIFICADOS [📄]",
    "ACTAS [📑]",
    "TELEFONIA [☎️]",
    "FAMILIARES [👨‍👩‍👧‍👦]",
    "MIGRACIONES [🌎]",
    "SUNARP [🏘️]",
    "FINANCIERO [📉]",
    "VEHICULOS [🚙]",
    "LABORAL [💼]",
    "RECONOCIMIENTO FACIAL [🤖🔥]",
    "PLUS GENERAL [🕵️]",
    "METADATA v1 [⚡]",
    "METADATA v2 [👾]",
]

CATEGORY_PAGES = {
    "RENIEC [🪪]": [["dni", "dnif", "dnidb", "dnifdb"], ["nm", "dnim", "mh", "c4"], ["dnivaz", "dnivam", "dnivel", "dniveln"], ["fa", "fad", "fb", "fbdb"], ["cnv", "cdef"]],
    "JUSTICIA [⚖️]": [["fis", "fisruc", "fishnm", "det"], ["rqh", "antpenv", "dend", "dence"], ["denpas", "denci", "denp", "denar"], ["dencd"]],
    "SUNAT [🏛️]": [["sun", "sunr"]],
    "CERTIFICADOS [📄]": [["antpen", "antpol", "antjud"]],
    "ACTAS [📑]": [["actan", "actam", "actad"], ["actamdb", "actaddb"], ["actancc", "actamcc", "actadcc"]],
    "TELEFONIA [☎️]": [["tel", "telp", "osiptel", "claro"], ["entel"]],
    "FAMILIARES [👨‍👩‍👧‍👦]": [["ag", "agv", "agvp", "fam"], ["fam2", "fam3"]],
    "MIGRACIONES [🌎]": [[]],
    "SUNARP [🏘️]": [["pro"]],
    "FINANCIERO [📉]": [["sbs", "sbsdb", "sbsrucdb", "cafp"]],
    "VEHICULOS [🚙]": [["pla"]],
    "LABORAL [💼]": [["tra", "tremp", "sue"]],
    "RECONOCIMIENTO FACIAL [🤖🔥]": [[]],
    "PLUS GENERAL [🕵️]": [["con", "exd", "meta", "dir"]],
    "METADATA v1 [⚡]": [["cor", "seeker", "afp", "bdir"]],
    "METADATA v2 [👾]": [["ce", "nmv", "cedula", "pasaporte"]],
}

# Filtra el menú /cmds para mostrar solo comandos documentados en Postman v1.7
_filtered_category_pages: dict[str, list[list[str]]] = {}
for _category, _pages in CATEGORY_PAGES.items():
    _new_pages: list[list[str]] = []
    for _row in _pages:
        _filtered_row = [cmd for cmd in _row if cmd in POSTMAN_V17_DOCUMENTED_COMMANDS]
        if _filtered_row:
            _new_pages.append(_filtered_row)
    if _new_pages:
        _filtered_category_pages[_category] = _new_pages

CATEGORY_PAGES = _filtered_category_pages
CATEGORY_ORDER = [cat for cat in CATEGORY_ORDER if cat in CATEGORY_PAGES]

API_COMMAND_MAP = {
    "dni": {"endpoint": "/persona/reniec", "type": "dni", "extra": {"source": LEDER_API_SOURCE}},
    "dnif": {"endpoint": "/persona/reniec", "type": "dni", "extra": {"source": LEDER_API_SOURCE}},
    "dnidb": {"endpoint": "/persona/reniec", "type": "dni", "extra": {"source": "database"}},
    "dnifdb": {"endpoint": "/persona/reniec", "type": "dni", "extra": {"source": "database"}},
    "nm": {"endpoint": "/persona/reniec/nombres", "type": "reniec_nombres"},
    "dnim": {"endpoint": "/persona/reniec", "type": "dni", "extra": {"source": LEDER_API_SOURCE}},
    "mh": {"endpoint": "/persona/reniec", "type": "dni", "extra": {"source": LEDER_API_SOURCE}},
    "c4": {"endpoint": "/persona/certificado-inscripcion", "type": "dni", "extra": {"source": LEDER_API_SOURCE}},
    "dnivaz": {"endpoint": "/persona/reniec", "type": "dni", "extra": {"source": "dnivaz"}},
    "dnivam": {"endpoint": "/persona/reniec", "type": "dni", "extra": {"source": "dnivam"}},
    "dnivel": {"endpoint": "/persona/reniec", "type": "dni", "extra": {"source": "dnivel"}},
    "dniveln": {"endpoint": "/persona/reniec", "type": "dni", "extra": {"source": "dniveln"}},
    "fa": {"endpoint": "/persona/reniec", "type": "dni", "extra": {"source": "fa"}},
    "fad": {"endpoint": "/persona/reniec", "type": "dni", "extra": {"source": "fad"}},
    "fadb": {"endpoint": "/persona/reniec", "type": "dni", "extra": {"source": "fad"}},
    "fb": {"endpoint": "/persona/reniec", "type": "dni", "extra": {"source": "fb"}},
    "fbdb": {"endpoint": "/persona/reniec", "type": "dni", "extra": {"source": "fbdb"}},
    "dend": {"endpoint": "/persona/denuncias-policiales-dni", "type": "dni"},
    "dence": {"endpoint": "/persona/denuncias-policiales-dni", "type": "dni"},
    "denpas": {"endpoint": "/persona/denuncias-policiales-dni", "type": "dni"},
    "denci": {"endpoint": "/persona/denuncias-policiales-dni", "type": "dni"},
    "det": {"endpoint": "/persona/denuncias-policiales-dni", "type": "dni"},
    "rqh": {"endpoint": "/persona/denuncias-policiales-dni", "type": "dni"},
    "antpenv": {"endpoint": "/persona/denuncias-policiales-dni", "type": "dni"},
    "denp": {"endpoint": "/persona/denuncias-policiales-placa", "type": "placa"},
    "denar": {"endpoint": "/persona/denuncias-policiales-placa", "type": "placa"},
    "sue": {"endpoint": "/persona/sueldos", "type": "dni"},
    "tra": {"endpoint": "/persona/trabajos", "type": "dni"},
    "tremp": {"endpoint": "/persona/empresas", "type": "dni"},
    "sun": {"endpoint": "/empresa/sunat", "type": "data"},
    "sunr": {"endpoint": "/empresa/sunat/razon-social", "type": "data"},
    "meta": {"endpoint": "/persona/consumos", "type": "dni"},
    "ag": {"endpoint": "/persona/arbol-genealogico", "type": "dni"},
    "agv": {"endpoint": "/persona/arbol-genealogico", "type": "dni"},
    "agvp": {"endpoint": "/persona/arbol-genealogico", "type": "dni"},
    "fam": {"endpoint": "/persona/familia-1", "type": "dni"},
    "fam2": {"endpoint": "/persona/familia-2", "type": "dni"},
    "fam3": {"endpoint": "/persona/familia-3", "type": "dni"},
    "con": {"endpoint": "/persona/matrimonios", "type": "dni"},
    "actam": {"endpoint": "/persona/matrimonios", "type": "dni"},
    "exd": {"endpoint": "/persona/empresas", "type": "dni"},
    "dir": {"endpoint": "/persona/direcciones", "type": "dni"},
    "cor": {"endpoint": "/persona/correos", "type": "dni"},
    "tel": {"endpoint": "/telefonia/documento", "type": "documento"},
    "telp": {"endpoint": "/telefonia/documento", "type": "documento"},
    "osiptel": {"endpoint": "/telefonia/documento", "type": "documento"},
    "claro": {"endpoint": "/telefonia/numero", "type": "numero"},
    "entel": {"endpoint": "/telefonia/numero", "type": "numero"},
    "pla": {"endpoint": "/vehiculos/sunarp", "type": "placa"},
    "pro": {"endpoint": "/vehiculos/sunarp", "type": "placa"},
    "fis": {"endpoint": "/persona/justicia/fiscalia/dni", "type": "dni"},
    "fisruc": {"endpoint": "/persona/justicia/fiscalia/dni", "type": "dni"},
    "fishnm": {"endpoint": "/persona/justicia/fiscalia/nombres", "type": "fiscalia_nombres"},
}

# =================================================================
# 2. Funciones de Utilidad y Teclados
# =================================================================

def get_keyboard_main(is_logged_in):
    return ReplyKeyboardRemove()


def _command_image_path(command_name: str) -> str | None:
    base_name = COMMAND_IMAGE_BASENAME.get(command_name)
    if not base_name:
        return None
    for ext in SUPPORTED_IMAGE_EXTENSIONS:
        candidate = os.path.join(BOT_IMAGES_DIR, f"{base_name}{ext}")
        if os.path.isfile(candidate):
            return candidate
    return None


async def _send_command_image(
    update: Update,
    command_name: str,
    caption: str | None = None,
    reply_markup: InlineKeyboardMarkup | None = None,
) -> bool:
    if not update.message:
        return False
    image_path = _command_image_path(command_name)
    if not image_path:
        return False
    try:
        with open(image_path, "rb") as image_file:
            await update.message.reply_photo(
                photo=InputFile(image_file),
                caption=caption,
                reply_markup=reply_markup,
                reply_to_message_id=update.message.message_id,
            )
        return True
    except Exception as exc:
        logger.warning(f"No se pudo enviar imagen de {command_name}: {exc}")
        return True


def _fit_caption(text: str, max_len: int = 1024) -> str:
    cleaned = (text or "").strip()
    if len(cleaned) <= max_len:
        return cleaned
    return cleaned[: max_len - 3].rstrip() + "..."


async def _edit_cmds_message(query, text: str, reply_markup: InlineKeyboardMarkup) -> None:
    is_photo_message = bool(query.message and query.message.photo)
    if is_photo_message:
        caption_text = text[:1024]
        try:
            await query.edit_message_caption(caption=caption_text, reply_markup=reply_markup)
            return
        except BadRequest as exc:
            if "not modified" in str(exc).lower():
                return
            await query.edit_message_reply_markup(reply_markup=reply_markup)
            return
    try:
        await query.edit_message_text(text=text, reply_markup=reply_markup)
    except BadRequest as exc:
        if "not modified" in str(exc).lower():
            return
        raise


def _get_logged_user(telegram_id: int):
    with get_session() as session_db:
        return session_db.query(Usuario).filter_by(telegram_id=telegram_id).first()


def _build_username(base: str, telegram_id: int) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9_]", "", (base or "").strip())
    if not cleaned:
        cleaned = f"tg_{telegram_id}"
    return cleaned[:50]


def _normalize_plan(plan_name: str | None) -> str:
    normalized = (plan_name or "FREE").strip().upper()
    if normalized == "STANDAR":
        return "STANDARD"
    return normalized


def _has_minimum_plan(user_plan: str | None, minimum_plan: str | None) -> bool:
    user_rank = PLAN_RANK.get(_normalize_plan(user_plan), 1)
    required_rank = PLAN_RANK.get(_normalize_plan(minimum_plan), 1)
    return user_rank >= required_rank


def _plan_antispam_seconds(plan_name: str | None) -> int:
    normalized = _normalize_plan(plan_name)
    return int(PLAN_ANTISPAM_SECONDS.get(normalized, 8))


def _ensure_registered_user(telegram_user) -> Usuario:
    telegram_id = telegram_user.id
    base_username = _build_username(telegram_user.username or telegram_user.first_name or "", telegram_id)

    with get_session() as session_db:
        usuario = session_db.query(Usuario).filter_by(telegram_id=telegram_id).first()
        if usuario:
            return usuario

        username_candidate = base_username
        suffix = 1
        while session_db.query(Usuario).filter_by(username=username_candidate).first() is not None:
            username_candidate = f"{base_username[:40]}_{suffix}"
            suffix += 1

        usuario = Usuario(
            telegram_id=telegram_id,
            username=username_candidate,
            login_key=f"AUTO_{telegram_id}",
            saldo=5.0,
            es_admin=False,
            plan="FREE",
            estado="ACTIVO",
        )
        session_db.add(usuario)
        session_db.commit()
        session_db.refresh(usuario)
        return usuario


def _has_credits(telegram_id: int, amount: float) -> tuple[bool, str]:
    with get_session() as session_db:
        usuario = session_db.query(Usuario).filter_by(telegram_id=telegram_id).first()
        if not usuario:
            return False, "Debes iniciar sesión para usar comandos con créditos."
        if amount > 0 and usuario.saldo < amount:
            return False, f"Créditos insuficientes. Tu saldo actual es {usuario.saldo:.2f}."
        return True, "OK"


def _charge_credits(telegram_id: int, amount: float) -> tuple[bool, str, float]:
    with get_session() as session_db:
        usuario = session_db.query(Usuario).filter_by(telegram_id=telegram_id).first()
        if not usuario:
            return False, "Debes iniciar sesión para usar comandos con créditos.", 0.0

        if amount <= 0:
            return True, "OK", float(usuario.saldo)

        if usuario.saldo < amount:
            return False, f"Créditos insuficientes. Tu saldo actual es {usuario.saldo:.2f}.", float(usuario.saldo)

        usuario.saldo -= amount
        session_db.commit()
        return True, "OK", float(usuario.saldo)


def _build_cmds_home_keyboard() -> InlineKeyboardMarkup:
    rows = []
    row = []
    for index, category in enumerate(CATEGORY_ORDER):
        row.append(InlineKeyboardButton(category, callback_data=f"cmds:cat:{index}:0"))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton("✖", callback_data="cmds:close")])
    return InlineKeyboardMarkup(rows)


def _build_category_page_text(category_name: str, page_index: int) -> str:
    pages = CATEGORY_PAGES.get(category_name, [[]])
    page_items = pages[page_index] if 0 <= page_index < len(pages) else []

    text = [f"[#TORRES_DOX]", "", f"Categoria : {category_name}", "======================"]

    if not page_items:
        text.append("\nNo hay comandos configurados en esta categoría.")
    else:
        for command_key in page_items:
            info = COMMAND_DEFINITIONS[command_key]
            text.extend([
                "",
                f"[📌] {info['name']}",
                "----------------------",
                f"[🚀] Uso: /{command_key} {info['args']}",
                f"[💰] Consumo: {info['cost']} créditos",
                f"[📄] Respuesta: {info['response']}",
                f"[📍] Plan Minimo: {info['plan']}",
                "=============================="
            ])

    text.append("")
    text.append(f"Página {page_index + 1}/{len(pages)}")
    return "\n".join(text)


def _build_category_page_keyboard(category_idx: int, page_index: int) -> InlineKeyboardMarkup:
    category_name = CATEGORY_ORDER[category_idx]
    total_pages = len(CATEGORY_PAGES.get(category_name, [[]]))
    prev_page = (page_index - 1) if page_index > 0 else total_pages - 1
    next_page = (page_index + 1) if page_index < total_pages - 1 else 0

    rows = [
        [
            InlineKeyboardButton("⟵", callback_data=f"cmds:cat:{category_idx}:{prev_page}"),
            InlineKeyboardButton("✖", callback_data="cmds:close"),
            InlineKeyboardButton("⟶", callback_data=f"cmds:cat:{category_idx}:{next_page}"),
        ],
        [InlineKeyboardButton("REGRESAR AL INICIO", callback_data="cmds:home")],
        [InlineKeyboardButton("Comprar créditos 💰", url=BUY_CREDITS_URL)],
    ]
    return InlineKeyboardMarkup(rows)


def _first_value(data: dict, keys: list[str]) -> str:
    for key in keys:
        value = data.get(key)
        if value is not None and str(value).strip() != "":
            return str(value)
    return "No disponible"


def _is_empty_value(value) -> bool:
    if value is None:
        return True
    text = str(value).strip().lower()
    normalized = _normalize_key(text)
    return normalized in {
        "",
        "none",
        "null",
        "nan",
        "na",
        "sindatos",
        "sininformacion",
        "nodisponible",
        "desconocido",
        "-",
    }


def _normalize_key(key: str) -> str:
    return re.sub(r"[^a-z0-9]", "", str(key).strip().lower())


def _matches_alias_key(normalized_key: str, keys_normalized: set[str]) -> bool:
    if normalized_key in keys_normalized:
        return True
    for alias in keys_normalized:
        if not alias:
            continue
        if (
            normalized_key.startswith(alias)
            or normalized_key.endswith(alias)
            or alias in normalized_key
        ):
            return True
    return False


FIELD_ALIASES: dict[str, list[str]] = {
    "dni": [
        "dni", "num_doc", "numero_documento", "nro_documento", "nro_doc", "numero_doc",
        "documento", "documento_numero", "n_documento", "nu_doc", "doc_num", "id_numero",
        "id_documento", "document_number", "documentnumber", "ndocumento", "numerodocumento",
        "dni_numero", "numerodni", "docidentidad", "identity_document",
    ],
    "digito": ["digito_verificacion", "digito", "dv", "dverificador", "check_digit", "checkdigit", "digitoverificacion", "verification_digit"],
    "nombres": ["nombres", "nombre", "prenombres", "nombres_completos", "nombrescompletos", "nombre_completo", "fullname", "full_name", "given_names", "name"],
    "apellido_paterno": ["apellido_paterno", "ape_paterno", "ap_paterno", "paterno", "apellido1", "primer_apellido", "lastname1", "apellidopaterno"],
    "apellido_materno": ["apellido_materno", "ape_materno", "ap_materno", "materno", "apellido2", "segundo_apellido", "lastname2", "apellidomaterno"],
    "apellidos": ["apellidos", "apellido", "last_name", "last_names", "surname", "surnames", "family_name", "apellidos_completos"],
    "genero": ["genero", "sexo", "sex", "gender", "genero_desc", "sexo_desc", "desc_genero", "desc_sexo"],
    "edad": ["edad", "age", "anios", "years"],
    "fecha_nacimiento": ["fecha_nacimiento", "fec_nac", "nacimiento", "fecha_nac", "birth_date", "birthdate", "date_of_birth", "dob"],
    "departamento": ["departamento", "departamento_nacimiento", "depa", "region", "department", "depto", "dep", "ubigeo_departamento"],
    "provincia": ["provincia", "prov", "province", "provincia_nacimiento", "province_name", "ubigeo_provincia"],
    "distrito": ["distrito", "dist", "district", "distrito_nacimiento", "district_name", "ubigeo_distrito"],
    "grado_instruccion": ["grado_instruccion", "instruccion", "nivel_instruccion", "gradoinstruccion"],
    "estado_civil": ["estado_civil", "estadocivil", "civil_status", "marital_status"],
    "estatura": ["estatura", "talla", "altura", "height"],
    "fecha_inscripcion": ["fecha_inscripcion", "fec_inscripcion", "inscripcion", "registration_date"],
    "fecha_emision": ["fecha_emision", "fec_emision", "emision", "issue_date"],
    "fecha_caducidad": ["fecha_caducidad", "fec_caducidad", "caducidad", "expiry_date", "expiration_date"],
    "padre": ["padre", "nombre_padre", "father", "father_name"],
    "madre": ["madre", "nombre_madre", "mother", "mother_name"],
    "restriccion": ["restriccion", "restricciones", "restriction", "observacion_restriccion"],
    "direccion": ["direccion", "direccion_completa", "domicilio", "address", "direccion_actual", "address_line"],
    "direccion_departamento": ["direccion_departamento", "departamento_direccion", "dep_direccion"],
    "direccion_provincia": ["direccion_provincia", "provincia_direccion", "prov_direccion", "direccion_prov"],
    "direccion_distrito": ["direccion_distrito", "distrito_direccion", "dist_direccion", "direccion_dist"],
    "ubigeo_reniec": ["ubigeo_reniec", "ubigeoreniec"],
    "ubigeo_inei": ["ubigeo_inei", "ubigeoinei"],
    "ubigeo_sunat": ["ubigeo_sunat", "ubigeosunat"],
    "codigo_postal": ["codigo_postal", "postal_code", "zip_code", "cod_postal"],
    "matrimonio": ["matrimonio", "acta_matrimonio"],
    "nacimiento_acta": ["nacimiento_acta", "acta_nacimiento"],
    "defuncion": ["defuncion", "acta_defuncion"],
    "cert_nacido": ["cert_nacido", "certificado_nacido", "cert_nacimiento"],
    "cert_defuncion": ["cert_defuncion", "certificado_defuncion"],
    "hijos": ["hijos", "cantidad_hijos", "children_count"],
    "telefono": ["telefono", "telefonos", "celular", "numero", "mobile", "phone"],
    "correo": ["correo", "email", "mail", "correo_electronico"],
    "ruc": ["ruc", "numero_ruc", "documento_ruc"],
    "razon_social": ["razon_social", "razonsocial", "empresa", "business_name"],
    "placa": ["placa", "placa_vehiculo", "plate"],
    "marca": ["marca", "brand", "vehicle_brand"],
    "modelo": ["modelo", "model", "vehicle_model"],
    "color": ["color", "vehicle_color"],
    "anio": ["anio", "año", "year_model", "model_year"],
    "serie": ["serie", "nro_serie", "numero_serie", "serial"],
    "motor": ["motor", "nro_motor", "numero_motor", "engine"],
    "operador": ["operador", "company", "carrier", "operadora", "empresa_operadora"],
    "estado_linea": ["estado_linea", "estado", "line_status", "status"],
    "fecha_activacion": ["fecha_activacion", "activation_date", "fecha_alta"],
    "plan_linea": ["plan", "plan_linea", "line_plan", "tarifa", "tipo_plan", "producto"],
    "fuente": ["fuente", "source", "operador_detalle", "provider", "company_name"],
    "periodo": ["periodo", "period", "mes", "anio_mes", "year_month", "corte"],
    "empresa": ["empresa", "company", "employer", "razon_social", "business_name"],
    "cargo": ["cargo", "puesto", "position", "job_title"],
    "sueldo": ["sueldo", "salario", "ingreso", "salary", "income", "monto"],
    "parentesco": ["parentesco", "relacion", "relation", "vinculo"],
    "numero_ficha": ["numero_ficha", "nro_ficha", "ficha_numero", "numeroficha", "ficha"],
    "grupo_votacion": ["grupo_votacion", "grupo", "votacion", "grupovotacion"],
    "estado_documento": ["estado_documento", "estado_doc", "estado", "status_documento", "status_doc"],
    "expediente": ["expediente", "nro_expediente", "numero_expediente", "case_number"],
    "delito": ["delito", "crime", "tipo_delito"],
    "condicion": ["condicion", "condition", "estado_contribuyente"],
    "actividad_economica": ["actividad_economica", "actividad", "economic_activity", "giro"],
}

FIELD_LABELS: dict[str, str] = {
    "dni": "DNI",
    "digito": "DÍGITO VERIFICACIÓN",
    "nombres": "NOMBRES",
    "apellido_paterno": "APELLIDO PATERNO",
    "apellido_materno": "APELLIDO MATERNO",
    "apellidos": "APELLIDOS",
    "genero": "GENERO",
    "edad": "EDAD",
    "fecha_nacimiento": "FECHA NACIMIENTO",
    "departamento": "DEPARTAMENTO",
    "provincia": "PROVINCIA",
    "distrito": "DISTRITO",
    "grado_instruccion": "GRADO INSTRUCCION",
    "estado_civil": "ESTADO CIVIL",
    "estatura": "ESTATURA",
    "fecha_inscripcion": "FECHA INSCRIPCION",
    "fecha_emision": "FECHA EMISION",
    "fecha_caducidad": "FECHA CADUCIDAD",
    "padre": "PADRE",
    "madre": "MADRE",
    "restriccion": "RESTRICCION",
    "direccion": "DIRECCION",
    "direcciondepartamento": "DEPARTAMENTO",
    "direccionprovincia": "PROVINCIA",
    "direcciondistrito": "DISTRITO",
    "ubigeoreniec": "UBIGEO RENIEC",
    "ubigeoinei": "UBIGEO INEI",
    "ubigeosunat": "UBIGEO SUNAT",
    "codigopostal": "CODIGO POSTAL",
    "telefono": "TELEFONO",
    "correo": "CORREO",
    "ruc": "RUC",
    "razonsocial": "RAZON SOCIAL",
    "placa": "PLACA",
    "marca": "MARCA",
    "modelo": "MODELO",
    "color": "COLOR",
    "anio": "AÑO",
    "serie": "SERIE",
    "motor": "MOTOR",
    "operador": "OPERADOR",
    "estadolinea": "ESTADO LINEA",
    "fechaactivacion": "FECHA ACTIVACION",
    "planlinea": "PLAN",
    "fuente": "FUENTE",
    "periodo": "PERIODO",
    "empresa": "EMPRESA",
    "cargo": "CARGO",
    "sueldo": "SUELDO",
    "parentesco": "PARENTESCO",
    "numeroficha": "NUMERO FICHA",
    "grupovotacion": "GRUPO VOTACION",
    "estadodocumento": "ESTADO",
    "expediente": "EXPEDIENTE",
    "delito": "DELITO",
    "condicion": "CONDICION",
    "actividadeconomica": "ACTIVIDAD ECONOMICA",
}


def _value_alias(api_response: dict, alias_name: str, default: str = "-") -> str:
    keys = FIELD_ALIASES.get(alias_name, [alias_name])
    return _first_value_any(api_response, keys, default=default)


def _append_line(lines: list[str], label: str, value: str) -> None:
    value_text = str(value).strip() if value is not None else ""
    if _is_empty_value(value_text):
        value_text = "-"
    lines.append(f"<b>{html.escape(label)}</b> : {html.escape(value_text)}")


def _looks_like_media_value(key_name: str, value_text: str) -> bool:
    key_norm = _normalize_key(key_name)
    value_lower = value_text.lower()
    if value_lower.startswith("data:") and "base64," in value_lower:
        return True
    if value_lower.startswith("http") and any(t in value_lower for t in [".jpg", ".jpeg", ".png", ".webp", ".pdf", "image", "foto", "rostro", "huella", "firma", "pdf"]):
        return True
    return any(token in key_norm for token in ["foto", "image", "img", "rostro", "huella", "firma", "pdf", "archivo", "documento", "reporte"])


def _find_value_recursive(node, keys_normalized: set[str]) -> str | None:
    if isinstance(node, dict):
        for key, value in node.items():
            normalized_key = _normalize_key(key)
            if _matches_alias_key(normalized_key, keys_normalized):
                if isinstance(value, (dict, list)):
                    nested = _find_value_recursive(value, keys_normalized)
                    if nested is not None:
                        return nested
                elif not _is_empty_value(value):
                    return str(value)

        for value in node.values():
            nested = _find_value_recursive(value, keys_normalized)
            if nested is not None:
                return nested

    elif isinstance(node, list):
        for item in node:
            nested = _find_value_recursive(item, keys_normalized)
            if nested is not None:
                return nested

    return None


def _first_value_any(api_response: dict, keys: list[str], default: str = "-") -> str:
    normalized = {_normalize_key(k) for k in keys}
    found = _find_value_recursive(api_response, normalized)
    return found if found is not None else default


def _extract_payload(api_response: dict) -> dict:
    if not isinstance(api_response, dict):
        return {}

    for key in ('data', 'result', 'results', 'persona', 'response'):
        value = api_response.get(key)
        if isinstance(value, dict):
            return value

    return api_response


def _collect_records(api_response: dict) -> list[dict]:
    if not isinstance(api_response, dict):
        return []

    candidate_lists: list[list[dict]] = []

    def _scan(node):
        if isinstance(node, dict):
            for value in node.values():
                _scan(value)
            return

        if isinstance(node, list):
            dict_items = [item for item in node if isinstance(item, dict)]
            if dict_items:
                candidate_lists.append(dict_items)
            for item in node:
                _scan(item)

    for key in (
        "data", "result", "results", "items", "rows", "records", "response",
        "payload", "list", "detalle", "detalles", "telefonos", "lineas", "coincidencias",
    ):
        if key in api_response:
            _scan(api_response.get(key))

    _scan(api_response)

    if not candidate_lists:
        return []

    candidate_lists.sort(key=len, reverse=True)
    return candidate_lists[0]


def _query_api(endpoint: str, payload: dict) -> tuple[bool, dict | str]:
    if not LEDER_API_TOKEN:
        return False, "No se configuró `LEDER_API_TOKEN` en variables de entorno."

    data_payload = dict(payload)
    data_payload["token"] = LEDER_API_TOKEN

    url = f"{LEDER_API_BASE_URL}{endpoint}"
    req = request.Request(
        url,
        data=json.dumps(data_payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with request.urlopen(req, timeout=25) as response:
            body = response.read().decode("utf-8", errors="replace")
            data = json.loads(body) if body else {}
            return True, data
    except error.HTTPError as exc:
        try:
            body = exc.read().decode("utf-8", errors="replace")
            details = json.loads(body) if body else {}
        except Exception:
            details = {}
        message = None
        if isinstance(details, dict):
            message = (
                details.get("message")
                or details.get("error")
                or details.get("detail")
                or details.get("msg")
            )
        if message:
            return False, f"Error HTTP {exc.code}: {message}"
        return False, f"Error HTTP {exc.code} al consultar la API."
    except error.URLError:
        return False, "No se pudo conectar a la API. Verifica URL/red."
    except json.JSONDecodeError:
        return False, "La API respondió con un formato no válido."
    except Exception as exc:
        logger.error(f"Error inesperado consultando API {endpoint}: {exc}")
        return False, "Ocurrió un error inesperado al consultar la API."


def _query_api_with_fallback(command_name: str, endpoint: str, payload: dict) -> tuple[bool, dict | str]:
    candidates: list[tuple[str, dict]] = [(endpoint, dict(payload))]

    if "source" in payload:
        payload_no_source = dict(payload)
        payload_no_source.pop("source", None)
        candidates.append((endpoint, payload_no_source))

    if command_name == "nm":
        nombres = str(payload.get("nombres", "")).strip()
        apep = str(payload.get("apepaterno", "")).strip()
        apem = str(payload.get("apematerno", "")).strip()
        if nombres and apep and apem:
            candidates.append(("/persona/reniec", {"nombres": nombres, "apepaterno": apep, "apematerno": apem}))
            candidates.append(("/persona/reniec", {"data": f"{nombres}|{apep}|{apem}"}))
            candidates.append(("/persona/reniec/nombres", {"nombres": nombres, "apellido_paterno": apep, "apellido_materno": apem}))
            candidates.append(("/persona/reniec/nombres", {"nombre": nombres, "apepaterno": apep, "apematerno": apem}))
            candidates.append(("/persona/reniec/nombres", {"data": f"{nombres} {apep} {apem}"}))

    if command_name == "c4":
        dni = str(payload.get("dni", "")).strip()
        if dni:
            candidates.extend([
                ("/persona/c4", {"dni": dni}),
                ("/persona/certificado-inscripcion", {"dni": dni}),
                ("/persona/certificado-inscripcion", {"documento": dni}),
                ("/persona/certificado-inscripcion", {"numero_documento": dni}),
                ("/persona/c4", {"documento": dni}),
                ("/persona/reniec", {"dni": dni, "source": "c4"}),
                ("/persona/reniec", {"dni": dni}),
            ])

    virtual_cmds = {"dnivaz", "dnivam", "dnivel", "dniveln", "fa", "fad", "fadb", "fb", "fbdb"}
    if command_name in virtual_cmds:
        dni = str(payload.get("dni", "")).strip()
        if dni:
            source_aliases = {
                "dnivaz": ["dnivaz", "virtual_azul", "dni_virtual_azul", "azul"],
                "dnivam": ["dnivam", "virtual_amarillo", "dni_virtual_amarillo", "amarillo"],
                "dnivel": ["dnivel", "virtual_electronico", "dni_virtual_electronico", "electronico"],
                "dniveln": ["dniveln", "virtual_electronico_new", "dni_virtual_electronico_new", "electronico_new"],
                "fa": ["fa", "ficha_azul_online", "ficha_reniec_azul_online", "azul_online"],
                "fad": ["fad", "ficha_azul_database", "ficha_reniec_azul_database", "azul_database"],
                "fadb": ["fadb", "ficha_azul_database", "ficha_reniec_azul_database", "azul_database"],
                "fb": ["fb", "ficha_blanco_online", "ficha_reniec_blanco_online", "blanco_online"],
                "fbdb": ["fbdb", "ficha_blanco_database", "ficha_reniec_blanco_database", "blanco_database"],
            }
            for src in [*source_aliases.get(command_name, []), "online", "database", LEDER_API_SOURCE, "reniec"]:
                candidates.append(("/persona/reniec", {"dni": dni, "source": src}))
            candidates.append(("/persona/reniec", {"dni": dni}))
            candidates.append(("/persona/reniec", {"documento": dni}))
            candidates.append(("/persona/reniec", {"data": dni}))

            if command_name.startswith("dniv"):
                candidates.append(("/persona/dni-virtual", {"dni": dni, "source": command_name}))
                candidates.append(("/persona/dni-virtual", {"dni": dni}))
            if command_name in {"fa", "fad", "fadb", "fb", "fbdb"}:
                candidates.append(("/persona/ficha-reniec", {"dni": dni, "source": command_name}))
                candidates.append(("/persona/ficha-reniec", {"dni": dni}))

    if "dni" in payload:
        dni = str(payload.get("dni", "")).strip()
        if dni:
            candidates.append((endpoint, {"documento": dni}))
            candidates.append((endpoint, {"numero_documento": dni}))
            candidates.append((endpoint, {"data": dni}))

    seen: set[tuple[str, str]] = set()
    last_error: str = "No se pudo consultar la API."
    best_empty_result: dict | None = None

    def _is_retryable_ok_response(data: object) -> bool:
        if not isinstance(data, dict):
            return False

        status = data.get("status")
        if status in {False, 0, "false", "error", "fail", "failed"}:
            return True
        if data.get("success") is False or data.get("ok") is False:
            return True

        msg = str(
            data.get("message")
            or data.get("error")
            or data.get("detail")
            or data.get("msg")
            or ""
        ).strip().lower()
        if any(token in msg for token in ["check payload", "invalid", "not found", "unsupported", "source"]):
            return True

        records = _collect_records(data)
        extracted = _extract_payload(data)
        has_payload_dict = isinstance(extracted, dict) and bool(extracted)
        has_media = any(bool(x) for x in _extract_media_urls(data))
        if not records and not has_payload_dict and not has_media:
            if command_name in {"nm", "c4", "dnivaz", "dnivam", "dnivel", "dniveln", "fa", "fad", "fadb", "fb", "fbdb"}:
                return True
        return False

    for candidate_endpoint, candidate_payload in candidates:
        signature = (candidate_endpoint, json.dumps(candidate_payload, sort_keys=True, ensure_ascii=False))
        if signature in seen:
            continue
        seen.add(signature)

        ok, result = _query_api(candidate_endpoint, candidate_payload)
        if ok:
            if _is_retryable_ok_response(result):
                if isinstance(result, dict) and best_empty_result is None:
                    best_empty_result = result
                continue
            return True, result
        last_error = str(result)

    if best_empty_result is not None:
        return True, best_empty_result

    return False, last_error


def _top_command_usage_help() -> str:
    return (
        "Comandos más usados:\n"
        "• `/dni 12345678`\n"
        "• `/tel 12345678` o `/tel 912345678`\n"
        "• `/fishnm juan|perez|lopez`\n"
        "• `/pla ABC123`\n"
        "• `/sun 10412345678`\n"
        "• `/cmds`"
    )


def _build_format_error_message(command_name: str, raw_input: str, reason: str) -> str:
    attempted = f"/{command_name} {raw_input}".strip()

    templates: dict[str, dict[str, object]] = {
        "dni": {
            "tipo": "Datos personales mediante DNI, foto y dirección.",
            "base": "/dni dni",
            "reglas": [
                "El DNI debe tener 8 dígitos.",
                "El DNI solo debe contener números.",
            ],
            "ok": ["/dni 43652762"],
            "bad": ["/dni 876172", "/dni juan"],
        },
        "dnif": {
            "tipo": "Datos RENIEC con múltiples fotos.",
            "base": "/dnif dni",
            "reglas": [
                "El DNI debe tener 8 dígitos.",
                "El DNI solo debe contener números.",
            ],
            "ok": ["/dnif 43652762"],
            "bad": ["/dnif 876172"],
        },
        "dnidb": {
            "tipo": "Datos RENIEC database con 1 foto.",
            "base": "/dnidb dni",
            "reglas": [
                "El DNI debe tener 8 dígitos.",
                "El DNI solo debe contener números.",
            ],
            "ok": ["/dnidb 43652762"],
            "bad": ["/dnidb 876172"],
        },
        "dnifdb": {
            "tipo": "Datos RENIEC database con múltiples fotos.",
            "base": "/dnifdb dni",
            "reglas": [
                "El DNI debe tener 8 dígitos.",
                "El DNI solo debe contener números.",
            ],
            "ok": ["/dnifdb 43652762"],
            "bad": ["/dnifdb 876172"],
        },
        "dnim": {
            "tipo": "Metadata personal por DNI.",
            "base": "/dnim dni",
            "reglas": ["El DNI debe tener 8 dígitos numéricos."],
            "ok": ["/dnim 43652762"],
            "bad": ["/dnim 12345"],
        },
        "tel": {
            "tipo": "Líneas telefónicas por DNI o teléfono.",
            "base": "/tel dni_o_telefono",
            "reglas": [
                "Si ingresas DNI, debe tener 8 dígitos.",
                "Si ingresas teléfono, debe tener 9 dígitos.",
                "Solo se permiten números.",
            ],
            "ok": ["/tel 43652762", "/tel 923476172"],
            "bad": ["/tel 876172", "/tel juan|quispe|mamani"],
        },
        "telp": {
            "tipo": "Líneas telefónicas premium por DNI o teléfono.",
            "base": "/telp dni_o_telefono",
            "reglas": [
                "Si ingresas DNI, debe tener 8 dígitos.",
                "Si ingresas teléfono, debe tener 9 dígitos.",
                "Solo se permiten números.",
            ],
            "ok": ["/telp 43652762", "/telp 923476172"],
            "bad": ["/telp 876172", "/telp juan"],
        },
        "osiptel": {
            "tipo": "Consulta OSIPTEL por DNI o teléfono.",
            "base": "/osiptel dni_o_telefono",
            "reglas": [
                "Si ingresas DNI, debe tener 8 dígitos.",
                "Si ingresas teléfono, debe tener 9 dígitos.",
                "Solo se permiten números.",
            ],
            "ok": ["/osiptel 43652762", "/osiptel 923476172"],
            "bad": ["/osiptel 876172", "/osiptel perez"],
        },
        "claro": {
            "tipo": "Consulta CLARO por número de teléfono.",
            "base": "/claro telefono",
            "reglas": [
                "El teléfono debe tener 9 dígitos.",
                "Solo se permiten números.",
            ],
            "ok": ["/claro 923476172"],
            "bad": ["/claro 876172", "/claro juan"],
        },
        "entel": {
            "tipo": "Consulta ENTEL por número de teléfono.",
            "base": "/entel telefono",
            "reglas": [
                "El teléfono debe tener 9 dígitos.",
                "Solo se permiten números.",
            ],
            "ok": ["/entel 923476172"],
            "bad": ["/entel 876172", "/entel juan"],
        },
        "fishnm": {
            "tipo": "Fiscalía por nombres y apellidos.",
            "base": "/fishnm nombres|apellidopaterno|apellidomaterno",
            "reglas": [
                "El apellido paterno y materno son obligatorios.",
                "Usa exactamente 3 segmentos separados por '|'.",
                "Si un apellido tiene más de 1 palabra, usa '+'.",
            ],
            "ok": ["/fishnm juan|perez|lopez", "/fishnm juan|del+sol|lopez"],
            "bad": ["/fishnm 876172", "/fishnm juan||"],
        },
        "pla": {
            "tipo": "Búsqueda vehicular por placa.",
            "base": "/pla placa",
            "reglas": [
                "La placa debe tener al menos 5 caracteres.",
                "No uses espacios intermedios.",
            ],
            "ok": ["/pla ABC123", "/pla B7K452"],
            "bad": ["/pla A12", "/pla ABC 123"],
        },
        "sun": {
            "tipo": "Consulta SUNAT por DNI o RUC.",
            "base": "/sun dni_o_ruc",
            "reglas": [
                "DNI: 8 dígitos.",
                "RUC: 11 dígitos.",
            ],
            "ok": ["/sun 43652762", "/sun 10436527621"],
            "bad": ["/sun abcde", "/sun 1234"],
        },
    }

    template = templates.get(command_name)
    if not template:
        args_hint = COMMAND_DEFINITIONS.get(command_name, {}).get("args", "<parametros>")
        return (
            "Por favor, usa el formato correcto.\n\n"
            f"Motivo: {reason}\n"
            f"Intentaste: `{attempted}`\n\n"
            f"Formato base:\n`/{command_name} {args_hint}`\n\n"
            f"{_top_command_usage_help()}"
        )

    reglas = "\n".join(f"- {r}" for r in template["reglas"])
    ejemplos_ok = "\n".join(f"`{x}`" for x in template["ok"])
    ejemplos_bad = "\n".join(f"`{x}`" for x in template["bad"])

    return (
        "Por favor, usa el formato correcto.\n\n"
        f"Motivo: {reason}\n"
        f"Intentaste: `{attempted}`\n\n"
        f"Tipo de búsqueda:\n{template['tipo']}\n\n"
        f"Formato base:\n`{template['base']}`\n\n"
        "Reglas:\n"
        f"{reglas}\n\n"
        "Ejemplos correctos:\n"
        f"{ejemplos_ok}\n\n"
        "Ejemplos incorrectos:\n"
        f"{ejemplos_bad}"
    )


def _unsupported_api_message(command_name: str) -> str:
    return (
        f"⚠️ `/{command_name}` no aparece en la documentación oficial de tu API (v1.7), "
        "por eso devuelve 404/400 o resultados vacíos.\n\n"
        "Comandos RENIEC documentados en ese link:\n"
        "- `/dni`, `/dnif`, `/dnidb`, `/dnifdb` (vía `/persona/reniec` con `source=online|database`)\n\n"
        "Si te habilitan ese módulo en backend, pásame el endpoint/payload exacto y lo activo al toque."
    )


def _parse_command_payload(command_name: str, args: list[str]) -> tuple[bool, str, str, dict]:
    config = API_COMMAND_MAP.get(command_name)
    if not config:
        return False, "Comando sin endpoint configurado aún.", "", {}

    raw = " ".join(args).strip()
    if not args:
        return False, _build_format_error_message(command_name, raw, "Faltan parámetros."), "", {}

    query_type = config["type"]
    payload: dict = {}

    if query_type == "dni":
        value = args[0].strip()
        if not value.isdigit() or len(value) != 8:
            return False, _build_format_error_message(command_name, raw, "El DNI debe tener 8 dígitos numéricos."), "", {}
        payload["dni"] = value
    elif query_type == "placa":
        value = args[0].strip().upper().replace(" ", "")
        if len(value) < 5:
            return False, _build_format_error_message(command_name, raw, "La placa debe tener al menos 5 caracteres."), "", {}
        payload["placa"] = value
    elif query_type == "data":
        if not raw:
            return False, _build_format_error_message(command_name, raw, "Debes ingresar un valor de búsqueda."), "", {}
        payload["data"] = raw
    elif query_type == "documento":
        value = args[0].strip()
        only_digits = value.isdigit()

        if command_name in {"tel", "telp", "osiptel"}:
            if not only_digits:
                return False, _build_format_error_message(command_name, raw, "Solo se permiten números (DNI o teléfono)."), "", {}
            if len(value) == 8:
                payload["documento"] = value
                if "extra" in config:
                    payload.update(config["extra"])
                return True, "OK", config["endpoint"], payload
            if len(value) == 9:
                payload["numero"] = value
                config = dict(config)
                config["endpoint"] = "/telefonia/numero"
                if "extra" in config:
                    payload.update(config["extra"])
                return True, "OK", config["endpoint"], payload
            return False, _build_format_error_message(command_name, raw, "Si ingresas DNI deben ser 8 dígitos. Si ingresas teléfono deben ser 9 dígitos."), "", {}

        payload["documento"] = value
    elif query_type == "numero":
        value = args[0].strip()
        if not value.isdigit() or len(value) != 9:
            return False, _build_format_error_message(command_name, raw, "El número de teléfono debe tener 9 dígitos numéricos."), "", {}
        payload["numero"] = value
    elif query_type == "fiscalia_nombres":
        if len(args) >= 3:
            payload["nombres"] = args[0]
            payload["apepaterno"] = args[1]
            payload["apematerno"] = args[2]
        else:
            parts = raw.split("|")
            if len(parts) != 3:
                return False, _build_format_error_message(command_name, raw, "Debes usar exactamente 3 segmentos separados por '|'."), "", {}
            payload["nombres"] = parts[0].strip()
            payload["apepaterno"] = parts[1].strip()
            payload["apematerno"] = parts[2].strip()

        if not payload["apepaterno"] or not payload["apematerno"]:
            return False, _build_format_error_message(command_name, raw, "El apellido paterno y materno son obligatorios."), "", {}
    elif query_type == "reniec_nombres":
        if len(args) >= 3:
            payload["nombres"] = args[0].strip()
            payload["apepaterno"] = args[1].strip()
            payload["apematerno"] = args[2].strip()
        else:
            parts = [p.strip() for p in raw.split("|")]
            if len(parts) != 3:
                return False, _build_format_error_message(command_name, raw, "Debes usar: nombres|apellidopaterno|apellidomaterno."), "", {}
            payload["nombres"] = parts[0]
            payload["apepaterno"] = parts[1]
            payload["apematerno"] = parts[2]

        if not payload["nombres"] or not payload["apepaterno"] or not payload["apematerno"]:
            return False, _build_format_error_message(command_name, raw, "Nombres y apellidos son obligatorios."), "", {}
    else:
        return False, "Tipo de query no soportado aún.", "", {}

    if "extra" in config:
        payload.update(config["extra"])

    return True, "OK", config["endpoint"], payload


async def unknown_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return

    command_name = _command_from_message_text(update.message.text or "")
    known = list(COMMAND_DEFINITIONS.keys()) + ["start", "cmds", "help", "me", "token_api", "register"]
    if command_name in known:
        return

    suggestions = difflib.get_close_matches(command_name, known, n=3, cutoff=0.55)

    suggestion_text = ""
    if suggestions:
        suggestion_text = "\nQuizá quisiste decir:\n" + "\n".join(f"• /{s}" for s in suggestions)

    await update.message.reply_text(
        "Comando no reconocido.\n\n"
        f"Intentaste: /{command_name}{suggestion_text}\n\n"
        f"{_top_command_usage_help()}"
    )


def _format_generic_api_message(command_name: str, result: dict, remaining_credits: float) -> str:
    pretty = json.dumps(result, ensure_ascii=False, indent=2)
    if len(pretty) > 3200:
        pretty = pretty[:3200] + "\n..."
    return (
        f"✅ Consulta `/{command_name}` exitosa\n\n"
        f"```json\n{pretty}\n```\n\n"
        f"💳 Créditos restantes: **{remaining_credits:.2f}**"
    )


def _format_record_lines(record: dict) -> str:
    lines = []
    for key, value in record.items():
        if isinstance(value, (dict, list)):
            continue
        value_text = str(value).strip()
        if _looks_like_media_value(str(key), value_text):
            continue

        if _is_empty_value(value_text):
            value_text = "-"

        normalized = _normalize_key(key)
        key_text = FIELD_LABELS.get(normalized, str(key).replace("_", " ").upper())
        lines.append(f"{key_text} : {value_text}")
    return "\n".join(lines) if lines else "Sin datos útiles para mostrar."


def _preferred_aliases_for_command(command_name: str) -> list[str]:
    query_type = (API_COMMAND_MAP.get(command_name, {}) or {}).get("type", "")

    base_by_type: dict[str, list[str]] = {
        "dni": [
            "dni", "apellidos", "apellido_paterno", "apellido_materno", "nombres",
            "genero", "edad", "fecha_nacimiento", "estado_civil",
            "direccion", "departamento", "provincia", "distrito",
            "telefono", "correo", "fuente", "periodo",
        ],
        "documento": ["dni", "telefono", "plan_linea", "fuente", "periodo"],
        "numero": ["dni", "telefono", "plan_linea", "fuente", "periodo"],
        "placa": ["placa", "marca", "modelo", "color", "anio", "serie", "motor", "dni", "nombres", "ruc", "razon_social"],
        "data": ["ruc", "razon_social", "condicion", "actividad_economica", "direccion", "departamento", "provincia", "distrito"],
        "fiscalia_nombres": ["nombres", "apellido_paterno", "apellido_materno", "expediente", "delito", "periodo", "fuente"],
    }
    base_fields = base_by_type.get(query_type, ["dni", "nombres", "apellidos", "telefono", "ruc", "razon_social"])

    type_fields: dict[str, list[str]] = {
        "dni": ["digito", "genero", "edad", "fecha_nacimiento", "estado_civil", "padre", "madre"],
        "documento": ["telefono", "plan_linea", "fuente", "periodo", "dni"],
        "numero": ["telefono", "plan_linea", "fuente", "periodo", "dni"],
        "placa": ["marca", "modelo", "color", "anio", "serie", "motor"],
        "data": ["condicion", "actividad_economica"],
        "fiscalia_nombres": ["expediente", "delito", "estado_linea"],
    }

    command_fields: dict[str, list[str]] = {
        "ag": ["dni", "nombres", "apellidos", "parentesco", "fecha_nacimiento", "direccion", "telefono"],
        "agv": ["dni", "nombres", "apellidos", "parentesco", "fecha_nacimiento", "direccion", "telefono"],
        "agvp": ["dni", "nombres", "apellidos", "parentesco", "fecha_nacimiento", "direccion", "telefono"],
        "fam": ["dni", "nombres", "apellidos", "parentesco", "fecha_nacimiento", "direccion"],
        "fam2": ["dni", "nombres", "apellidos", "parentesco", "fecha_nacimiento", "direccion"],
        "fam3": ["dni", "nombres", "apellidos", "parentesco", "fecha_nacimiento", "direccion"],
        "tra": ["dni", "nombres", "empresa", "cargo", "sueldo", "periodo"],
        "sue": ["dni", "nombres", "empresa", "cargo", "sueldo", "periodo"],
        "tremp": ["ruc", "razon_social", "dni", "nombres", "cargo", "sueldo", "periodo"],
        "exd": ["dni", "nombres", "ruc", "razon_social", "empresa", "cargo", "periodo"],
        "con": ["dni", "nombres", "apellidos", "estado_civil", "fecha_nacimiento", "parentesco"],
        "dir": ["dni", "nombres", "direccion", "departamento", "provincia", "distrito", "periodo"],
        "cor": ["dni", "nombres", "correo", "periodo", "fuente"],
        "meta": ["dni", "nombres", "telefono", "correo", "direccion", "empresa", "periodo", "fuente"],
        "sun": ["ruc", "razon_social", "condicion", "direccion", "departamento", "provincia", "distrito", "actividad_economica"],
        "sunr": ["ruc", "razon_social", "condicion", "direccion", "actividad_economica"],
        "claro": ["dni", "telefono", "plan_linea", "fuente", "periodo"],
        "entel": ["dni", "telefono", "plan_linea", "fuente", "periodo"],
        "tel": ["dni", "telefono", "plan_linea", "fuente", "periodo"],
        "telp": ["dni", "telefono", "plan_linea", "fuente", "periodo"],
        "osiptel": ["dni", "telefono", "plan_linea", "fuente", "periodo"],
        "pla": ["placa", "marca", "modelo", "color", "anio", "serie", "motor", "dni", "nombres", "ruc", "razon_social"],
        "pro": ["placa", "marca", "modelo", "color", "anio", "serie", "motor", "dni", "nombres", "ruc", "razon_social"],
    }

    merged: list[str] = []
    seen: set[str] = set()
    for field in [*base_fields, *type_fields.get(query_type, []), *command_fields.get(command_name, [])]:
        norm = _normalize_key(field)
        if norm in seen:
            continue
        seen.add(norm)
        merged.append(field)
    return merged


def _forced_aliases_for_command(command_name: str) -> set[str]:
    if command_name in {"tel", "telp", "osiptel", "claro", "entel"}:
        return {"dni", "telefono", "planlinea", "fuente", "periodo"}
    return set()


def _format_record_lines_for_command(command_name: str, record: dict) -> str:
    preferred = _preferred_aliases_for_command(command_name)
    forced = _forced_aliases_for_command(command_name)
    lines: list[str] = []

    used_alias_norm = {_normalize_key(a) for a in preferred}
    for alias in preferred:
        alias_norm = _normalize_key(alias)
        label = FIELD_LABELS.get(_normalize_key(alias), alias.replace("_", " ").upper())
        value = _value_alias(record, alias, default="-")
        if _is_empty_value(value):
            if alias_norm not in forced:
                continue
            value = "-"
        lines.append(f"<b>{html.escape(label)}</b> : {html.escape(value)}")

    for key, value in record.items():
        if isinstance(value, (dict, list)):
            continue
        value_text = str(value).strip()
        if _looks_like_media_value(str(key), value_text):
            continue
        normalized = _normalize_key(key)
        if _matches_alias_key(normalized, used_alias_norm):
            continue
        if _is_empty_value(value_text):
            continue
        key_text = FIELD_LABELS.get(normalized, str(key).replace("_", " ").upper())
        lines.append(f"<b>{html.escape(key_text)}</b> : {html.escape(value_text)}")

    return "\n".join(lines) if lines else "Sin datos útiles para mostrar."


def _format_api_blocks(command_name: str, result: dict, remaining_credits: float, requester: str) -> list[str]:
    title_overrides = {
        "dni": "RENIEC ONLINE [PREMIUM]",
        "dnif": "RENIEC ONLINE [PREMIUM]",
        "dnidb": "RENIEC DB [PREMIUM]",
        "dnifdb": "RENIEC DB [PREMIUM]",
        "dnim": "RENIEC DNI META [PREMIUM]",
        "nm": "RENIEC NOMBRES [PREMIUM]",
        "mh": "RENIEC MEJORES HUELLAS ONLINE [PREMIUM]",
        "c4": "CERTIFICADO DE INSCRIPCIÓN [PREMIUM]",
        "dnivaz": "DNI VIRTUAL AZUL [PREMIUM]",
        "dnivam": "DNI VIRTUAL AMARILLO [PREMIUM]",
        "dnivel": "DNI VIRTUAL ELECTRONICO [PREMIUM]",
        "dniveln": "DNI VIRTUAL ELECTRONICO NEW [PREMIUM]",
        "fa": "FICHA RENIEC AZUL ONLINE [PREMIUM]",
        "fad": "FICHA RENIEC AZUL DATABASE [PREMIUM]",
        "fadb": "FICHA RENIEC AZUL DATABASE [PREMIUM]",
        "fb": "FICHA RENIEC BLANCO ONLINE [PREMIUM]",
        "fbdb": "FICHA RENIEC BLANCO DATABASE [PREMIUM]",
    }
    command_title = title_overrides.get(
        command_name,
        COMMAND_DEFINITIONS.get(command_name, {}).get("name", command_name.upper()),
    )
    header_tag = BOT_RESPONSE_TAG
    header = f"[{header_tag}] → {command_title}"
    records = _collect_records(result)

    if not records:
        payload = _extract_payload(result)
        if payload:
            body = _format_record_lines_for_command(command_name, payload)
        else:
            body = "Sin resultados para la consulta."
        return [
            f"{header}\n\n{body}\n\nCredits : {remaining_credits:.0f}\nWanted for : {requester}"
        ]

    page_size = 50 if command_name in {"tel", "telp", "osiptel", "claro", "entel"} else 5
    pages = [records[i:i + page_size] for i in range(0, len(records), page_size)]
    messages: list[str] = []
    for idx, page in enumerate(pages, start=1):
        page_blocks = [f"Se encontro {len(records)} resultados.", ""]
        if len(pages) > 1:
            page_blocks.insert(0, "")
            page_blocks.insert(0, f"[{idx}/{len(pages)}]")
        for rec in page:
            page_blocks.append(_format_record_lines_for_command(command_name, rec))
            page_blocks.append("")
        footer = f"Credits : {remaining_credits:.0f}\nWanted for : {requester}"
        messages.append(f"{header}\n\n" + "\n".join(page_blocks).strip() + f"\n\n{footer}")

    return messages


def _decode_data_url(data_url: str) -> tuple[str, bytes, str] | None:
    if not data_url.startswith("data:") or ";base64," not in data_url:
        return None
    try:
        header, b64_data = data_url.split(",", 1)
        mime = header[5:].split(";", 1)[0].lower()
        raw = base64.b64decode(b64_data)
        if "pdf" in mime:
            filename = "reporte.pdf"
        elif "png" in mime:
            filename = "imagen.png"
        elif "jpeg" in mime or "jpg" in mime:
            filename = "imagen.jpg"
        elif "webp" in mime:
            filename = "imagen.webp"
        else:
            filename = "archivo.bin"
        return mime, raw, filename
    except Exception:
        return None


def _decode_raw_base64(value: str, parent_key: str) -> tuple[str, bytes, str] | None:
    text = value.strip().replace("\n", "")
    if len(text) < 200:
        return None
    if not re.fullmatch(r"[A-Za-z0-9+/=]+", text):
        return None
    try:
        padded = text + ("=" * ((4 - len(text) % 4) % 4))
        raw = base64.b64decode(padded, validate=False)
        if len(raw) < 100:
            return None
    except Exception:
        return None

    key_norm = _normalize_key(parent_key)
    if any(token in key_norm for token in ["pdf", "archivo", "documento", "reporte"]):
        return "application/pdf", raw, "reporte.pdf"

    if raw.startswith(b"%PDF"):
        return "application/pdf", raw, "reporte.pdf"
    if raw.startswith(b"\x89PNG"):
        return "image/png", raw, "imagen.png"
    if raw[:3] == b"\xff\xd8\xff":
        return "image/jpeg", raw, "imagen.jpg"

    if any(token in key_norm for token in ["foto", "photo", "image", "img", "rostro", "huella", "firma", "finger", "pulgar", "indice", "anverso", "reverso", "sign"]):
        return "image/jpeg", raw, "imagen.jpg"

    return None


def _extract_media_urls(payload: object) -> tuple[list[str], list[str], list[tuple[str, bytes]], list[tuple[str, bytes]]]:
    photos: list[str] = []
    docs: list[str] = []
    photo_blobs: list[tuple[str, bytes]] = []
    doc_blobs: list[tuple[str, bytes]] = []

    def walk(node, parent_key: str = ""):
        if isinstance(node, dict):
            for key, value in node.items():
                walk(value, str(key))
            return
        if isinstance(node, list):
            for value in node:
                walk(value, parent_key)
            return
        if not isinstance(node, str):
            return

        value = node.strip()
        key_norm = _normalize_key(parent_key)
        key_suggests_pdf = any(token in key_norm for token in ["pdf", "archivo", "documento", "reporte", "report"])
        key_suggests_image = any(token in key_norm for token in ["foto", "photo", "image", "img", "rostro", "huella", "firma", "finger", "pulgar", "indice", "anverso", "reverso", "sign"])

        decoded = _decode_data_url(value)
        if decoded is not None:
            mime, raw, filename = decoded
            if "pdf" in mime:
                doc_blobs.append((filename, raw))
            elif any(t in mime for t in ["image", "png", "jpeg", "jpg", "webp"]):
                photo_blobs.append((filename, raw))
            return

        decoded_raw = _decode_raw_base64(value, parent_key)
        if decoded_raw is not None:
            mime, raw, filename = decoded_raw
            if "pdf" in mime:
                doc_blobs.append((filename, raw))
            else:
                photo_blobs.append((filename, raw))
            return

        if not value.startswith("http"):
            if key_suggests_image and len(value) > 20 and " " not in value:
                photos.append(value)
            elif key_suggests_pdf and len(value) > 20 and " " not in value:
                docs.append(value)
            return

        lower = value.lower()
        if lower.endswith((".jpg", ".jpeg", ".png", ".webp")) or key_suggests_image or any(t in lower for t in ["image", "img", "foto", "rostro", "firma", "huella", "finger", "anverso", "reverso"]):
            photos.append(value)
        elif lower.endswith(".pdf") or key_suggests_pdf or "pdf" in lower:
            docs.append(value)
        else:
            photos.append(value)

    walk(payload)
    return list(dict.fromkeys(photos)), list(dict.fromkeys(docs)), photo_blobs, doc_blobs


def _query_dni_api(dni: str) -> tuple[bool, dict | str]:
    if not LEDER_API_TOKEN:
        return False, "No se configuró `LEDER_API_TOKEN` en variables de entorno."

    url = f"{LEDER_API_BASE_URL}/persona/reniec"
    payload = {
        "dni": dni,
        "source": LEDER_API_SOURCE,
        "token": LEDER_API_TOKEN
    }

    req = request.Request(
        url,
        data=json.dumps(payload).encode('utf-8'),
        headers={'Content-Type': 'application/json'},
        method='POST'
    )

    try:
        with request.urlopen(req, timeout=25) as response:
            body = response.read().decode('utf-8', errors='replace')
            data = json.loads(body) if body else {}
            return True, data
    except error.HTTPError as exc:
        try:
            body = exc.read().decode('utf-8', errors='replace')
            details = json.loads(body) if body else {}
        except Exception:
            details = {}
        message = details.get('message') if isinstance(details, dict) else None
        return False, message or f"Error HTTP {exc.code} al consultar la API."
    except error.URLError:
        return False, "No se pudo conectar a la API. Verifica URL/red."
    except json.JSONDecodeError:
        return False, "La API respondió con un formato no válido."
    except Exception as exc:
        logger.error(f"Error inesperado consultando DNI: {exc}")
        return False, "Ocurrió un error inesperado al consultar la API."


def _format_dni_message(command_name: str, raw_response: dict, queried_dni: str = "") -> str:
    title_overrides = {
        "dni": "RENIEC ONLINE [PREMIUM]",
        "dnif": "RENIEC ONLINE [PREMIUM]",
        "dnidb": "RENIEC DB [PREMIUM]",
        "dnifdb": "RENIEC DB [PREMIUM]",
        "dnim": "RENIEC DNI META [PREMIUM]",
        "mh": "RENIEC MEJORES HUELLAS ONLINE [PREMIUM]",
        "c4": "CERTIFICADO DE INSCRIPCIÓN [PREMIUM]",
    }
    command_title = title_overrides.get(
        command_name,
        COMMAND_DEFINITIONS.get(command_name, {}).get("name", "RENIEC"),
    )

    dni = _value_alias(raw_response, "dni", default=queried_dni or "-")
    if _is_empty_value(dni) and queried_dni:
        dni = queried_dni

    digito = _value_alias(raw_response, "digito", default="")
    nombres = _value_alias(raw_response, "nombres")
    apellido_paterno = _value_alias(raw_response, "apellido_paterno", default="")
    apellido_materno = _value_alias(raw_response, "apellido_materno", default="")
    apellidos_full = _value_alias(raw_response, "apellidos", default="")
    genero = _value_alias(raw_response, "genero")
    edad = _value_alias(raw_response, "edad", default="")

    fecha_nac = _value_alias(raw_response, "fecha_nacimiento")
    departamento = _value_alias(raw_response, "departamento")
    provincia = _value_alias(raw_response, "provincia")
    distrito = _value_alias(raw_response, "distrito")

    grado_instruccion = _value_alias(raw_response, "grado_instruccion")
    estado_civil = _value_alias(raw_response, "estado_civil")
    estatura = _value_alias(raw_response, "estatura")
    fecha_inscripcion = _value_alias(raw_response, "fecha_inscripcion")
    fecha_emision = _value_alias(raw_response, "fecha_emision")
    fecha_caducidad = _value_alias(raw_response, "fecha_caducidad")
    padre = _value_alias(raw_response, "padre")
    madre = _value_alias(raw_response, "madre")
    restriccion = _value_alias(raw_response, "restriccion")

    direccion_dep = _value_alias(raw_response, "direccion_departamento", default=departamento)
    direccion_prov = _value_alias(raw_response, "direccion_provincia", default=provincia)
    direccion_dist = _value_alias(raw_response, "direccion_distrito", default=distrito)
    direccion = _value_alias(raw_response, "direccion")

    ubigeo_reniec = _value_alias(raw_response, "ubigeo_reniec")
    ubigeo_inei = _value_alias(raw_response, "ubigeo_inei")
    ubigeo_sunat = _value_alias(raw_response, "ubigeo_sunat")
    codigo_postal = _value_alias(raw_response, "codigo_postal")

    matrimonio = _value_alias(raw_response, "matrimonio", default="")
    nacimiento_acta = _value_alias(raw_response, "nacimiento_acta", default="")
    defuncion = _value_alias(raw_response, "defuncion", default="")
    cert_nacido = _value_alias(raw_response, "cert_nacido", default="")
    cert_defuncion = _value_alias(raw_response, "cert_defuncion", default="")
    hijos = _value_alias(raw_response, "hijos", default="")

    apellidos_line = f"{apellido_paterno} {apellido_materno}".strip()
    if _is_empty_value(apellidos_line):
        apellidos_line = apellidos_full

    dni_line = dni if _is_empty_value(digito) else f"{dni} - {digito}"
    fecha_nac_line = fecha_nac
    if not _is_empty_value(edad) and not _is_empty_value(fecha_nac):
        fecha_nac_line = f"{fecha_nac} ({edad})"

    lines: list[str] = [f"[{BOT_RESPONSE_TAG}] → {command_title}", ""]
    _append_line(lines, "DNI", dni_line)
    _append_line(lines, "APELLIDOS", apellidos_line)
    _append_line(lines, "NOMBRES", nombres)
    _append_line(lines, "GENERO", genero)

    birth_block: list[str] = []
    _append_line(birth_block, "FECHA NACIMIENTO", fecha_nac_line)
    _append_line(birth_block, "DEPARTAMENTO", departamento)
    _append_line(birth_block, "PROVINCIA", provincia)
    _append_line(birth_block, "DISTRITO", distrito)
    if birth_block:
        lines.extend(["", "[📅] NACIMIENTO", "", *birth_block])

    extra_block: list[str] = []
    _append_line(extra_block, "GRADO INSTRUCCION", grado_instruccion)
    _append_line(extra_block, "ESTADO CIVIL", estado_civil)
    _append_line(extra_block, "ESTATURA", estatura)
    _append_line(extra_block, "FECHA INSCRIPCION", fecha_inscripcion)
    _append_line(extra_block, "FECHA EMISION", fecha_emision)
    _append_line(extra_block, "FECHA CADUCIDAD", fecha_caducidad)
    _append_line(extra_block, "PADRE", padre)
    _append_line(extra_block, "MADRE", madre)
    _append_line(extra_block, "RESTRICCION", restriccion)
    if extra_block:
        lines.extend(["", *extra_block])

    address_block: list[str] = []
    _append_line(address_block, "DEPARTAMENTO", direccion_dep)
    _append_line(address_block, "PROVINCIA", direccion_prov)
    _append_line(address_block, "DISTRITO", direccion_dist)
    _append_line(address_block, "DIRECCION", direccion)
    if address_block:
        lines.extend(["", "[📍] DIRECCION", "", *address_block])

    geo_block: list[str] = []
    _append_line(geo_block, "UBIGEO RENIEC", ubigeo_reniec)
    _append_line(geo_block, "UBIGEO INEI", ubigeo_inei)
    _append_line(geo_block, "UBIGEO SUNAT", ubigeo_sunat)
    _append_line(geo_block, "CODIGO POSTAL", codigo_postal)
    if geo_block:
        lines.extend(["", "[📍] UBICACION", "", *geo_block])

    acts_block: list[str] = []
    _append_line(acts_block, "MATRIMONIO", matrimonio)
    _append_line(acts_block, "NACIMIENTO", nacimiento_acta)
    _append_line(acts_block, "DEFUNCION", defuncion)
    _append_line(acts_block, "CERT. NACIDO", cert_nacido)
    _append_line(acts_block, "CERT. DEFUNCION", cert_defuncion)
    _append_line(acts_block, "HIJOS", hijos)
    if acts_block:
        lines.extend(["", "[🩸] ACTAS / CERTIFICADOS", "", *acts_block])

    return "\n".join(lines).strip()


def _format_dnim_message(raw_response: dict, queried_dni: str = "") -> str:
    dni = _value_alias(raw_response, "dni", default=queried_dni or "-")
    numero_ficha = _value_alias(raw_response, "numero_ficha")
    grupo_votacion = _value_alias(raw_response, "grupo_votacion")
    correo = _value_alias(raw_response, "correo")
    telefono = _value_alias(raw_response, "telefono")

    lines: list[str] = [f"[{BOT_RESPONSE_TAG}] → RENIEC DNI META [PREMIUM]", ""]
    _append_line(lines, "DNI", dni)
    _append_line(lines, "NUMERO FICHA", numero_ficha)
    _append_line(lines, "GRUPO VOTACION", grupo_votacion)
    _append_line(lines, "CORREO", correo)
    _append_line(lines, "TELEFONO", telefono)
    return "\n".join(lines).strip()


def _format_nm_message(raw_response: dict) -> str:
    records = _collect_records(raw_response)
    if not records:
        payload = _extract_payload(raw_response)
        if isinstance(payload, dict) and payload:
            records = [payload]

    lines: list[str] = [f"[{BOT_RESPONSE_TAG}] → RENIEC NOMBRES [PREMIUM]", ""]
    lines.append(f"Se encontro {len(records)} resultado{'s' if len(records) != 1 else ''}.")
    lines.append("")

    for rec in records[:15]:
        dni = _value_alias(rec, "dni")
        digito = _value_alias(rec, "digito", default="")
        apellido_paterno = _value_alias(rec, "apellido_paterno", default="")
        apellido_materno = _value_alias(rec, "apellido_materno", default="")
        apellidos_full = _value_alias(rec, "apellidos", default="")
        nombres = _value_alias(rec, "nombres")
        edad = _value_alias(rec, "edad")

        apellidos = f"{apellido_paterno} {apellido_materno}".strip()
        if _is_empty_value(apellidos):
            apellidos = apellidos_full

        dni_line = dni if _is_empty_value(digito) else f"{dni} - {digito}"
        _append_line(lines, "DNI", dni_line)
        _append_line(lines, "APELLIDOS", apellidos)
        _append_line(lines, "NOMBRES", nombres)
        _append_line(lines, "EDAD", edad)
        lines.append("")

    lines.append("~ Puedes visualizar la foto de una coincidencia antes de usar /dni ~")
    return "\n".join(lines).strip()

# =================================================================
# 3. Handlers de Inicio y Login
# =================================================================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Muestra el mensaje de bienvenida."""
    if _is_duplicate_command(update):
        return ConversationHandler.END

    if update.effective_user:
        user_id = int(update.effective_user.id)
        now_ts = time.monotonic()
        last_ts = USER_LAST_START_TS.get(user_id)
        if last_ts is not None and (now_ts - last_ts) < 2:
            return ConversationHandler.END
        USER_LAST_START_TS[user_id] = now_ts

    user_name = update.effective_user.first_name if update.effective_user else "usuario"

    message = (
        f"Hola {user_name}, bienvenido a tu bot favorito 🤖 !\n\n"
        "====================\n\n"
        "[💻] COMANDOS BASICOS :\n\n"
        "[🔰] Para registrarte usa /register (5 créditos gratis)\n"
        "[🛠️] Para ver los todos los comandos usa /cmds\n"
        "[❔] Para ver las preguntas frecuentes usa /faq\n"
        "[📱] Para ver tu perfil usa /me\n"
        "[🔑] Para usar nuestras apis usa /token_api\n\n"
        "====================\n\n"
        "[👥] UNETE A NUESTRA COMUNIDAD :\n\n"
        "[🚀] Unete a nuestro grupo publico Unirse.\n"
        "[🌐] Visita nuestra web oficial : https://torresshophacks.com\n"
        f"[🔥] Colabora con nosotros, escríbenos al {BUY_CREDITS_CONTACT}\n"
        f"[💰] Compra créditos o plan ilimitado al WhatsApp {BUY_CREDITS_CONTACT}\n\n"
        "====================\n\n"
        "[🆓] ¿QUIERES CREDITOS GRATIS? :\n\n"
        "- Unete al GRUPO PUBLICO (CLICK PARA UNIRTE) y al CANAL DE RESPALDOS (CLICK PARA UNIRTE) para ganar 10 creditos.\n\n"
        "IMPORTANTE : Visita nuestros enlaces de respaldos para SIEMPRE tener acceso a leder, incluso si este bot se elimina en un futuro.\n\n"
        "Enlace permanente : CLICK AQUI\n"
        "Enlace temporal : CLICK AQUI"
    )

    if await _send_command_image(
        update,
        "start",
        caption=_fit_caption(message),
        reply_markup=ReplyKeyboardRemove(),
    ):
        return ConversationHandler.END

    await update.message.reply_text(
        message,
        reply_markup=ReplyKeyboardRemove()
    )
    return ConversationHandler.END


async def register_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if _is_duplicate_command(update):
        return

    telegram_user = update.effective_user
    existing = _get_logged_user(telegram_user.id)

    if existing:
        await update.message.reply_text(
            f"[✅] {existing.username} ya está registrado.\n\n"
            f"[⚡] Plan : {_normalize_plan(existing.plan)}\n"
            f"[🔐] Rol : {'ADMIN' if existing.es_admin else 'Cliente'}\n"
            f"[💰] Créditos : {existing.saldo:.0f}",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Comprar créditos 💰", url=BUY_CREDITS_URL)]])
        )
        return

    usuario = _ensure_registered_user(telegram_user)
    await update.message.reply_text(
        f"[✅] {usuario.username} registrado correctamente, para darte la bienvenida acabo de agregar 5 creditos a tu cuenta.\n\n"
        f"[⚡] Plan : FREE\n"
        f"[🔐] Rol : Cliente",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Comprar créditos 💰", url=BUY_CREDITS_URL)]])
    )


async def help_api(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if _is_duplicate_command(update):
        return

    await update.message.reply_text(
        "📘 **Comandos disponibles**\n\n"
        "• `/cmds` → abre el centro de comandos\n"
        "• `/dni <numero>` → consulta datos por DNI\n"
        "Ejemplo: `/dni 47618138`",
        parse_mode='Markdown'
    )


async def faq_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if _is_duplicate_command(update):
        return

    faq_text = (
        "[#TORRES_DOX] → FAQ\n\n"
        "[📌] ¿La informacion es en tiempo real?\n\n"
        "Si, excepto lineas telefonicas que tambien mostramos data historica, a que me refiero es que se envía todas las lineas que figuran con dicha persona.\n\n"
        "[📌] ¿Que son los créditos?\n\n"
        "Los créditos son el tipo de moneda/saldo la cual se utilizan para realizar consultas en el bot.\n\n"
        "[📌] ¿Cuando se consumen mis creditos y cuanto consume cada consulta?\n\n"
        "Los créditos se gastan por consulta exitosa (consulta enviada) los costos de consultas lo puedes ver con /cmds apartado de doxing.\n\n"
        "[📌] ¿Puedo usar el bot sin comprar creditos?\n\n"
        "Claro! Contamos con comandos de búsqueda gratuitos pero la información que se envía es basica.\n\n"
        "[📌] ¿Como uso los créditos?\n\n"
        "Es muy fácil, puedes ir a /cmds y veras cada comando con una breve explicación.\n\n"
        "[📌] ¿Por qué el bot no responde?\n\n"
        "En algunos casos el bot se llega a saturar por las consultas excesivas que hacen los usuarios en modo (FREE) el bot en unos minutos vuelve a responder y envia la consulta.\n\n"
        "[📌] ¿Por qué sale que está en mantenimiento?\n\n"
        "El bot entra en mantenimiento constantemente para poder mejorar la seguridad y mejorar la velocidad de respuesta de las consultas en algunas ocasiones puede demorar mucho como poco.\n\n"
        "[📌] ¿Porque bot no envía mi consulta?\n\n"
        "En algunos casos los comandos para realizar las búsquedas tales como (/dni) y/o (/dnif) llegan a saturarse y dejan de responder pero no es problema, en cierto tiempo el bot respondera y enviará tu consulta.\n\n"
        "[📌] ¿Los créditos vencen?\n\n"
        "No, los creditos no tiene fecha de vencimiento, se finalizan solo cuando usas el total de tus créditos.\n\n"
        "[📌] ¿Como funciona el plan ilimitado?\n\n"
        "Podras realizar todas las consultas que quieras dentro del tiempo contratado, cuando expire tu plan podras usar tus créditos."
    )
    await update.message.reply_text(faq_text)


async def token_api_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if _is_duplicate_command(update):
        return

    text = (
        "[#TORRES_DOX]\n\n"
        "Tu token API personal se habilita por usuario.\n"
        "Solicítalo al administrador para usar las APIs externas."
    )
    if await _send_command_image(update, "token_api", caption=_fit_caption(text)):
        return
    await update.message.reply_text(text)


async def me_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if _is_duplicate_command(update):
        return

    telegram_user = update.effective_user
    user_id_telegram = telegram_user.id
    alias = telegram_user.username or (telegram_user.first_name or "usuario").lower()

    with get_session() as session_db:
        usuario = session_db.query(Usuario).filter_by(telegram_id=user_id_telegram).first()

    if not usuario:
        usuario = _ensure_registered_user(telegram_user)

    antispam_seconds = _plan_antispam_seconds(usuario.plan)
    desde_text = usuario.fecha_registro.strftime("%Y-%m-%d %H:%M:%S") if usuario.fecha_registro else "-"

    profile = (
        "[#TORRES_DOX]\n\n"
        f"PERFIL COMPLETO DE {telegram_user.first_name or usuario.username}\n\n"
        "DATOS GENERALES\n\n"
        f"[🧑] ID → {user_id_telegram}\n"
        f"[📄] NOMBRES → {telegram_user.first_name or usuario.username}\n"
        f"[⚡] ALIAS → {alias}\n"
        f"[💰] CREDITOS → {usuario.saldo:.0f}\n"
        f"[⏱] ANTI-SPAM →  {antispam_seconds}'\n"
        f"[�] DESDE →  {desde_text}\n"
        f"[��️] ROL → {'ADMIN' if usuario.es_admin else 'CLIENTE'}\n"
        f"[📈] PLAN → {_normalize_plan(usuario.plan)}\n"
        f"[🎭] ESTADO → {usuario.estado or 'ACTIVO'}\n\n"
        "API DE INTEGRACIÓN\n\n"
        "[🗣️] Ver tu token personal → /token_api\n\n"
        "PROGRAMA DE REFERIDOS\n\n"
        "[🗣️] CANTIDAD REFERIDOS → 0\n"
        f"[🔗] LINK REFERIDOS → https://t.me/{context.bot.username}?start={user_id_telegram}\n\n"
        "PROGRAMA DE RECOMPENSAS\n\n"
        "[🎁] Tienes recompensas disponibles 🥳, reclamalas con /recompensas\n\n"
        "Visualiza tus compras con el comando /compras"
    )

    if await _send_command_image(update, "me", caption=_fit_caption(profile)):
        return

    await update.message.reply_text(profile)


async def compras_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if _is_duplicate_command(update):
        return

    telegram_user = update.effective_user
    user_id_telegram = telegram_user.id

    with get_session() as session_db:
        usuario = session_db.query(Usuario).filter_by(telegram_id=user_id_telegram).first()

        if not usuario:
            usuario = _ensure_registered_user(telegram_user)

        compras = (
            session_db.query(Compra)
            .filter_by(usuario_id=usuario.id)
            .order_by(Compra.fecha.desc())
            .limit(10)
            .all()
        )

    if not compras:
        message = (
            "[#TORRES_DOX]\n\n"
            f"COMPRAS DEL USUARIO → {user_id_telegram}\n\n"
            "Se encontro 0 resultados.\n\n"
            "NRO VENTA : SIN REGISTROS\n"
            "ESTADO : -\n"
            "VENDEDOR : -\n"
            "TIPO : CRÉDITOS\n"
            "PLAN : -\n"
            "CANTIDAD : 0\n"
            "RECOMPENSA : -\n"
            "FECHA : -\n"
            "DETALLE : -"
        )
        await update.message.reply_text(message)
        return

    lines = [
        "[#TORRES_DOX]",
        "",
        f"COMPRAS DEL USUARIO → {user_id_telegram}",
        "",
        f"Se encontro {len(compras)} resultado(s).",
    ]

    for compra in compras:
        fecha_texto = compra.fecha.strftime("%Y-%m-%d %H:%M:%S") if compra.fecha else "-"
        lines.extend([
            "",
            f"NRO VENTA : {compra.nro_venta}",
            f"ESTADO : {compra.estado or '-'}",
            f"VENDEDOR : {compra.vendedor or '-'}",
            f"TIPO : {compra.tipo or '-'}",
            f"PLAN : {getattr(compra, 'plan', '-') or '-'}",
            f"CANTIDAD : {float(compra.cantidad):.2f}",
            f"RECOMPENSA : {compra.recompensa or '-'}",
            f"FECHA : {fecha_texto}",
            f"DETALLE : {compra.detalle or '-'}",
            "------------------------------",
        ])

    await update.message.reply_text("\n".join(lines))


async def recompensas_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if _is_duplicate_command(update):
        return

    user_id_telegram = update.effective_user.id
    text = (
        f"Felicidades {update.effective_user.first_name} [{user_id_telegram}] 🎉! , revisamos tus compras y por tu preferencia te recompensamos con:\n\n"
        "RECOMPENSA : 0 CRÉDITOS 🎖️ !\n\n"
        "Si realizas mas compras obtendras mas recompensas."
    )
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("Comprar créditos 💰", url=BUY_CREDITS_URL)],
        [InlineKeyboardButton("Unete a nuestro grupo publico 🔥", url="https://t.me/")]
    ])
    await update.message.reply_text(text, reply_markup=keyboard)


async def cmds_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if _is_duplicate_command(update):
        return

    text = (
        "[#TORRES_DOX]\n\n"
        "Iniciaste el centro de comandos correctamente.\n\n"
        "Está dividido en apartados según categorías de comandos.\n\n"
        "Para navegar, haz clic en los botones de la categoría que desees."
    )
    if await _send_command_image(
        update,
        "cmds",
        caption=_fit_caption(text),
        reply_markup=_build_cmds_home_keyboard(),
    ):
        return
    await update.message.reply_text(text, reply_markup=_build_cmds_home_keyboard())


async def cmds_button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()

    data = query.data or ""
    if data == "cmds:close":
        await query.edit_message_reply_markup(reply_markup=None)
        return

    if data == "cmds:home":
        text = (
            "[#TORRES_DOX]\n\n"
            "Iniciaste el centro de comandos correctamente.\n\n"
            "Está dividido en apartados según categorías de comandos.\n\n"
            "Para navegar, haz clic en los botones de la categoría que desees."
        )
        await _edit_cmds_message(query, text, _build_cmds_home_keyboard())
        return

    parts = data.split(":")
    if len(parts) == 4 and parts[0] == "cmds" and parts[1] == "cat":
        category_index = int(parts[2])
        page_index = int(parts[3])

        if category_index < 0 or category_index >= len(CATEGORY_ORDER):
            return

        category_name = CATEGORY_ORDER[category_index]
        pages = CATEGORY_PAGES.get(category_name, [[]])
        if page_index < 0 or page_index >= len(pages):
            page_index = 0

        text = _build_category_page_text(category_name, page_index)
        keyboard = _build_category_page_keyboard(category_index, page_index)
        await _edit_cmds_message(query, text, keyboard)


def _command_from_message_text(text: str) -> str:
    if not text or not text.strip():
        return ""
    token = text.strip().split()[0]
    token = token.split("@")[0]
    return token.lstrip("/").lower()


def _is_duplicate_command(update: Update) -> bool:
    if not update.message or not update.effective_chat:
        return False

    command_name = _command_from_message_text(update.message.text or "")
    if not command_name:
        return False

    now_ts = time.monotonic()
    for key, seen_ts in list(PROCESSED_COMMAND_MESSAGES.items()):
        if (now_ts - seen_ts) > COMMAND_DEDUP_SECONDS:
            PROCESSED_COMMAND_MESSAGES.pop(key, None)

    key = (int(update.effective_chat.id), command_name)
    last_ts = PROCESSED_COMMAND_MESSAGES.get(key)
    if last_ts is not None and (now_ts - last_ts) <= COMMAND_DEDUP_SECONDS:
        return True

    PROCESSED_COMMAND_MESSAGES[key] = now_ts
    return False


async def catalog_command_router(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if _is_duplicate_command(update):
        return

    command_name = _command_from_message_text(update.message.text or "")
    command_info = COMMAND_DEFINITIONS.get(command_name)
    if not command_info:
        return

    command_cost = float(command_info.get('cost', 0) or 0)

    if command_name in POSTMAN_V17_UNSUPPORTED_COMMANDS:
        await update.message.reply_text(
            _unsupported_api_message(command_name),
            parse_mode='Markdown',
            reply_to_message_id=update.message.message_id,
            allow_sending_without_reply=True,
        )
        return

    user = _get_logged_user(update.effective_user.id)
    if not user:
        await update.message.reply_text("❌ Debes registrarte primero con `/register`.", parse_mode='Markdown')
        return

    required_plan = command_info.get('plan', 'FREE')
    if not _has_minimum_plan(user.plan, required_plan):
        await update.message.reply_text(
            f"❌ Necesitas plan `{_normalize_plan(required_plan)}` para usar `/{command_name}`. Tu plan actual es `{_normalize_plan(user.plan)}`.",
            parse_mode='Markdown',
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Comprar créditos 💰", url=BUY_CREDITS_URL)]])
        )
        return

    user_plan = _normalize_plan(user.plan)
    cooldown_seconds = _plan_antispam_seconds(user_plan)
    user_id = int(update.effective_user.id)
    now_ts = time.monotonic()
    last_ts = USER_LAST_COMMAND_TS.get(user_id)
    if last_ts is not None:
        elapsed = now_ts - last_ts
        if elapsed < cooldown_seconds:
            wait_seconds = int(math.ceil(cooldown_seconds - elapsed))
            await update.message.reply_text(
                f"⛔ ANTI-SPAM ({user_plan}: {cooldown_seconds}s) - INTENTA DESPUES DE {wait_seconds}s"
            )
            return

    if command_name not in API_COMMAND_MAP:
        await update.message.reply_text(
            f"⚠️ /{command_name} está en mantenimiento o sin endpoint activo.\n\n"
            f"{_build_format_error_message(command_name, ' '.join(context.args).strip(), 'Comando temporalmente no disponible en API_COMMAND_MAP.')}"
        )
        return

    ok_parse, parse_message, endpoint, payload = _parse_command_payload(command_name, context.args)
    if not ok_parse:
        await update.message.reply_text(f"❌ {parse_message}")
        return

    has_credit, credit_message = _has_credits(update.effective_user.id, command_cost)
    if not has_credit:
        await update.message.reply_text(
            f"❌ {credit_message}",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Comprar créditos 💰", url=BUY_CREDITS_URL)]])
        )
        return

    USER_LAST_COMMAND_TS[user_id] = now_ts

    wait_message = await update.message.reply_text(
        f"🔎 Consultando /{command_name} en la API...",
        reply_to_message_id=update.message.message_id,
        allow_sending_without_reply=True,
    )
    ok, result = await asyncio.to_thread(_query_api_with_fallback, command_name, endpoint, payload)

    if not ok:
        await wait_message.edit_text(f"❌ {result}")
        return

    if not isinstance(result, dict):
        await wait_message.edit_text("❌ La API no devolvió una respuesta válida.")
        return

    can_pay, message, remaining = _charge_credits(update.effective_user.id, command_cost)
    if not can_pay:
        await wait_message.edit_text(f"❌ {message}")
        return

    photos, docs, photo_blobs, doc_blobs = _extract_media_urls(result)

    document_image_commands = {"dnivaz", "dnivam", "dnivel", "dniveln", "fa", "fad", "fadb", "fb", "fbdb"}
    if command_name in document_image_commands:
        docs.extend(photos)
        photos = []
        doc_blobs.extend(photo_blobs)
        photo_blobs = []

    command_photo_limit = {
        "dni": 1,
        "dnidb": 1,
        "dnif": 4,
        "dnifdb": 4,
    }
    max_photos = command_photo_limit.get(command_name, 5)

    command_doc_limit = {
        "dni": 0,
        "dnidb": 0,
        "dnif": 0,
        "dnifdb": 0,
        "dnivaz": 2,
        "dnivam": 2,
        "dnivel": 2,
        "dniveln": 2,
        "fa": 1,
        "fad": 1,
        "fadb": 1,
        "fb": 1,
        "fbdb": 1,
    }
    max_docs = command_doc_limit.get(command_name, 3)

    response_hint = str(command_info.get("response", "")).lower()
    wants_pdf = any(token in response_hint for token in ["pdf", "documento pdf", "reporte", "certificado"])
    wants_image = any(token in response_hint for token in ["imagen", "imagenes", "foto", "fotos", "anverso", "reverso", "rostro", "huella", "firma", "acta original"])
    wants_text_only = "texto" in response_hint and not wants_pdf and not wants_image
    document_image_commands = {"dnivaz", "dnivam", "dnivel", "dniveln", "fa", "fad", "fadb", "fb", "fbdb"}

    if command_name == "dnim":
        max_photos = max(max_photos, 1)
        max_docs = 0
    elif command_name in document_image_commands:
        max_photos = 0
        max_docs = max(max_docs, 1)
    elif wants_text_only:
        max_docs = 0
        max_photos = 0
    elif wants_pdf and not wants_image:
        max_photos = 0
    elif wants_image and not wants_pdf:
        max_docs = 0

    if max_docs == 0:
        docs = []
        doc_blobs = []
    if max_photos >= 0:
        photos = photos[:max_photos]
        photo_blobs = photo_blobs[:max_photos]

    for doc_url in docs[:max_docs]:
        try:
            await context.bot.send_document(
                chat_id=update.effective_chat.id,
                document=doc_url,
                reply_to_message_id=update.message.message_id,
            )
        except Exception as exc:
            logger.warning(f"No se pudo enviar PDF {doc_url}: {exc}")

    for filename, raw in doc_blobs[:max_docs]:
        try:
            await context.bot.send_document(
                chat_id=update.effective_chat.id,
                document=InputFile(BytesIO(raw), filename=filename),
                reply_to_message_id=update.message.message_id,
            )
        except Exception as exc:
            logger.warning(f"No se pudo enviar PDF base64 {filename}: {exc}")

    if command_name in {"dni", "dnif", "dnidb", "dnifdb"}:
        queried_dni = context.args[0].strip() if context.args else ""
        requester = update.effective_user.first_name or user.username
        formatted_text = _format_dni_message(command_name, result, queried_dni=queried_dni)
        formatted_text += f"\n\nCredits : {remaining:.0f}\nWanted for : {requester}"
        formatted_blocks = [formatted_text]
    elif command_name == "dnim":
        queried_dni = context.args[0].strip() if context.args else ""
        requester = update.effective_user.first_name or user.username
        formatted_text = _format_dnim_message(result, queried_dni=queried_dni)
        formatted_text += f"\n\nCredits : {remaining:.0f}\nWanted for : {requester}"
        formatted_blocks = [formatted_text]
    elif command_name == "nm":
        requester = update.effective_user.first_name or user.username
        formatted_text = _format_nm_message(result)
        formatted_text += f"\n\nCredits : {remaining:.0f}\nWanted for : {requester}"
        formatted_blocks = [formatted_text]
    else:
        requester = update.effective_user.first_name or user.username
        formatted_blocks = _format_api_blocks(command_name, result, remaining, requester)
        formatted_text = "\n\n".join(formatted_blocks)

    has_any_photo = bool(photos or photo_blobs)
    if has_any_photo:
        try:
            await wait_message.delete()
        except Exception:
            pass

        caption_text = _fit_caption(formatted_text)

        if command_name == "dnim":
            sent_photo = False
            if photos:
                first_photo = photos.pop(0)
                try:
                    await update.message.reply_photo(
                        photo=first_photo,
                        reply_to_message_id=update.message.message_id,
                            )
                    sent_photo = True
                except Exception as exc:
                    logger.warning(f"No se pudo enviar imagen dnim {first_photo}: {exc}")

            if not sent_photo and photo_blobs:
                first_name, first_raw = photo_blobs.pop(0)
                try:
                    await update.message.reply_photo(
                        photo=InputFile(BytesIO(first_raw), filename=first_name),
                        reply_to_message_id=update.message.message_id,
                            )
                    sent_photo = True
                except Exception as exc:
                    logger.warning(f"No se pudo enviar imagen dnim base64 {first_name}: {exc}")

            await update.message.reply_text(
                formatted_text,
                parse_mode='HTML',
                reply_to_message_id=update.message.message_id,
            )
            return

        if command_name in {"dnif", "dnifdb"}:
            media_group: list[InputMediaPhoto] = []

            for idx, photo_url in enumerate(photos):
                if idx == 0:
                    media_group.append(InputMediaPhoto(media=photo_url, caption=caption_text, parse_mode='HTML'))
                else:
                    media_group.append(InputMediaPhoto(media=photo_url))

            for filename, raw in photo_blobs:
                input_file = InputFile(BytesIO(raw), filename=filename)
                if not media_group:
                    media_group.append(InputMediaPhoto(media=input_file, caption=caption_text, parse_mode='HTML'))
                else:
                    media_group.append(InputMediaPhoto(media=input_file))

            if media_group:
                try:
                    await context.bot.send_media_group(
                        chat_id=update.effective_chat.id,
                        media=media_group,
                        reply_to_message_id=update.message.message_id,
                            )
                    return
                except Exception as exc:
                    logger.warning(f"No se pudo enviar álbum de imágenes {command_name}: {exc}")

        sent_primary = False

        if photos:
            first_photo = photos.pop(0)
            try:
                await update.message.reply_photo(
                    photo=first_photo,
                    caption=caption_text,
                    parse_mode='HTML',
                    reply_to_message_id=update.message.message_id,
                    )
                sent_primary = True
            except Exception as exc:
                logger.warning(f"No se pudo enviar imagen principal {first_photo}: {exc}")

        if not sent_primary and photo_blobs:
            first_name, first_raw = photo_blobs.pop(0)
            try:
                await update.message.reply_photo(
                    photo=InputFile(BytesIO(first_raw), filename=first_name),
                    caption=caption_text,
                    parse_mode='HTML',
                    reply_to_message_id=update.message.message_id,
                    )
                sent_primary = True
            except Exception as exc:
                logger.warning(f"No se pudo enviar imagen principal base64 {first_name}: {exc}")

        if not sent_primary:
            await update.message.reply_text(
                formatted_text,
                parse_mode='HTML',
                reply_to_message_id=update.message.message_id,
            )
            return

        for photo_url in photos:
            try:
                await context.bot.send_photo(
                    chat_id=update.effective_chat.id,
                    photo=photo_url,
                    reply_to_message_id=update.message.message_id,
                    )
            except Exception as exc:
                logger.warning(f"No se pudo enviar imagen {photo_url}: {exc}")

        for filename, raw in photo_blobs:
            try:
                await context.bot.send_photo(
                    chat_id=update.effective_chat.id,
                    photo=InputFile(BytesIO(raw), filename=filename),
                    reply_to_message_id=update.message.message_id,
                    )
            except Exception as exc:
                logger.warning(f"No se pudo enviar imagen base64 {filename}: {exc}")
        return

    if command_name in {"dni", "dnif", "dnidb", "dnifdb"}:
        await wait_message.edit_text(formatted_blocks[0], parse_mode='HTML')
        return

    await wait_message.edit_text(formatted_blocks[0], parse_mode='HTML')
    for block in formatted_blocks[1:]:
        await update.message.reply_text(block, parse_mode='HTML')

async def show_login_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Pide al usuario que ingrese las credenciales."""
    await update.message.reply_text(
        "🔒 Enter the credentials provided by the administrator in the following format:\n\n"
        "**LOGIN PASSWORD**",
        parse_mode='Markdown',
        reply_markup=ReplyKeyboardRemove()
    )
    return LOGIN_KEY

async def handle_login_key(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Procesa el login_key y la contraseña ingresada."""
    text = update.message.text
    
    if text == "🔒 Login":
        return await show_login_prompt(update, context) 

    parts = text.split()
    
    session_db = get_session()
    try:
        if len(parts) != 2:
            await update.message.reply_text(
                "❌ Format error. Please use: `LOGIN PASSWORD`",
                parse_mode='Markdown'
            )
            return LOGIN_KEY

        username, login_key_input = parts
        user_id_telegram = update.effective_user.id

        usuario = session_db.query(Usuario).filter_by(username=username, login_key=login_key_input).first()

        if usuario:
            if usuario.telegram_id is None:
                if session_db.query(Usuario).filter_by(telegram_id=user_id_telegram).first() is None:
                    usuario.telegram_id = user_id_telegram
                    session_db.commit()
                else:
                    await update.message.reply_text(
                        "❌ Tu ID de Telegram ya está en uso. Desloguea la cuenta anterior o contacta al administrador."
                    )
                    return LOGIN_KEY

            await update.message.reply_text(
                "✅ **You have been successfully authorized!**",
                parse_mode='Markdown',
                reply_markup=get_keyboard_main(True)
            )
            return ConversationHandler.END
        else:
            await update.message.reply_text(
                "❌ Login failed. Incorrect credentials or user not found. Try again, or type /start."
            )
            return LOGIN_KEY
    except Exception as e:
        logger.error(f"Error en handle_login_key: {e}")
        session_db.rollback()
        await update.message.reply_text("Ha ocurrido un error inesperado. Intenta de nuevo o usa /start.")
        return ConversationHandler.END
    finally:
        session_db.close()

async def logout(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text("ℹ️  Usa /register para crear cuenta automáticamente.")
        
async def show_account(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Muestra la información de la cuenta."""
    user_id_telegram = update.effective_user.id
    
    with get_session() as session_db:
        usuario = session_db.query(Usuario).filter_by(telegram_id=user_id_telegram).first()
    
    if usuario:
        message = (
            f"👤 **Your account:**\n"
            f"• Login: **{usuario.username}**\n"
            f"• Créditos: **{usuario.saldo:.2f}**\n\n"
            f"// Historial de compras/recargas no implementado //"
        )
        
        await update.message.reply_text(
            message,
            parse_mode='Markdown'
        )
    else:
        await update.message.reply_text("❌ Debes registrarte primero con /register.")
        
# =================================================================
# 4. Handlers de Compra (Buy keys) - Lógica de Inventario
# =================================================================

async def show_buy_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Muestra las categorías de productos."""
    user_id_telegram = update.effective_user.id
    
    with get_session() as session_db:
        usuario = session_db.query(Usuario).filter_by(telegram_id=user_id_telegram).first()
        
        if not usuario:
            await update.message.reply_text("❌ Please log in first.")
            return ConversationHandler.END

        categorias = session_db.query(Producto.categoria).distinct().all()
    
    keyboard_rows = []
    for cat_tuple in categorias:
        categoria = cat_tuple[0]
        if categoria: 
            keyboard_rows.append([KeyboardButton(categoria)])
            
    keyboard_rows.append([KeyboardButton("Back")]) 

    reply_markup = ReplyKeyboardMarkup(keyboard_rows, resize_keyboard=True, one_time_keyboard=False)

    await update.message.reply_text(
        "Choose a category:",
        reply_markup=reply_markup
    )
    return BUY_CATEGORY

async def handle_category_selection(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Maneja la selección de la categoría y muestra los productos y acciones."""
    category = update.message.text
    
    if category == "Back":
        return await start(update, context) 

    with get_session() as session_db:
        productos = session_db.query(Producto).filter_by(categoria=category).all()

    if not productos:
        await update.message.reply_text(f"❌ No products found in category: **{category}**", parse_mode='Markdown')
        return BUY_CATEGORY

    context.user_data['selected_category'] = category

    product_keys = []
    
    for producto in productos:
        with get_session() as s:
            stock = s.query(Key).filter(Key.producto_id == producto.id, Key.estado == 'available').count()
        
        button_text = f"{producto.nombre} - ${producto.precio:.2f} (Stock: {stock})"
        product_keys.append([KeyboardButton(button_text)])
            
    product_keys.append([KeyboardButton("Go back")])
    
    reply_markup = ReplyKeyboardMarkup(product_keys, resize_keyboard=True, one_time_keyboard=False)
    
    await update.message.reply_text(
        f"Choose a product in category {category}:",
        reply_markup=reply_markup
    )
    return BUY_PRODUCT


async def handle_final_purchase(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Procesa las selecciones de compra (Buy)."""
    text = update.message.text
    user_id_telegram = update.effective_user.id
    
    if text == "Go back":
        return await show_buy_menu(update, context)
    
    session_db = get_session()
    try:
        parts = text.rsplit(' - $', 1) 
        if len(parts) != 2:
            raise ValueError("Invalid product format.")
            
        product_name = parts[0].strip()
        price_str = parts[1].split('(')[0].strip() 
        price = float(price_str.replace('$', '').replace(',', '.'))
        
        usuario = session_db.query(Usuario).filter_by(telegram_id=user_id_telegram).first()
        producto = session_db.query(Producto).filter_by(nombre=product_name).first()

        if not usuario or not producto:
            await update.message.reply_text("❌ Error interno: Usuario o producto no encontrado.", reply_markup=get_keyboard_main(True))
            return ConversationHandler.END

        # 1. Verificar Saldo
        if usuario.saldo < price:
            await update.message.reply_text(f"❌ Saldo insuficiente. Tu saldo es: ${usuario.saldo:.2f}", reply_markup=update.message.reply_markup)
            return BUY_PRODUCT
            
        # 2. Buscar Key Disponible (Inventario)
        available_key = session_db.query(Key).filter_by(
            producto_id=producto.id, 
            estado='available'
        ).with_for_update().first() 

        if not available_key:
            await update.message.reply_text(f"❌ Producto agotado. No hay claves disponibles para {producto.nombre}.", reply_markup=update.message.reply_markup)
            return BUY_PRODUCT
            
        # 3. Realizar la Transacción
        usuario.saldo -= price
        available_key.estado = 'used'
        
        session_db.commit()

        # 4. Éxito y Entrega de Clave
        await update.message.reply_text(
            f"🎉 **Compra Exitosa de {producto.nombre}!**\n"
            f"Costo: **${price:.2f}**\n"
            f"Tu nuevo saldo: **${usuario.saldo:.2f}**\n\n"
            f"🔐 **Tu Key/Licencia:** `{available_key.licencia}`", 
            parse_mode='Markdown'
        )
        return await start(update, context)

    except ValueError:
        await update.message.reply_text("❌ Error al procesar la selección. Intenta de nuevo.", reply_markup=update.message.reply_markup)
        return BUY_PRODUCT
    except Exception as e:
        logger.error(f"Error en la transacción: {e}")
        session_db.rollback()
        await update.message.reply_text("❌ Ocurrió un error en la compra. Intenta de nuevo o usa /start.")
        return ConversationHandler.END
    finally:
        session_db.close()
            
    await update.message.reply_text("Opción no válida. Elige una de las opciones del menú.", reply_markup=update.message.reply_markup)
    return BUY_PRODUCT


# =================================================================
# 5. Función Principal de Ejecución
# =================================================================

def main() -> None:
    """Ejecuta el bot."""
    application = (
        Application.builder()
        .token(TOKEN)
        .request(HTTPXRequest(connect_timeout=20, read_timeout=20, write_timeout=20, pool_timeout=20))
        .build()
    )

    # Handlers de comandos y botones de texto simples
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("register", register_command))
    application.add_handler(CommandHandler("help", help_api))
    application.add_handler(CommandHandler("cmds", cmds_command))
    application.add_handler(CommandHandler("faq", faq_command))
    application.add_handler(CommandHandler("me", me_command))
    application.add_handler(CommandHandler("compras", compras_command))
    application.add_handler(CommandHandler("recompensas", recompensas_command))
    application.add_handler(CommandHandler("token_api", token_api_command))
    application.add_handler(CommandHandler("logout", logout))
    application.add_handler(CallbackQueryHandler(cmds_button_handler, pattern=r"^cmds:"))

    # Registro de todos los comandos del catálogo
    for cmd_name in COMMAND_DEFINITIONS.keys():
        application.add_handler(CommandHandler(cmd_name, catalog_command_router))

    application.add_handler(MessageHandler(filters.COMMAND, unknown_command))

    async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
        if isinstance(context.error, Conflict):
            logger.warning("409 Conflict: instancia vieja aun activa, reintentando...")
            return
        logger.error("Excepción no capturada:", exc_info=context.error)
        tb = "".join(traceback.format_exception(None, context.error, context.error.__traceback__))
        logger.error(f"Traceback:\n{tb}")

    application.add_error_handler(error_handler)
    logger.info("El Bot de Telegram se está iniciando... (esperando 5s para liberar instancias viejas)")
    time.sleep(5)
    application.run_polling(
        allowed_updates=Update.ALL_TYPES,
        drop_pending_updates=True,
    )


if __name__ == '__main__':
    main()
