import os
import logging
import sys
from sqlalchemy import create_engine, Column, Integer, String, Float, Boolean, DateTime, BigInteger, ForeignKey
from sqlalchemy.orm import sessionmaker, declarative_base, relationship
from sqlalchemy import text
from datetime import datetime
from dotenv import load_dotenv 

# --- Configuración de Logging ---
logging.basicConfig(level=logging.INFO)

# --- Definición de la Base ---
Base = declarative_base()

# --- Modelos de Datos ---
class Usuario(Base):
    __tablename__ = 'usuarios'
    id = Column(Integer, primary_key=True)
    telegram_id = Column(BigInteger, unique=True, nullable=True) 
    username = Column(String(50), unique=True, nullable=False)
    login_key = Column(String(100), nullable=False) 
    saldo = Column(Float, default=0.00)
    es_admin = Column(Boolean, default=False)
    plan = Column(String(20), default='FREE')
    estado = Column(String(20), default='ACTIVO')
    fecha_registro = Column(DateTime, default=datetime.now)
    compras = relationship("Compra", back_populates="usuario", cascade="all, delete-orphan")

class Producto(Base):
    __tablename__ = 'productos'
    id = Column(Integer, primary_key=True)
    nombre = Column(String(100), nullable=False)
    categoria = Column(String(50), nullable=False)
    precio = Column(Float, nullable=False)
    descripcion = Column(String(255)) 
    fecha_creacion = Column(DateTime, default=datetime.now)
    keys = relationship("Key", back_populates="producto")

class Key(Base):
    __tablename__ = 'keys'
    id = Column(Integer, primary_key=True)
    producto_id = Column(Integer, ForeignKey('productos.id'), nullable=False)
    licencia = Column(String(255), unique=True, nullable=False)
    estado = Column(String(20), default='available') 
    producto = relationship("Producto", back_populates="keys")

class Compra(Base):
    __tablename__ = 'compras'
    id = Column(Integer, primary_key=True)
    nro_venta = Column(String(60), unique=True, nullable=False)
    usuario_id = Column(Integer, ForeignKey('usuarios.id'), nullable=False)
    estado = Column(String(30), default='APROBADO')
    vendedor = Column(String(80), nullable=False)
    tipo = Column(String(50), default='CRÉDITOS')
    plan = Column(String(20), default='FREE')
    cantidad = Column(Float, default=0.0)
    recompensa = Column(String(80), default='-')
    fecha = Column(DateTime, default=datetime.now)
    detalle = Column(String(255), default='')

    usuario = relationship("Usuario", back_populates="compras")


# --- Conexión y Sesión (Lee DATABASE_URL de ENV) ---
load_dotenv() 
DATABASE_URL = os.getenv('DATABASE_URL', 'sqlite:///socios_bot.db') 
_is_postgres = DATABASE_URL.startswith('postgresql') or DATABASE_URL.startswith('postgres')
ENGINE = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    **({'pool_size': 5, 'max_overflow': 10, 'pool_recycle': 300, 'pool_timeout': 30} if _is_postgres else {})
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=ENGINE)

def get_session():
    """Retorna una nueva sesión de SQLAlchemy."""
    return SessionLocal()

def inicializar_db(engine=ENGINE): 
    """Crea las tablas, y el usuario administrador si no existen."""
    Base.metadata.create_all(bind=engine) 

    with engine.begin() as conn:
        try:
            conn.execute(text("ALTER TABLE usuarios ADD COLUMN IF NOT EXISTS plan VARCHAR(20) DEFAULT 'FREE'"))
        except Exception:
            pass
        try:
            conn.execute(text("ALTER TABLE usuarios ADD COLUMN IF NOT EXISTS estado VARCHAR(20) DEFAULT 'ACTIVO'"))
        except Exception:
            pass
        try:
            conn.execute(text("ALTER TABLE compras ADD COLUMN IF NOT EXISTS plan VARCHAR(20) DEFAULT 'FREE'"))
        except Exception:
            pass

    Session = sessionmaker(bind=engine)
    with Session() as session:
        if session.query(Usuario).filter(Usuario.es_admin == True).count() == 0:
            logging.info("Insertando USUARIO ADMINISTRADOR INICIAL: admin/adminpass")
            admin_user = Usuario(username='admin', login_key='adminpass', saldo=1000.00, es_admin=True, plan='DIAMOND', estado='ACTIVO')
            session.add(admin_user)
            session.commit()
            print("Base de datos inicializada con usuario administrador.")
        else:
            session.query(Usuario).filter(Usuario.plan.is_(None)).update({Usuario.plan: 'FREE'}, synchronize_session=False)
            session.query(Usuario).filter(Usuario.estado.is_(None)).update({Usuario.estado: 'ACTIVO'}, synchronize_session=False)
            session.commit()
            print("Base de datos verificada. Usuario administrador existente.")


if __name__ == '__main__':
    # Este bloque se ejecuta cuando el comando de inicio en Railway llama a este archivo.
    print(f"Conectando a Base de Datos con URL: {DATABASE_URL}")
    try:
        inicializar_db(ENGINE) 
        print("¡Proceso de creación de tablas finalizado con éxito!")
    except Exception as e:
        print(f"\n--- ERROR CRÍTICO DE CONEXIÓN EN DB_MODELS.PY ---\nDetalle: {e}")
        sys.exit(1)