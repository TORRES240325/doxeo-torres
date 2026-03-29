import subprocess
import sys
import time
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

print("Iniciando bot_admin.py...")
p_admin = subprocess.Popen([sys.executable, os.path.join(BASE_DIR, 'bot_admin.py')])

print("Esperando 8s antes de iniciar bot_main.py...")
time.sleep(8)

print("Iniciando bot_main.py...")
p_main = subprocess.Popen([sys.executable, os.path.join(BASE_DIR, 'bot_main.py')])

print("Ambos bots iniciados. Monitoreando...")

try:
    while True:
        if p_admin.poll() is not None:
            print(f"bot_admin.py terminó (código {p_admin.returncode}). Reiniciando todo...")
            p_main.terminate()
            sys.exit(1)
        if p_main.poll() is not None:
            print(f"bot_main.py terminó (código {p_main.returncode}). Reiniciando todo...")
            p_admin.terminate()
            sys.exit(1)
        time.sleep(5)
except KeyboardInterrupt:
    print("Deteniendo bots...")
    p_admin.terminate()
    p_main.terminate()
