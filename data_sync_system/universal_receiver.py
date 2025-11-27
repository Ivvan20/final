import json
from datetime import datetime
import paho.mqtt.client as mqtt
import logging
import sqlite3
import sys

# Добавляем путь для VSCode
sys.path.append("C:\\Users\\Student\\AppData\\Roaming\\Python\\Python313\\site-packages")

# Настройка логирования
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Импортируем конфигурацию
try:
    from config import MQTT_BROKER, MQTT_PORT, MQTT_TOPIC
except ImportError as e:
    logger.error("❌ config.py не найден!")
    sys.exit(1)

# Глобальные переменные для базы данных
conn = None
cursor = None

def setup_storage():
    """Настраивает центральное хранилище"""
    try:
        conn = sqlite3.connect("central_universal.db", check_same_thread=False)
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS received_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                original_id INTEGER,
                sensor_id INTEGER NOT NULL,
                value REAL NOT NULL,
                timestamp TEXT NOT NULL,
                received_at TEXT NOT NULL,
                source_db TEXT,
                db_type TEXT
            )
        ''')
        conn.commit()
        logger.info("✅ Центральное хранилище готово")
        return conn, cursor
    except Exception as e:
        logger.error(f"❌ Ошибка хранилища: {e}")
        return None, None

def on_connect(client, userdata, flags, rc, properties):
    if rc == 0:
        logger.info("✅ ПРИЁМНИК ПОДКЛЮЧЕН К MQTT!")
        client.subscribe(MQTT_TOPIC)
        logger.info(f"📡 Подписан на: {MQTT_TOPIC}")
    else:
        logger.error(f"❌ Ошибка подключения: {rc}")

def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode())
        
        logger.info(f"\n📨 ПОЛУЧЕНО СООБЩЕНИЕ")
        logger.info(f"├─ Источник: {payload.get('source', 'unknown')}")
        logger.info(f"├─ База данных: {payload.get('database_type', 'unknown')}")
        logger.info(f"├─ ID: {payload.get('id')}")
        logger.info(f"├─ Датчик: {payload.get('sensor_id')}")
        logger.info(f"└─ Значение: {payload.get('value')}°C")
        
        # Сохраняем в базу
        cursor.execute('''
            INSERT INTO received_data 
            (original_id, sensor_id, value, timestamp, received_at, source_db, db_type) 
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (
            payload.get('id'),
            payload.get('sensor_id'),
            payload.get('value'),
            payload.get('timestamp'),
            datetime.now().isoformat(),
            payload.get('source'),
            payload.get('database_type')
        ))
        conn.commit()
        
        # Сохраняем в лог-файл
        with open("received_universal.log", "a", encoding="utf-8") as f:
            f.write(f"{datetime.now().isoformat()} | {json.dumps(payload)}\n")
            
        logger.info("✅ Данные сохранены")
        
    except Exception as e:
        logger.error(f"💥 Ошибка обработки: {e}")

def main():
    global conn, cursor
    
    logger.info("🚀 УНИВЕРСАЛЬНЫЙ ПРИЁМНИК ЗАПУЩЕН")
    logger.info("=" * 50)
    
    # Настраиваем хранилище
    conn, cursor = setup_storage()
    if not conn:
        return
    
    # Настраиваем MQTT клиента
    client = mqtt.Client("universal_receiver")
    client.on_connect = on_connect
    client.on_message = on_message
    
    try:
        logger.info(f"🔗 Подключение к {MQTT_BROKER}...")
        client.connect(MQTT_BROKER, MQTT_PORT, 60)
        
        logger.info("🎧 Ожидание данных...")
        client.loop_forever()
        
    except KeyboardInterrupt:
        logger.info("\n🛑 Остановка пользователем")
    except Exception as e:
        logger.error(f"💥 Ошибка: {e}")
    finally:
        client.disconnect()
        conn.close()
        logger.info("🎯 Приёмник остановлен")

if __name__ == "__main__":
    main()