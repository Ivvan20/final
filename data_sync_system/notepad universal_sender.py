import sys
import os
import time
import json
from datetime import datetime
import logging
import paho.mqtt.client as mqtt

# Добавляем путь к библиотекам
sys.path.append('C:\\Users\\Student\\AppData\\Roaming\\Python\\Python313\\site-packages')

try:
    import mysql.connector
    MYSQL_AVAILABLE = True
except ImportError:
    MYSQL_AVAILABLE = False
    print("❌ MySQL connector не установлен. Используйте: pip install mysql-connector-python")

import sqlite3

# Импортируем конфигурацию
try:
    from config import DATABASE_CONFIG, ACTIVE_DATABASE, MQTT_BROKER, MQTT_PORT, MQTT_TOPIC
except ImportError:
    print("❌ Файл config.py не найден! Создайте его сначала.")
    sys.exit(1)

# =============================================================================
# НАСТРОЙКА ЛОГИРОВАНИЯ
# =============================================================================
def setup_logging():
    os.makedirs('logs', exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('logs/universal_sender.log', encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger(__name__)

logger = setup_logging()

# =============================================================================
# 1. УНИВЕРСАЛЬНОЕ ПОДКЛЮЧЕНИЕ К БАЗЕ ДАННЫХ
# =============================================================================

class DatabaseManager:
    def __init__(self, config):
        self.config = config
        self.connection = None
        self.cursor = None
        
    def connect(self):
        """Устанавливает соединение с выбранной СУБД"""
        try:
            if self.config['type'] == 'sqlite':
                self.connection = sqlite3.connect(
                    self.config['database'], 
                    check_same_thread=False
                )
                self.cursor = self.connection.cursor()
                logger.info(f"✅ Подключено к SQLite: {self.config['database']}")
                
            elif self.config['type'] == 'mysql' and MYSQL_AVAILABLE:
                self.connection = mysql.connector.connect(
                    host=self.config['host'],
                    user=self.config['user'],
                    password=self.config['password'],
                    database=self.config['database'],
                    port=self.config.get('port', 3306)
                )
                self.cursor = self.connection.cursor()
                logger.info(f"✅ Подключено к MySQL: {self.config['database']}")
                
            else:
                if self.config['type'] == 'mysql' and not MYSQL_AVAILABLE:
                    raise Exception("MySQL connector не установлен")
                else:
                    raise Exception("Тип базы данных не поддерживается")
                
            self._create_tables()
            return True
            
        except Exception as e:
            logger.error(f"❌ Ошибка подключения к базе данных: {e}")
            return False
    
    def _create_tables(self):
        """Создает необходимые таблицы"""
        if self.config['type'] == 'sqlite':
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS sensor_data (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    sensor_id INTEGER NOT NULL,
                    value REAL NOT NULL,
                    timestamp TEXT NOT NULL,
                    sent INTEGER DEFAULT 0,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            ''')
        elif self.config['type'] == 'mysql':
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS sensor_data (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    sensor_id INT NOT NULL,
                    value FLOAT NOT NULL,
                    timestamp TEXT NOT NULL,
                    sent INT DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
        
        self.connection.commit()
        logger.info("✅ Таблицы созданы/проверены")
    
    def insert_test_data(self):
        """Добавляет тестовые данные"""
        test_data = [
            (1, 23.5, datetime.now().isoformat(), 0),
            (2, 18.9, datetime.now().isoformat(), 0),
            (1, 24.1, datetime.now().isoformat(), 0),
            (3, 19.7, datetime.now().isoformat(), 0),
            (2, 22.3, datetime.now().isoformat(), 0),
        ]
        
        if self.config['type'] == 'sqlite':
            self.cursor.executemany(
                "INSERT OR IGNORE INTO sensor_data (sensor_id, value, timestamp, sent) VALUES (?, ?, ?, ?)", 
                test_data
            )
        else:
            self.cursor.executemany(
                "INSERT IGNORE INTO sensor_data (sensor_id, value, timestamp, sent) VALUES (%s, %s, %s, %s)", 
                test_data
            )
        
        self.connection.commit()
        logger.info("✅ Тестовые данные добавлены")
    
    def get_unsent_data(self):
        """Получает неотправленные данные"""
        query = "SELECT id, sensor_id, value, timestamp FROM sensor_data WHERE sent = 0 ORDER BY id"
        self.cursor.execute(query)
        return self.cursor.fetchall()
    
    def mark_as_sent(self, record_id):
        """Помечает запись как отправленную"""
        if self.config['type'] == 'sqlite':
            self.cursor.execute("UPDATE sensor_data SET sent = 1 WHERE id = ?", (record_id,))
        else:
            self.cursor.execute("UPDATE sensor_data SET sent = 1 WHERE id = %s", (record_id,))
        self.connection.commit()
    
    def close(self):
        """Закрывает соединение"""
        if self.connection:
            self.connection.close()

# =============================================================================
# 2. MQTT КЛИЕНТ
# =============================================================================

def setup_mqtt_client():
    client_id = f"universal_sender_{datetime.now().strftime('%H%M%S')}"
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=client_id)
    
    delivery_status = {}

    def on_connect(client, userdata, flags, rc, properties):
        if rc == 0:
            logger.info("✅ Успешно подключились к MQTT брокеру!")
        else:
            logger.error(f"❌ Ошибка подключения к брокеру. Код: {rc}")

    def on_publish(client, userdata, mid, reason_code, properties):
        delivery_status[mid] = True
        logger.debug(f"📨 Подтверждение доставки для сообщения ID {mid}")

    client.on_connect = on_connect
    client.on_publish = on_publish

    try:
        client.connect(MQTT_BROKER, MQTT_PORT, 60)
        client.loop_start()
        time.sleep(2)
        return client, delivery_status
    except Exception as e:
        logger.error(f"❌ Не удалось подключиться к MQTT брокеру: {e}")
        raise

# =============================================================================
# 3. ФУНКЦИЯ СИНХРОНИЗАЦИИ
# =============================================================================

def sync_data(db_manager, client, delivery_status):
    try:
        new_records = db_manager.get_unsent_data()

        if not new_records:
            logger.info("💤 Новых данных для отправки нет")
            return True

        logger.info(f"📦 Найдено {len(new_records)} новых записей")

        success_count = 0
        for record in new_records:
            record_id, sensor_id, value, timestamp = record

            payload = {
                "id": record_id,
                "sensor_id": sensor_id,
                "value": value,
                "timestamp": timestamp,
                "source": ACTIVE_DATABASE,
                "database_type": DATABASE_CONFIG[ACTIVE_DATABASE]['type'],
                "version": "3.0"
            }
            json_payload = json.dumps(payload, ensure_ascii=False)

            logger.info(f"🚀 Отправляем запись ID {record_id} из {DATABASE_CONFIG[ACTIVE_DATABASE]['type'].upper()}")

            msg_info = client.publish(MQTT_TOPIC, json_payload, qos=1)
            
            if msg_info.rc != mqtt.MQTT_ERR_SUCCESS:
                logger.error(f"❌ Ошибка публикации записи ID {record_id}")
                continue
                
            message_mid = msg_info.mid
            delivery_status[message_mid] = False

            # Ждем подтверждения
            wait_time = 0
            while not delivery_status.get(message_mid, False) and wait_time < 10:
                time.sleep(0.5)
                wait_time += 0.5

            if delivery_status.get(message_mid, False):
                db_manager.mark_as_sent(record_id)
                success_count += 1
                logger.info(f"✅ Запись ID {record_id} доставлена и помечена")
                delivery_status.pop(message_mid, None)
            else:
                logger.warning(f"⚠️  Таймаут доставки записи ID {record_id}")
                return False

        logger.info(f"🎉 Успешно отправлено {success_count} из {len(new_records)} записей")
        return True

    except Exception as e:
        logger.error(f"💥 Ошибка синхронизации: {e}")
        return False

# =============================================================================
# 4. ГЛАВНАЯ ФУНКЦИЯ
# =============================================================================

def main():
    logger.info(f"🚀 УНИВЕРСАЛЬНАЯ СИСТЕМА ПЕРЕДАЧИ ДАННЫХ")
    logger.info(f"📊 Активная СУБД: {ACTIVE_DATABASE.upper()}")
    logger.info("=" * 50)
    
    db_manager = None
    client = None
    
    try:
        # Инициализация базы данных
        db_manager = DatabaseManager(DATABASE_CONFIG[ACTIVE_DATABASE])
        if not db_manager.connect():
            return
        
        # Добавляем тестовые данные
        db_manager.insert_test_data()
        
        # MQTT клиент
        client, delivery_status = setup_mqtt_client()
        
        # Основной цикл
        cycle_count = 0
        logger.info("\n🔄 Служба синхронизации запущена")
        
        while True:
            cycle_count += 1
            logger.info(f"\n{'='*30}")
            logger.info(f"ЦИКЛ СИНХРОНИЗАЦИИ #{cycle_count}")
            logger.info(f"{'='*30}")
            
            sync_success = sync_data(db_manager, client, delivery_status)
            
            if sync_success:
                logger.info("✅ Цикл завершен успешно")
            else:
                logger.warning("⚠️  В цикле возникли проблемы")
            
            logger.info("⏳ Ожидание 30 секунд...")
            time.sleep(30)
            
    except KeyboardInterrupt:
        logger.info("\n🛑 ОСТАНОВКА СИСТЕМЫ")
    except Exception as e:
        logger.error(f"💥 Критическая ошибка: {e}")
    finally:
        logger.info("\n🔚 Завершение работы...")
        if client:
            client.loop_stop()
            client.disconnect()
        if db_manager:
            db_manager.close()
        logger.info("🎯 СИСТЕМА ОСТАНОВЛЕНА")

if __name__ == "__main__":
    main()