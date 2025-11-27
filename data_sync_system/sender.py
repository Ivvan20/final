import sqlite3
import time
import json
from datetime import datetime
import paho.mqtt.client as mqtt
import logging
import sys
import os

# =============================================================================
# НАСТРОЙКА ЛОГИРОВАНИЯ
# =============================================================================
def setup_logging():
    """Настраивает логирование для системы"""
    # Создаем папку для логов если её нет
    os.makedirs('logs', exist_ok=True)
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('logs/sync_system.log', encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger(__name__)

logger = setup_logging()

# =============================================================================
# 1. СОЗДАНИЕ И ЗАПОЛНЕНИЕ БАЗЫ ДАННЫХ
# =============================================================================

def setup_database():
    """Создает и наполняет базу данных тестовыми данными"""
    try:
        # Подключаемся к базе данных
        conn = sqlite3.connect('local_sensor_data.db', check_same_thread=False)
        cursor = conn.cursor()

        # Создаем таблицу для данных с датчиков
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS sensor_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sensor_id INTEGER NOT NULL,
                value REAL NOT NULL,
                timestamp TEXT NOT NULL,
                sent INTEGER DEFAULT 0,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        conn.commit()

        # Проверяем, есть ли данные в таблице
        cursor.execute("SELECT COUNT(*) FROM sensor_data WHERE sent = 0")
        unsent_count = cursor.fetchone()[0]
        
        if unsent_count == 0:
            logger.info("Добавляем тестовые данные в базу...")
            test_data = [
                (1, 23.5, datetime.now().isoformat(), 0),
                (2, 18.9, datetime.now().isoformat(), 0),
                (1, 24.1, datetime.now().isoformat(), 0),
                (3, 19.7, datetime.now().isoformat(), 0),
                (2, 22.3, datetime.now().isoformat(), 0),
            ]
            cursor.executemany(
                "INSERT INTO sensor_data (sensor_id, value, timestamp, sent) VALUES (?, ?, ?, ?)", 
                test_data
            )
            conn.commit()
            logger.info("✅ Добавлено 5 тестовых записей")
        else:
            logger.info(f"📊 В базе найдено {unsent_count} неотправленных записей")

        return conn, cursor

    except Exception as e:
        logger.error(f"❌ Ошибка при создании базы данных: {e}")
        raise

# =============================================================================
# 2. НАСТРОЙКА MQTT КЛИЕНТА
# =============================================================================

def setup_mqtt_client():
    """Настраивает и подключает MQTT клиента"""
    
    # Настройки MQTT
    MQTT_BROKER = "broker.hivemq.com"
    MQTT_PORT = 1883
    MQTT_TOPIC = "my_school_project/sensor_data_v2"
    
    # Создаем клиента MQTT с уникальным ID
    client_id = f"sender_{datetime.now().strftime('%H%M%S')}"
    client = mqtt.Client(client_id=client_id, protocol=mqtt.MQTTv311)
    
    # Словарь для отслеживания статуса доставки
    delivery_status = {}

    def on_connect(client, userdata, flags, rc, properties=None):
        """Обработчик подключения к брокеру"""
        if rc == 0:
            logger.info("✅ Успешно подключились к MQTT брокеру!")
            logger.info(f"📡 Брокер: {MQTT_BROKER}:{MQTT_PORT}")
            logger.info(f"🎯 Топик: {MQTT_TOPIC}")
        else:
            error_codes = {
                1: "неверная версия протокола",
                2: "неверный идентификатор клиента", 
                3: "сервер недоступен",
                4: "неверные логин/пароль",
                5: "ошибка авторизации"
            }
            error_msg = error_codes.get(rc, f"неизвестная ошибка (код {rc})")
            logger.error(f"❌ Ошибка подключения: {error_msg}")

    def on_publish(client, userdata, mid, properties=None):
        """Обработчик подтверждения публикации"""
        delivery_status[mid] = True
        logger.debug(f"📨 Подтверждение доставки для сообщения ID {mid}")

    def on_disconnect(client, userdata, rc, properties=None):
        """Обработчик отключения от брокера"""
        if rc != 0:
            logger.warning("⚠️  Неожиданное отключение от брокера. Попытка переподключения...")

    # Назначаем обработчики событий
    client.on_connect = on_connect
    client.on_publish = on_publish
    client.on_disconnect = on_disconnect

    # Подключаемся к брокеру
    logger.info(f"🔗 Подключаемся к MQTT брокеру {MQTT_BROKER}...")
    try:
        client.connect(MQTT_BROKER, MQTT_PORT, 60)
        # Запускаем фоновый поток
        client.loop_start()
        # Даем время на установление соединения
        time.sleep(2)
        return client, delivery_status, MQTT_TOPIC
    except Exception as e:
        logger.error(f"❌ Не удалось подключиться к MQTT брокеру: {e}")
        raise

# =============================================================================
# 3. ФУНКЦИЯ СИНХРОНИЗАЦИИ ДАННЫХ
# =============================================================================

def sync_data(conn, cursor, client, delivery_status, MQTT_TOPIC):
    """Основная функция синхронизации данных"""
    try:
        # Получаем новые записи для отправки
        cursor.execute("""
            SELECT id, sensor_id, value, timestamp 
            FROM sensor_data 
            WHERE sent = 0 
            ORDER BY id
        """)
        new_records = cursor.fetchall()

        if not new_records:
            logger.info("💤 Новых данных для отправки нет")
            return True

        logger.info(f"📦 Найдено {len(new_records)} новых записей")

        success_count = 0
        for record in new_records:
            record_id, sensor_id, value, timestamp = record

            # Подготавливаем данные для отправки
            payload = {
                "id": record_id,
                "sensor_id": sensor_id,
                "value": value,
                "timestamp": timestamp,
                "source": "local_sqlite",
                "version": "2.0"
            }
            json_payload = json.dumps(payload, ensure_ascii=False)

            # Сбрасываем флаг доставки
            message_mid = None

            logger.info(f"🚀 Отправляем запись ID {record_id} (Датчик {sensor_id}: {value}°C)")
            
            # Публикуем сообщение с гарантией доставки
            msg_info = client.publish(MQTT_TOPIC, json_payload, qos=1)
            
            if msg_info.rc != mqtt.MQTT_ERR_SUCCESS:
                logger.error(f"❌ Ошибка публикации записи ID {record_id}")
                continue
                
            message_mid = msg_info.mid
            delivery_status[message_mid] = False

            # Ждем подтверждения (макс. 10 секунд)
            wait_time = 0
            while not delivery_status.get(message_mid, False) and wait_time < 10:
                time.sleep(0.5)
                wait_time += 0.5

            if delivery_status.get(message_mid, False):
                # Помечаем запись как отправленную
                cursor.execute("UPDATE sensor_data SET sent = 1 WHERE id = ?", (record_id,))
                conn.commit()
                success_count += 1
                logger.info(f"✅ Запись ID {record_id} доставлена")
                
                # Очищаем словарь статусов
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
    """Основная функция программы"""
    logger.info("🚀 ЗАПУСК СИСТЕМЫ ПЕРЕДАЧИ ДАННЫХ")
    logger.info("=" * 50)
    
    conn = None
    client = None
    
    try:
        # Инициализируем компоненты системы
        conn, cursor = setup_database()
        client, delivery_status, MQTT_TOPIC = setup_mqtt_client()
        
        # Основной цикл работы
        cycle_count = 0
        logger.info("\n🔄 Служба синхронизации запущена")
        logger.info("⏰ Интервал проверки: 30 секунд")
        logger.info("⏹️  Для остановки нажмите Ctrl+C\n")
        
        while True:
            cycle_count += 1
            logger.info(f"\n{'='*30}")
            logger.info(f"ЦИКЛ СИНХРОНИЗАЦИИ #{cycle_count}")
            logger.info(f"{'='*30}")
            
            sync_success = sync_data(conn, cursor, client, delivery_status, MQTT_TOPIC)
            
            if sync_success:
                logger.info("✅ Цикл завершен успешно")
            else:
                logger.warning("⚠️  В цикле возникли проблемы")
            
            logger.info("⏳ Ожидание 30 секунд...")
            time.sleep(30)
            
    except KeyboardInterrupt:
        logger.info("\n🛑 ОСТАНОВКА СИСТЕМЫ ПОЛЬЗОВАТЕЛЕМ")
    except Exception as e:
        logger.error(f"💥 КРИТИЧЕСКАЯ ОШИБКА: {e}")
    finally:
        # Корректное завершение работы
        logger.info("\n🔚 Завершение работы системы...")
        if client:
            client.loop_stop()
            client.disconnect()
            logger.info("✅ MQTT клиент отключен")
        if conn:
            conn.close()
            logger.info("✅ База данных закрыта")
        logger.info("🎯 СИСТЕМА ОСТАНОВЛЕНА")

if __name__ == "__main__":
    main()