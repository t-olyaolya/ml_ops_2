# ml_ops_2
## 🏗️ Архитектура

Компоненты системы:
1. **`interface`** (Streamlit UI):
   
   Создан для удобной симуляции потоковых данных с транзакциями. Реальный продукт использовал бы прямой поток данных из других систем.
    - Имитирует отправку транзакций в Kafka через CSV-файлы.
    - Генерирует уникальные ID для транзакций.
    - Загружает транзакции отдельными сообщениями формата JSON в топик kafka `transactions`.
    - Отображает последние 10 фродовых транзакций 
    - Отображает распределение для 100 последних транзакций
    

2. **`fraud_detector`** (ML Service):
   - Загружает предобученную модель CatBoost (`lgb.txt`).
   - Выполняет препроцессинг данных
   - Кодирование категориальных переменных
   - Производит скоринг с порогом 0.98.
   - Выгружает результат скоринга в топик kafka `scoring`

3. **Kafka Infrastructure**:
   - Zookeeper + Kafka брокер
   - `kafka-setup`: автоматически создает топики `transactions` и `scoring`
   - Kafka UI: веб-интерфейс для мониторинга сообщений (порт 8080)

## 🚀 Быстрый старт

### Требования
- Docker 20.10+
- Docker Compose 2.0+

### Запуск
```bash
# Сборка и запуск всех сервисов
docker-compose up --build
```
После запуска:
- **Streamlit UI**: http://localhost:8501
- **Kafka UI**: http://localhost:8080
- **PgAdmin**: http://localhost:5050
- **Postgres**: postgresql://scorer:scorer_pass@localhost:5432/scoring_db
- **Логи сервисов**: 
  ```bash
  docker-compose logs <service_name>  # Например: fraud_detector, kafka, interface

## 🛠️ Использование

### 1. Загрузка данных:

 - Загрузите CSV через интерфейс Streamlit. Для тестирования работы проекта используется файл формата `test.csv` из соревнования https://www.kaggle.com/competitions/teta-ml-1-2025
 - Пример структуры данных:
    ```csv
    transaction_time,amount,lat,lon,merchant_lat,merchant_lon,gender,...
    2023-01-01 12:30:00,150.50,40.7128,-74.0060,40.7580,-73.9855,M,...
    ```
 - Для первых тестов рекомендуется загружать небольшой семпл данных (до 100 транзакций) за раз, чтобы исполнение кода не заняло много времени.

### 2. Мониторинг:
 - **Kafka UI**: Просматривайте сообщения в топиках transactions и scoring
 - **PgAdmin или postgres клиента**: Просматривайте содержимое БД
 - **Логи обработки**: /app/logs/service.log внутри контейнера fraud_detector

### 3. Результаты:

 - Скоринговые оценки пишутся в топик scoring в формате:
    ```json
    {
    "score": 0.995, 
    "fraud_flag": 1, 
    "transaction_id": "d6b0f7a0-8e1a-4a3c-9b2d-5c8f9d1e2f3a"
    }
    ```
- Скоринговые оценки сохраняются в БД в таблице scoring_results
- Последние 10 фродовых транзакций отображаюься в UI
- Распределение score для последних 100 транзакций отображается в UI
- 
## Структура проекта
```
.
├── fraud_detector/
│    └── src
│       ├── docker-compose.yamlpreprocessing.py    # Логика препроцессинга
│       └──scorer.py           # ML-модель и предсказания
│    └── app
│       └── app.py              # Kafka Consumer/Producer
│    ├──  Dockerfile
│    └── requirements.txt
├── interface/
│   ├── .streamlint
│       └── config.toml
│   ├── app.py              # Streamlit UI
│   ├── Dockerfile
│   └── requirements.txt
├── score_writer/
│    └── app
│       └── app.py              # Запись в БД
│    ├──  Dockerfile
│    └── requirements.txt
├── docker-compose.yaml
├── gitignore.yaml
└── README.md
```

## Настройки Kafka
```yml
Топики:
- transactions (входные данные)
- scoring (результаты скоринга)

Репликация: 1 (для разработки)
Партиции: 3
```

*Примечание:* 

Для полной функциональности убедитесь, что:
1. Модель `lgb.txt` размещена в `fraud_detector/models/`
2. Тренировочные данные находятся в `fraud_detector/train_data/`
3. Порты 8080, 8501, 5432, 5050 и 9095 свободны на хосте

*Примечание2:* 
Так как у меня вышло так, что все транзакции из моего test файла распознаются как не фродовые (я проверила и абсолютно такой же результат в submisson на соревновании, то есть дело в модели), то я меняла флаги в бд, чтобы проверить отображение 10 последних фрод транзакций в UI
