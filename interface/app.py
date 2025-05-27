import streamlit as st
import pandas as pd
from kafka import KafkaProducer
import json
import time
import os
import uuid
import psycopg2
import matplotlib.pyplot as plt

POSTGRES_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "postgres"),
    "port": os.getenv("POSTGRES_PORT", "5432"),
    "dbname": os.getenv("POSTGRES_DB", "scoring_db"),
    "user": os.getenv("POSTGRES_USER", "scorer"),
    "password": os.getenv("POSTGRES_PASSWORD", "scorer_pass"),
}


# Конфигурация Kafka
KAFKA_CONFIG = {
    "bootstrap_servers": os.getenv("KAFKA_BROKERS", "kafka:9092"),
    "topic": os.getenv("KAFKA_TOPIC", "transactions")
}

def get_results():
    conn = psycopg2.connect(**POSTGRES_CONFIG)
    cur = conn.cursor()

    # 10 последних fraud-транзакций
    cur.execute("""
        SELECT transaction_id, score, fraud_flag
        FROM scoring_results
        WHERE fraud_flag = true
        ORDER BY transaction_id DESC
        LIMIT 10;
    """)
    fraud_rows = cur.fetchall()

    # Последние 100 транзакций по убыванию
    cur.execute("""
        SELECT score
        FROM scoring_results
        ORDER BY transaction_id DESC
        LIMIT 100;
    """)
    score_rows = cur.fetchall()

    cur.close()
    conn.close()

    fraud_df = pd.DataFrame(fraud_rows, columns=["transaction_id", "score", "fraud_flag"])
    scores = [s[0] for s in score_rows]

    return fraud_df, scores


def load_file(uploaded_file):
    """Загрузка CSV файла в DataFrame"""
    try:
        return pd.read_csv(uploaded_file)
    except Exception as e:
        st.error(f"Ошибка загрузки файла: {str(e)}")
        return None

def send_to_kafka(df, topic, bootstrap_servers):
    """Отправка данных в Kafka с уникальным ID транзакции"""
    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            security_protocol="PLAINTEXT"
        )
        
        # Генерация уникальных ID для всех транзакций
        df['transaction_id'] = [str(uuid.uuid4()) for _ in range(len(df))]
        
        progress_bar = st.progress(0)
        total_rows = len(df)
        
        for idx, row in df.iterrows():
            # Отправляем данные вместе с ID
            producer.send(
                topic, 
                value={
                    "transaction_id": row['transaction_id'],
                    "data": row.drop('transaction_id').to_dict()
                }
            )
            progress_bar.progress((idx + 1) / total_rows)
            time.sleep(0.01)
            
        producer.flush()
     
        return True
    except Exception as e:
        st.error(f"Ошибка отправки данных: {str(e)}")
        return False

# Инициализация состояния
if "uploaded_files" not in st.session_state:
    st.session_state.uploaded_files = {}

# Интерфейс
st.title("📤 Отправка данных в Kafka")

# Блок загрузки файлов
uploaded_file = st.file_uploader(
    "Загрузите CSV файл с транзакциями",
    type=["csv"]
)

if uploaded_file and uploaded_file.name not in st.session_state.uploaded_files:
    # Добавляем файл в состояние
    st.session_state.uploaded_files[uploaded_file.name] = {
        "status": "Загружен",
        "df": load_file(uploaded_file)
    }
    st.success(f"Файл {uploaded_file.name} успешно загружен!")

# Список загруженных файлов
if st.session_state.uploaded_files:
    st.subheader("🗂 Список загруженных файлов")
    
    for file_name, file_data in st.session_state.uploaded_files.items():
        cols = st.columns([4, 2, 2])
        
        with cols[0]:
            st.markdown(f"**Файл:** `{file_name}`")
            st.markdown(f"**Статус:** `{file_data['status']}`")
        
        with cols[2]:
            if st.button(f"Отправить {file_name}", key=f"send_{file_name}"):
                if file_data["df"] is not None:
                    with st.spinner("Отправка..."):
                        success = send_to_kafka(
                            file_data["df"],
                            KAFKA_CONFIG["topic"],
                            KAFKA_CONFIG["bootstrap_servers"]
                        )
                        if success:
                            st.session_state.uploaded_files[file_name]["status"] = "Отправлен"
                            st.rerun()
                else:
                    st.error("Файл не содержит данных")

st.markdown("---")
st.subheader("📊 Анализ результатов скоринга")

if st.button("Посмотреть результаты"):
    with st.spinner("Загрузка из базы данных..."):
        fraud_df, score_list = get_results()

        if fraud_df.empty:
            st.info("Нет транзакций с флагом fraud.")
        else:
            st.markdown("### 🔍 Последние 10 фродовых транзакций:")
            st.dataframe(fraud_df)

        if score_list:
            st.markdown("### 📈 Распределение скоров (последние 100):")
            fig, ax = plt.subplots()
            ax.hist(score_list, bins=20)
            ax.set_xlabel("Score")
            ax.set_ylabel("Количество")
            ax.set_title("Гистограмма score")
            st.pyplot(fig)
        else:
            st.info("Нет данных для построения гистограммы.")
