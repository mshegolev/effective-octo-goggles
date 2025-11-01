# Data Engineering — Раздел 5. Airflow и Kafka

**Автор:** Щеголев Михаил  
**Курс подготовлен для личного обучения, 2025**

---

## 📘 Содержание

1. [Введение в Apache Airflow](#введение-в-apache-airflow)
2. [Основные концепции DAG, Operators, Sensors, Hooks](#основные-концепции-dag-operators-sensors-hooks)
3. [Передача данных между задачами (XCom)](#передача-данных-между-задачами-xcom)
4. [Kafka — основы потоковой обработки](#kafka--основы-потоковой-обработки)
5. [Мини-проект: Потоковая обработка Kafka → Spark → DWH](#минипроект-потоковая-обработка-kafka--spark--dwh)
6. [Контрольные вопросы и GPT-подсказки](#контрольные-вопросы-и-gptподсказки)

---

## Введение в Apache Airflow

📘 Материалы: *Index — Roadmappers (Airflow)*  

### 🧠 Что такое Airflow
Apache Airflow — оркестратор рабочих процессов, позволяющий создавать, планировать и отслеживать выполнение задач (pipelines).

### 💡 Основные компоненты
- **DAG (Directed Acyclic Graph)** — граф задач, описывающий зависимости.  
- **Task** — отдельная задача внутри DAG.  
- **Scheduler** — планировщик выполнения.  
- **Executor** — механизм запуска задач (Local, Celery, Kubernetes).

### 💻 Пример базового DAG

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def hello_world():
    print("Hello from Airflow!")

dag = DAG(
    'example_dag',
    start_date=datetime(2025, 1, 1),
    schedule_interval='@daily',
    catchup=False
)

task = PythonOperator(
    task_id='print_hello',
    python_callable=hello_world,
    dag=dag
)
```

💡 *Совет:* каждый DAG — это просто Python-скрипт.

---

## Основные концепции DAG, Operators, Sensors, Hooks

### 🧠 Термины
- **Operator** — класс, выполняющий задачу (PythonOperator, BashOperator, DummyOperator).  
- **Sensor** — ожидает событие (например, появление файла).  
- **Hook** — интерфейс для подключения к внешним системам (PostgresHook, S3Hook).  
- **Dataset** — объект данных, связывающий DAG-и между собой.

### 💻 Пример с Sensor и Hook

```python
from airflow.sensors.filesystem import FileSensor
from airflow.providers.postgres.hooks.postgres import PostgresHook

def load_to_db():
    hook = PostgresHook(postgres_conn_id='pg_connection')
    conn = hook.get_conn()
    cur = conn.cursor()
    cur.execute("INSERT INTO logs VALUES (now(), 'file_loaded')")
    conn.commit()

wait_for_file = FileSensor(
    task_id='wait_for_input',
    filepath='/data/input.csv',
    poke_interval=60,
    timeout=600,
    dag=dag
)
```

### 💡 GPT-подсказки
> Объясни, как работают Sensors в Airflow.  
> Как с помощью Hook подключиться к S3 и скачать файл?

---

## Передача данных между задачами (XCom)

### 🧠 Теория
**XCom (Cross-Communication)** — механизм передачи данных между задачами DAG.

### 💻 Пример

```python
def extract(**context):
    data = {"user_count": 42}
    context['ti'].xcom_push(key='data', value=data)

def transform(**context):
    data = context['ti'].xcom_pull(key='data')
    print(f"Полученные данные: {data}")

extract_task = PythonOperator(
    task_id='extract',
    python_callable=extract,
    provide_context=True,
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform',
    python_callable=transform,
    provide_context=True,
    dag=dag
)

extract_task >> transform_task
```

### 💡 GPT-подсказки
> Попроси GPT: «Как лучше передавать большие объёмы данных между задачами в Airflow?»  
> «Чем XCom отличается от Dataset?»

---

## Kafka — основы потоковой обработки

📘 Материалы: *Index — Roadmappers (Kafka)*  

### 🧠 Основные понятия
- **Producer** — отправляет сообщения в Kafka.  
- **Consumer** — читает сообщения из Kafka.  
- **Topic** — канал, куда публикуются сообщения.  
- **Partition** — деление топика для параллельной обработки.  
- **Offset** — позиция сообщения в партиции.  
- **Retention** — время хранения сообщений (по умолчанию 7 дней).  

### 💻 Пример продюсера и консьюмера

```python
# Producer
from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

producer.send('sales_topic', {'user': 'mike', 'amount': 250})
producer.flush()

# Consumer
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'sales_topic',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

for msg in consumer:
    print(msg.value)
```

### 💡 GPT-подсказки
> Объясни, зачем нужны партиции в Kafka.  
> Что такое лог ретеншн и как его настроить?  
> Придумай задачу с несколькими консьюмерами для одного топика.

---

## Мини-проект: Потоковая обработка Kafka → Spark → DWH

### 🎯 Цель
Построить пайплайн, который:
1. Читает поток данных из Kafka (топик `sales_topic`).  
2. Обрабатывает их в Spark Streaming.  
3. Загружает агрегированные данные в DWH.

### 💻 Пример

```python
# Spark Streaming consumer
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("KafkaToDWH").getOrCreate()

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sales_topic") \
    .load()

# Преобразование и агрегация
df_parsed = df.selectExpr("CAST(value AS STRING)")

agg = df_parsed.groupBy("region").count()

# Запись в DWH (Postgres)
agg.writeStream \
    .outputMode("complete") \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/dwh") \
    .option("dbtable", "sales_summary") \
    .option("user", "airflow") \
    .option("password", "secret") \
    .start()
```

💡 *Совет:* Этот пайплайн можно запускать из Airflow DAG через `SparkSubmitOperator`.

### 💡 GPT-подсказки
> Попроси GPT написать Airflow DAG, который запускает этот Spark Streaming job.  
> Добавь обработку ошибок при падении Kafka-коннекта.

---

## Контрольные вопросы и GPT-подсказки

1. Что такое DAG и чем он отличается от Task?  
2. Какие типы операторов существуют в Airflow?  
3. Что делает Sensor и в каких сценариях он полезен?  
4. Что такое XCom и как он используется?  
5. Как устроена архитектура Kafka (Producer, Broker, Consumer)?  
6. Что такое партиции и зачем они нужны?  
7. Что такое лог ретеншн?  
8. Какие типы сообщений поддерживает Kafka?

💡 *Совет:* если не уверен — спроси GPT:  
> «Объясни пошагово, как данные проходят через Kafka и Airflow DAG»  
> «Придумай задачу по обмену данными между задачами в Airflow»

---

✅ **Итог раздела:**  
- Освоены принципы оркестрации пайплайнов в Airflow.  
- Понимаешь архитектуру Kafka и умеешь работать с потоками данных.  
- Готов к построению end-to-end потоковых пайплайнов (Kafka → Spark → DWH).
