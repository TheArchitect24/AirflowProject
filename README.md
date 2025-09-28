This Airflow project contains 2 project contains 
1. ETL Toll Data Pipeline
2. Server Access Log Processing

# 🚦 ETL Toll Data Pipeline with Apache Airflow

This project demonstrates an ETL (Extract, Transform, Load) pipeline built with Apache Airflow to process toll data from multiple file formats and load it into a PostgreSQL database. Bash and PythonOperators are combined in this pipeline.
---
## 📌 Project Overview

The pipeline performs the following tasks:

Unzips a compressed archive containing toll data.

Extracts relevant columns from:

CSV (vehicle-data.csv)

TSV (tollplaza-data.tsv)

Fixed-width text (payment-data.txt)

Transforms & consolidates the extracted data into a single file.

Loads the final dataset into a PostgreSQL table (car_details).
---
## 🏗️ Tech Stack

Apache Airflow – Orchestration

Python (pandas, SQLAlchemy) – Data consolidation & database loading

Bash – Data extraction and preprocessing

PostgreSQL – Data storage
```
📂 Project Structure
project/
│── dags/
│   └── ETL_toll_data.py         # Main Airflow DAG
│── tolldata/
│   ├── vehicle-data.csv
│   ├── tollplaza-data.tsv
│   ├── payment-data.txt
│   └── cleaned/                 # Intermediate cleaned data files
│── tolldata.tgz                 # Compressed input data
```
---
⚙️ Setup Instructions
1. Install Dependencies

Make sure you have Airflow, PostgreSQL, and the required Python libraries installed:
```
pip install apache-airflow pandas sqlalchemy psycopg2-binary
```
2. Configure PostgreSQL

Create a database and user for the pipeline:
```
CREATE DATABASE tolldata;
CREATE USER airflow_user WITH PASSWORD 'airflow_pass';
GRANT ALL PRIVILEGES ON DATABASE tolldata TO airflow_user;
```

Update the connection string in the DAG if your DB settings differ:
```
engine = create_engine("postgresql+psycopg2://dbusername:dbpassword@localhost:5432/tolldata")
```
3. Place Data Files

Ensure tolldata.tgz is placed in the project directory.

4. Run Airflow

Initialize and start Airflow:
```
airflow db init
airflow scheduler &
airflow webserver --port 8080
```

Open the UI at http://localhost:8080
 to trigger the DAG.

📊 Final Output

The processed data is stored in PostgreSQL in a table named:
```
car_details
```

With columns:
```
id

timestamp

vehicle_number

vehicle_type

type

code

vin

tin

plate
```
---
## 🚀 DAG Workflow
unzip_data 
   → extract_data_from_csv 
   → extract_data_from_tsv 
   → extract_data_from_fixed_width 
   → consolidate_data 
   → postgresload
----




# ETL Server Access Log Processing with Apache Airflow

This project demonstrates how to build an ETL (Extract, Transform, Load) pipeline using Apache Airflow. The pipeline downloads web server access logs, extracts useful fields, transforms the data, and saves the cleaned output for further use.
---
```
📂 Project Structure
project/
│── dags/
│    └── ETL_Server_Access_Log_Processing.py   # Airflow DAG definition
│── web-server-access-log.txt                  # Raw input file (downloaded)
│── webserverextract.txt                       # Extracted fields
│── webservertransform.txt                     # Transformed lowercase data
│── captilized.txt                             # Final processed file
```
---
## 🔄 ETL Workflow

Download

Fetches the raw log file from a public URL.

Extract

Splits log lines by #.

Selects the first and fourth fields.

Transform

Converts the extracted fields to lowercase for consistency.

Load

Saves the transformed data into a final output file (captilized.txt).

Check

Prints the processed data to verify results.
---
## ⚙️ Airflow DAG

The DAG is defined with the following tasks:

download → extract → transform → load → check

Each task is implemented as a PythonOperator in Airflow.
---
# ✨ Author

# 👨‍💻 Onyinyechukwu Kenneth Nebe aka The Engineer
# 📧 kennethnebe@gmail.com
