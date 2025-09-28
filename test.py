from sqlalchemy import create_engine
engine = create_engine("postgresql+psycopg2://airflow_user:airflow_pass@localhost:5432/tolldata")
with engine.connect() as conn:
    print(conn)
