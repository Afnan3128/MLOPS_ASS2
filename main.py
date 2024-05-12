from bs4 import BeautifulSoup
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os

# Sources for data extraction
sources = ['https://www.dawn.com/', 'https://www.bbc.com/']

def extract():
    links = []
    titles = []
    descriptions = []
    for source in sources:
        reqs = requests.get(source)
        soup = BeautifulSoup(reqs.text, 'html.parser')
        for link in soup.find_all('a'):
            links.append(link.get('href'))
        for article in soup.find_all('article'):
            title = article.find('h1')
            description = article.find('p')
            if title and description:
                titles.append(title.text.strip())
                descriptions.append(description.text.strip())
    return links, titles, descriptions

def transform(links, titles, descriptions):
    # Preprocess the extracted data (e.g., clean, format)
    # Placeholder function
    transformed_data = [(link, title, description) for link, title, description in zip(links, titles, descriptions)]
    return transformed_data

def load(transformed_data):
    # Mount Google Drive as a local drive
    mount_point = '/path/to/your/mounted/google/drive'  # Replace with your mount point
    if not os.path.exists(mount_point):
        os.system('google-drive-ocamlfuse /path/to/your/mounted/google/drive')

    # Define the file path
    file_path = os.path.join(mount_point, 'processed_data.txt')

    # Save transformed data to a text file
    with open(file_path, 'w') as f:
        for link, title, description in transformed_data:
            f.write(f'{link}\t{title}\t{description}\n')

# Define the Airflow DAG
default_args = {
    'owner': 'airflow-demo',
    'start_date': datetime(2024, 5, 12),
    'catchup': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    'mlops-dag',
    default_args=default_args,
    description='MLOps DAG for data extraction, transformation, and storage',
    schedule_interval='@daily',
    max_active_runs=1,
)

# Define tasks
task_extract = PythonOperator(
    task_id='task_extract',
    python_callable=extract,
    dag=dag,
)

task_transform = PythonOperator(
    task_id='task_transform',
    python_callable=transform,
    dag=dag,
)

task_load = PythonOperator(
    task_id='task_load',
    python_callable=load,
    dag=dag,
)

# Define task dependencies
task_extract >> task_transform >> task_load
