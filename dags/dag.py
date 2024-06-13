from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
import requests
from bs4 import BeautifulSoup
import csv
import re
import time
import os

PURPLE = "\033[94m"
GREEN = "\033[92m"
RED = "\033[91m"
RESET = "\033[0m"

# Function to extract data from a given URL using BeautifulSoup
def extract_data(url):
    start_time = time.strftime("%Y%m%d-%H%M%S")
    print(f"\nExtracting data from {url}")
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    
    # Extracting links and articles from the page
    links = [link.get('href') for link in soup.find_all('a', href=True)]
    articles = soup.find_all('article')
    article_data = []
    for idx, article in enumerate(articles):
        title = article.find('h2').text.strip() if article.find('h2') else None
        description = article.find('p').text.strip() if article.find('p') else None
        article_data.append({'id': idx+1, 'title': title, 'description': description, 'source': url})
    
    end_time = time.strftime("%Y%m%d-%H%M%S")
    print(f"Scrapped data in {calculate_duration(start_time, end_time)} seconds")
    print(f"Scrapped {len(article_data)} articles and {len(links)} links from {url}")
    return links, article_data

# Function to save articles data to a CSV file
def save_to_csv(file_name, articles):
    with open(file_name, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['id', 'title', 'description', 'source']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for article in articles:
            writer.writerow(article)

# Function to preprocess text by removing HTML tags and non-alphabet characters
def preprocess(text):
    clean_text = re.sub('<.*?>', '', text)
    clean_text = re.sub('[^a-zA-Z]', ' ', clean_text)
    clean_text = clean_text.lower()
    clean_text = re.sub(' +', ' ', clean_text)
    return clean_text

# Function to clean the extracted data using preprocessing function
def clean_data(data):
    cleaned_data = []
    for article in data:
        article['title'] = preprocess(article['title']) if article.get('title') else None
        article['description'] = preprocess(article['description']) if article.get('description') else None
        cleaned_data.append(article)
    return cleaned_data

# Helper function to calculate duration between two timestamps
def calculate_duration(start_time, end_time):
    start = time.strptime(start_time, "%Y%m%d-%H%M%S")
    end = time.strptime(end_time, "%Y%m%d-%H%M%S")
    duration = time.mktime(end) - time.mktime(start)
    return duration

# Commands to manage version control and data version control
def git_push():
    os.system('git status')
    os.system('git pull')
    os.system('git status')
    os.system('git add .')
    os.system('git status')
    os.system('git commit -m "updated automatically by dvc"')
    os.system('git status')
    os.system('git push origin main')
    os.system('git status')

def dvc_push():
    os.system('dvc add data/data.csv')
    os.system('dvc push')

urls = ['https://www.dawn.com/', 'https://www.bbc.com/']
file_name = "/home/maryam/project/airflow-pipeline/data/data.csv"

# Extracts and processes data for all given URLs
def extract_data_task(urls):
    print("Extract data task")
    all_data = []
    for url in urls:
        links, articles = extract_data(url)
        all_data.extend(articles)
    return all_data

# Preprocesses extracted data
def preprocess_data_task(data):
    print("Preprocess data task")
    cleaned_data = clean_data(data)
    return cleaned_data

# Saves the cleaned data to a CSV file
def save_data_task(data, file_name):
    print("Save data task")
    save_to_csv(file_name, data)

# DAG configuration for the Airflow tasks
default_args = {
    'owner': 'maryam',
}

dag = DAG(
    dag_id='A2',
    default_args=default_args,
    tags=['Assignment_2', 'MLOPs'],
    catchup=False,
    schedule=None,
    start_date=datetime(2024, 5, 9),
)

# Setting up Airflow operators for tasks
with dag:
    extract_task = PythonOperator(
        task_id='extract_task',
        python_callable=extract_data_task,
        op_kwargs={'urls': urls},
        provide_context=True
    )

    preprocess_task = PythonOperator(
        task_id='preprocess_task',
        python_callable=preprocess_data_task,
        op_kwargs={'data': extract_task.output},
        provide_context=True
    )

    save_task = PythonOperator(
        task_id='save_task',
        python_callable=save_data_task,
        op_kwargs={'file_name': filename, 'data': preprocess_task.output},
        provide_context=True
    )

    dvc_push_task = PythonOperator(
        task_id='dvc_push_task',
        python_callable=dvc_push,
    )

    git_push_task = PythonOperator(
        task_id='git_push_task',
        python_callable=git_push,
    )

# Define the execution order for the tasks
extract_task >> preprocess_task >> save_task >> dvc_push_task >> git_push_task

# Optionally, manually run the pipeline
def main():
    dawn_url = 'https://www.dawn.com/'
    bbc_url = 'https://www.bbc.com/'
    file_name = "/home/maryam/project/airflow-pipeline/data/data.csv"

    dawn_links, dawn_articles = extract_data(dawn_url)
    dawn_data = clean_data(dawn_articles, dawn_url)

    bbc_links, bbc_articles = extract_data(bbc_url)
    bbc_data = clean_data(bbc_articles, bbc_url)

    data = dawn_data + bbc_data
    save_to_csv(file_name, data)

    print(f"Data saved to{RED} {file_name} {RESET}\n")

if __name__ == '__main__':
    main()
