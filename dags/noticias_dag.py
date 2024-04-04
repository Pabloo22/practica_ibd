import json
import datetime
from enum import Enum, auto
from bs4 import BeautifulSoup
import requests

import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator


class Journal(Enum):
    EL_PAIS = auto()
    ABC = auto()


def fetch_webpage_content(url: str) -> str:
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36"
    }
    response = requests.get(url, headers=headers, timeout=10)
    if response.status_code == 200:
        return response.text

    raise RuntimeError(
        "Error: Unable to fetch the webpage with status code "
        f"{response.status_code}"
    )


def get_description(link: str) -> str:
    try:
        html_content = fetch_webpage_content(link)
        soup = BeautifulSoup(html_content, "lxml")
        description = soup.find("h2")
        return (
            description.text.strip()
            if description
            else "No description found."
        )
    except RuntimeError as e:
        return str(e)


def get_date_from_link(link_noticia: str, journal: Journal) -> str:
    if journal == Journal.EL_PAIS:
        # Example link:
        # https://elpais.com/espana/madrid/2024-04-02/nombre-noticia.html
        date_str = link_noticia.split("/")[-2]
        return date_str
    if journal == Journal.ABC:
        # Example link:
        # https://www.abc.es/espana/madrid/nombre-noticia-20240402180803-nt.html
        last_part = link_noticia.split("/")[-1]
        date_str_and_code = last_part.split("-")[-2]
        date_str = date_str_and_code[:8]
        # format to YYYY-MM-DD
        date = datetime.datetime.strptime(date_str, "%Y%m%d")
        date_str = date.strftime("%Y-%m-%d")
        return date_str

    raise ValueError(f"Invalid journal: {journal}")


def parse_html_for_news(
    html_content: str, journal: Journal
) -> list[dict[str, str]]:
    soup = BeautifulSoup(html_content, "lxml")
    titles = soup.find_all("h2")

    data = []
    today = datetime.datetime.now().strftime("%Y-%m-%d")
    for title in titles:
        a = title.find("a")
        if not a:
            continue

        link = a["href"]
        date_from_link = get_date_from_link(link, journal)
        if date_from_link == today:
            data.append(
                {
                    "title": title.text.strip(),
                    "link": link,
                    "description": get_description(link),
                    "journal": journal.name,
                }
            )
    return data


def save_news_to_json(data: list[dict[str, str]], file_path: str):
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def extract_news_data(journal: Journal) -> list[dict[str, str]]:
    if journal == Journal.EL_PAIS:
        url = "https://elpais.com/espana/madrid/"
    elif journal == Journal.ABC:
        url = "https://www.abc.es/espana/madrid/?ref=https%3A%2F%2Fwww.abc.es%2Fespana%2Fmadrid%2F"
    else:
        raise ValueError("Invalid journal.")

    html_content = fetch_webpage_content(url)
    data = parse_html_for_news(html_content, journal)
    return data


def extract_news_data_from_all_journals() -> list[dict[str, str]]:
    data = []
    for journal in Journal:
        data.extend(extract_news_data(journal))
    return data


default_args = {
    "start_date": pendulum.datetime(2024, 4, 1, tz="UTC"),
    "retries": 2,
    "retry_delay": pendulum.duration(seconds=2),
    "catchup": False,
}
with DAG(
    dag_id="noticias",
    schedule_interval="35 23 * * *",
    tags=["noticias"],
    default_args=default_args,
) as dag:

    @task(task_id="extract_news_data")
    def extract():
        return extract_news_data_from_all_journals()

    @task(task_id="save_news_to_json")
    def load_raw(data):
        # Generate a unique filename based on the current timestamp
        timestamp_str = (str(pendulum.now(tz="UTC"))[:10]).replace("-", "_")
        file_path = f"/opt/airflow/raw/noticias_{timestamp_str}.json"
        save_news_to_json(data, file_path)

    extract_task = extract()
    save_task = load_raw(extract_task)

    end_task = PythonOperator(
        task_id="end",
        python_callable=lambda: print(
            "Data extraction completed successfully"
        ),
    )
    # pylint: disable=pointless-statement
    extract_task >> save_task >> end_task
