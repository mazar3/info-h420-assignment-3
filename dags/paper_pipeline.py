"""Assignment 3 — Group 33, Maximilien Zarioh."""

from datetime import datetime, timedelta, timezone

from airflow.sdk import dag, task
import library


@dag(
    dag_id="paper_pipeline",
    schedule="@hourly",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "Maximilien Zarioh", "retries": 0},
    tags=["assignment-3", "group-33"],
)
def paper_pipeline():
    @task
    def discover_pdfs():
        return library.discover_pdfs(library.Settings.from_env())

    @task
    def convert_to_markdown(files):
        return library.convert_pdfs(files, library.Settings.from_env())

    @task
    def generate_metadata(converted):
        return library.generate_metadata(converted, library.Settings.from_env())

    @task
    def save_metadata(records):
        return library.save_metadata(records, library.Settings.from_env())

    @task
    def build_library(_inserted):
        return library.build_library(library.Settings.from_env())

    @task(retries=1, retry_delay=timedelta(seconds=30))
    def notify_discord(_library_count):
        return library.notify_discord(library.Settings.from_env())

    files = discover_pdfs()
    converted = convert_to_markdown(files)
    records = generate_metadata(converted)
    inserted = save_metadata(records)
    library_count = build_library(inserted)
    notify_discord(library_count)


paper_pipeline()
