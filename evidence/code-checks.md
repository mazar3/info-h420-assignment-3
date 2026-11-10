# Code checks

Checked on 10 November 2026 with Python 3.12 and Apache Airflow 3.1.0.

`python -m unittest discover -s tests -v`: 11 tests passed.

The checks cover JSON parsing and one retry, basic field types, a real PDF-to-Markdown conversion, filename uniqueness, empty runs, HTML escaping and encoded links, case-insensitive search executed with Node.js, and notification retry without resending confirmed messages. Model responses and webhook requests are mocked in these tests. No live Discord message is sent by the test suite.

An Airflow `DagBag` load also passed with no import errors. Assertions verified one DAG (`paper_pipeline`), its hourly schedule, all six tasks and their linear dependencies, `catchup=False`, `max_active_runs=1`, and one notification-task retry.

These are code checks. They do not establish a live Airflow run, an actual local model response, Google Drive sync or Discord delivery. The separate [run results](run-results.json), [model outputs](metadata.json) and [library capture](library.jpeg) record the real-service demonstration.
