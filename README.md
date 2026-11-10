# Research paper library — Assignment 3

INFO-H420, Group 33. Individual submission by Maximilien Zarioh.

This project uses one local Airflow DAG to turn research PDFs into a small searchable library. The PDFs and HTML page are in a Google Drive-synced folder. The SQLite database, extracted Markdown and Airflow files stay outside that folder.

## Workflow

```text
discover_pdfs → convert_to_markdown → generate_metadata
             → save_metadata → build_library → notify_discord
```

The `paper_pipeline` DAG runs every hour. It uses `catchup=False` to avoid historical runs and `max_active_runs=1` to avoid overlapping writes.

1. Read filenames in `papers/` and compare them with SQLite.
2. Convert each new PDF to Markdown with PyMuPDF4LLM. Only filenames and local Markdown paths pass between these tasks.
3. Call `qwen2.5:3b` served locally by Ollama through the Python OpenAI client. JSON mode requests a title, summary, keywords and category. `json.loads()` checks the response. Invalid JSON or basic field types trigger one further model call; two invalid responses fail the task.
4. Insert one row per filename. Keywords are stored as a JSON string. `filename` is the primary key, and an existing row is not inserted again.
5. Rebuild `output/index.html`. Its search covers the title, summary, keywords, category and filename. Text is HTML-escaped and PDF links are URL-encoded. The page works locally without a server or external JavaScript.
6. Send one Discord message per pending paper, then set its `notified` flag. The notification task retries once after 30 seconds. A later scheduled run also sends remaining pending messages.

An empty scan is successful: the model is not called, the HTML is regenerated, and pending notifications are still checked. No messages are sent for previously notified papers.

## Files

```text
work-area/
├── assignment-3/                 # this Git repository
│   ├── dags/paper_pipeline.py    # schedule and task dependencies
│   ├── dags/library.py           # pipeline functions
│   ├── dags/library.html         # HTML and search template
│   ├── tests/test_library.py
│   ├── requirements.txt
│   └── .env.example
└── runtime/                      # private, outside the repository
    ├── config.env
    ├── venv/
    ├── airflow/
    └── data/
        ├── papers.db
        └── markdown/

Google Drive/ResearchLibrary/
├── papers/                      # input PDFs
└── output/index.html            # generated library
```

Keep the whole work area on a local disk, outside cloud sync. The database path is checked against the configured ResearchLibrary folder. Also avoid placing it inside another synced folder such as OneDrive. Google Drive for desktop must actually sync `ResearchLibrary`, and its PDFs must be available offline. Merely creating a folder named “Google Drive” does not configure sync.

## Install

Use Python 3.12. From the repository directory:

```sh
mkdir -p ../runtime
python3.12 -m venv ../runtime/venv
source ../runtime/venv/bin/activate
python -m pip install -r requirements.txt \
  --constraint https://raw.githubusercontent.com/apache/airflow/constraints-3.1.0/constraints-3.12.txt
python -m pip check
cp .env.example ../runtime/config.env
chmod 600 ../runtime/config.env
```

Edit `../runtime/config.env` with your absolute paths and Discord webhook URL. Never commit the real webhook: anyone with that URL can post to its channel. Create it in a Discord text channel under Edit Channel → Integrations → Webhooks.

Install [Ollama](https://ollama.com/download). Start its local server in another terminal with enough context for the paper excerpt:

```sh
OLLAMA_CONTEXT_LENGTH=16384 ollama serve
```

If Ollama is already running, stop that instance before starting this one. Then download the model:

```sh
ollama pull qwen2.5:3b
```

`LLM_API_KEY=ollama` is a placeholder required by the client; local Ollama does not require a paid OpenAI account. This project only allows a localhost LLM endpoint.

## Run

On macOS, uncomment `no_proxy="*"` and `NO_PROXY="*"` in your private configuration before starting Airflow. This avoids a system proxy lookup hanging in a forked worker, as explained in the [Python 3.12 documentation](https://docs.python.org/3.12/library/urllib.request.html). Use this setting for local development where a network proxy is not required; it bypasses proxies for all requests. Restart Airflow after changing it.

In a terminal in the repository directory:

```sh
source ../runtime/venv/bin/activate
set -a
source ../runtime/config.env
set +a
mkdir -p "$RESEARCH_LIBRARY_DIR/papers" "$RESEARCH_LIBRARY_DIR/output"
airflow standalone
```

Open [Airflow](http://localhost:8080) and use the login details printed by `standalone`. Enable `paper_pipeline`. Put a text-based research PDF in the synced `papers/` folder, then trigger the DAG or wait for its hourly schedule. Keep Airflow and Ollama running, and keep the disk containing the runtime connected.

After all six tasks succeed, open `ResearchLibrary/output/index.html` locally. Google Drive stores the HTML file but does not host it as a website. Clicking a paper title opens the neighbouring PDF. The Discord channel should contain a new-paper notification.

## Check the result

Run the automated tests from the repository directory. They use temporary files and mock model/webhook calls; one test converts a real generated PDF. If Node.js is available, another test executes the search JavaScript.

```sh
python -m unittest discover -s tests -v
airflow dags list-import-errors
```

For an end-to-end check, use the real services and inspect:

- Airflow: all six tasks are green.
- Local `data/markdown/`: the PDF has a Markdown file.
- SQLite: `SELECT filename, title, category, notified FROM papers;` returns one row, with `notified = 1` after Discord confirms delivery.
- HTML: the paper appears and search filters the list.
- Discord: the notification is present.

Trigger the DAG a second time with no new PDF. The number of database rows and Discord messages should remain unchanged. To check recovery, make the webhook unavailable for a new paper: its metadata and HTML should remain saved, and `notified` should stay `0`. Restore the webhook and rerun; only pending messages should be sent. The unit tests cover this failure without posting real messages.

## Demonstration

On 10 November 2026, the real standalone DAG processed two PDFs: [Attention Is All You Need](https://arxiv.org/abs/1706.03762) and [Retrieval-Augmented Generation for Knowledge-Intensive NLP Tasks](https://arxiv.org/abs/2005.11401). Both were converted, summarized by local Ollama, stored in SQLite, displayed in the HTML library and notified to Discord. A second run found no new papers and sent no new messages. The initial macOS proxy lookup issue was resolved using the setting above.

[Run results](evidence/run-results.json) and [generated metadata](evidence/metadata.json) contain the recorded outputs. The source PDFs and private configuration are not included.

## Scope and limitations

- Filenames are assumed unique, as allowed by the assignment. Replacing a file under the same name does not reprocess it. Use a new filename for a revised paper.
- The model reads the first 24,000 characters of Markdown (`LLM_MAX_CHARS`). A log warning records truncation. Long papers may therefore have incomplete summaries. This is a small study tool, and generated text should be checked against the source.
- Text-based PDFs are expected. Scanned images requiring OCR are outside this implementation.
- A bad PDF or a model failure stops the batch before insertion. Fix the input or service, then rerun the DAG.
- Discord and SQLite do not share a transaction. Normal reruns and confirmed failures do not resend successful messages, but a crash or lost response after Discord accepts a message can cause a duplicate on retry.
- The database is local. Drive shares the PDFs and HTML, not a live multi-user application.

## Documentation used

- [Airflow 3.1.0 quick start](https://airflow.apache.org/docs/apache-airflow/3.1.0/start.html) and [TaskFlow tutorial](https://airflow.apache.org/docs/apache-airflow/3.1.0/tutorial/taskflow.html).
- [PyMuPDF4LLM API](https://pymupdf.readthedocs.io/en/latest/pymupdf4llm/api.html). The project pins version 0.0.27 with PyMuPDF 1.26.5 for its simple `to_markdown()` API.
- [Ollama OpenAI compatibility](https://docs.ollama.com/api/openai-compatibility) and [OpenAI structured output documentation](https://developers.openai.com/api/docs/guides/structured-outputs).
- [Discord execute webhook](https://docs.discord.com/developers/resources/webhook#execute-webhook), including `wait=true` and `allowed_mentions`.
