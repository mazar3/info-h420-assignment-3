import json
import os
from pathlib import Path
import re
import shutil
import sqlite3
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import MagicMock, patch
from urllib.error import HTTPError

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dags"))
import library

METADATA = {"title": "A paper", "summary": "A useful study.",
            "keywords": ["machine learning"], "category": "Data Science"}


class LibraryTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        root = Path(self.directory.name)
        self.settings = library.Settings(root / "drive", root / "local/papers.db",
                                         root / "local/markdown", webhook="https://discord.com/api/webhooks/test/secret")
        (self.settings.library_dir / "papers").mkdir(parents=True)

    def rows(self):
        with sqlite3.connect(self.settings.database) as db:
            return db.execute("SELECT filename, notified FROM papers ORDER BY filename").fetchall()

    def fake_client(self, responses):
        client = MagicMock()
        client.chat.completions.create.side_effect = [
            SimpleNamespace(choices=[SimpleNamespace(message=SimpleNamespace(content=text))])
            for text in responses
        ]
        return client

    def test_invalid_json_retries_once_and_recovers(self):
        client = self.fake_client(["not JSON", json.dumps(METADATA)])
        self.assertEqual(library.ask_model("paper", client, self.settings), METADATA)
        self.assertEqual(client.chat.completions.create.call_count, 2)
        self.assertEqual(client.chat.completions.create.call_args.kwargs["response_format"], {"type": "json_object"})

    def test_bad_types_retry_once_then_fail(self):
        client = self.fake_client([json.dumps({**METADATA, "keywords": "word"}), "[]"])
        with self.assertRaisesRegex(ValueError, "twice"):
            library.ask_model("paper", client, self.settings)
        self.assertEqual(client.chat.completions.create.call_count, 2)
        for content in ['{}', 'null', json.dumps({**METADATA, "title": 1}),
                        json.dumps({**METADATA, "keywords": [1]})]:
            with self.subTest(content=content), self.assertRaises(ValueError):
                library.parse_metadata(content)

    def test_real_pdf_conversion(self):
        import pymupdf

        source = self.settings.library_dir / "papers/study.pdf"
        with pymupdf.open() as document:
            page = document.new_page()
            page.insert_text((72, 72), "A research paper\nMachine learning studies data.")
            document.save(source)
        converted = library.convert_pdfs([str(source)], self.settings)
        text = Path(converted[0]["markdown_path"]).read_text()
        self.assertIn("Machine learning", text)
        self.assertEqual(converted[0]["filename"], "study.pdf")

    def test_duplicates_and_new_file_discovery(self):
        papers = self.settings.library_dir / "papers"
        for filename in ("old.pdf", "new.PDF", "ignore.txt"):
            (papers / filename).touch()
        record = {"filename": "old.pdf", **METADATA}
        self.assertEqual(library.save_metadata([record], self.settings), 1)
        self.assertEqual(library.save_metadata([record], self.settings), 0)
        self.assertEqual(self.rows(), [("old.pdf", 0)])
        self.assertEqual(library.discover_pdfs(self.settings), [str(papers / "new.PDF")])

    def test_empty_run_still_builds_html(self):
        self.assertEqual(library.discover_pdfs(self.settings), [])
        self.assertEqual(library.convert_pdfs([], self.settings), [])
        self.assertEqual(library.generate_metadata([], self.settings), [])
        self.assertEqual(library.save_metadata([], self.settings), 0)
        self.assertEqual(library.build_library(self.settings), 0)
        self.assertEqual(library.notify_discord(self.settings), 0)
        self.assertTrue((self.settings.library_dir / "output/index.html").is_file())

    def test_html_escapes_paper_data_and_encodes_pdf_links(self):
        record = {**METADATA, "filename": 'a #".pdf',
                  "title": '<script>alert("test")</script>', "category": 'A & B'}
        library.save_metadata([record], self.settings)
        library.build_library(self.settings)
        page = (self.settings.library_dir / "output/index.html").read_text()
        self.assertNotIn(record["title"], page)
        self.assertIn("&lt;script&gt;", page)
        self.assertIn("A &amp; B", page)
        self.assertIn('../papers/a%20%23%22.pdf', page)
        self.assertNotIn("innerHTML", page)

    @unittest.skipUnless(shutil.which("node"), "Node is needed to execute the search JavaScript")
    def test_search_javascript_filters_case_insensitively(self):
        page = Path(library.__file__).with_name("library.html").read_text()
        script = re.search(r"<script>(.*?)</script>", page, re.S).group(1)
        harness = '''
const elements = {
  search: {value: '', addEventListener() {}}, count: {}, empty: {}
};
const articles = [{textContent: 'Machine Learning study'}, {textContent: 'Process analysis'}];
const document = {getElementById: id => elements[id], querySelectorAll: () => articles};
'''
        assertions = '''
const assert = require('node:assert/strict');
assert.equal(elements.count.textContent, '2 of 2 papers');
elements.search.value = ' MACHINE LEARNING '; filterPapers();
assert.deepEqual(articles.map(a => a.hidden), [false, true]);
assert.equal(elements.count.textContent, '1 of 2 papers');
elements.search.value = '<script>'; filterPapers();
assert.equal(elements.empty.hidden, false);
elements.search.value = ''; filterPapers();
assert.equal(elements.count.textContent, '2 of 2 papers');
'''
        subprocess.run([shutil.which("node"), "-e", harness + script + assertions], check=True)

    def test_notification_failure_recovers_without_resending_successes(self):
        library.save_metadata([{"filename": name, **METADATA} for name in ("a.pdf", "b.pdf")], self.settings)
        with patch.object(library, "post_discord", side_effect=[None, RuntimeError("offline")]):
            with self.assertRaisesRegex(RuntimeError, "offline"):
                library.notify_discord(self.settings)
        self.assertEqual(self.rows(), [("a.pdf", 1), ("b.pdf", 0)])
        with patch.object(library, "post_discord") as send:
            self.assertEqual(library.notify_discord(self.settings), 1)
            self.assertEqual(send.call_args.args[1]["filename"], "b.pdf")
            self.assertEqual(library.notify_discord(self.settings), 0)
            self.assertEqual(send.call_count, 1)
        self.assertEqual(self.rows(), [("a.pdf", 1), ("b.pdf", 1)])

    def test_webhook_waits_for_confirmation_and_disables_mentions(self):
        with patch.object(library, "urlopen") as send:
            library.post_discord(self.settings.webhook, {"filename": "a.pdf", **METADATA})
        request = send.call_args.args[0]
        self.assertIn("wait=true", request.full_url)
        self.assertEqual(json.loads(request.data)["allowed_mentions"], {"parse": []})

    def test_webhook_errors_do_not_expose_secret(self):
        error = HTTPError(self.settings.webhook, 429, "limited", {}, None)
        with patch.object(library, "urlopen", side_effect=error):
            with self.assertRaisesRegex(RuntimeError, "HTTP 429") as caught:
                library.post_discord(self.settings.webhook, {"filename": "a.pdf", **METADATA})
        self.assertNotIn("secret", str(caught.exception))
        self.assertTrue(caught.exception.__suppress_context__)

    def test_database_cannot_be_inside_synced_library(self):
        with patch.dict(os.environ, {"RESEARCH_LIBRARY_DIR": str(self.settings.library_dir),
                                     "SQLITE_PATH": str(self.settings.library_dir / "papers.db")}):
            with self.assertRaisesRegex(ValueError, "outside"):
                library.Settings.from_env()


if __name__ == "__main__":
    unittest.main()
