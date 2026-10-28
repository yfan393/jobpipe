# jobpipe feed

Fetches public job-board feeds for a daily job search and commits the results, because the assistant's
workspace can read GitHub but not the job boards themselves.

| Schedule (UTC) | Job | Output |
|---|---|---|
| daily 10:00 | merge `registry_additions.csv`, then `collect` every employer feed + new-grad lists + Ask HN | `data/latest/raw.json`, `data/latest/doctor.txt` |
| Sundays 08:30 | `registry-check`: verify every board in `companies.csv` | `data/registry_report.json`, `companies.csv` |
| 3rd of each month 07:00 | DOL LCA disclosure data → `sponsors.json`; `registry-grow` adds sponsors with a working board | `sponsors.json`, `companies.csv`, `data/registry_grow.txt`, `data/registry_candidates.csv` |

Run any job by hand: Actions → jobpipe → Run workflow → choose `collect`, `registry-check` or `sponsors`.

Requires Settings → Actions → General → Workflow permissions → **Read and write permissions**.

Only public data is here: postings, employer names, public DOL / USCIS statistics. No resumes or personal data.
`python -m unittest test_jobpipe` runs the tests (the resume-matching tests are skipped here).
