"""Offline tests for jobpipe v2. Run from the scripts folder:  python3 -m unittest test_jobpipe -v
Feeds are mocked; nothing touches the network. Cases marked (Oct 2026) replay real mistakes."""
import datetime as dt
import json
import os
import tempfile
import unittest

import jobpipe as jp

NOW = jp.NOW
H = lambda h: (NOW - dt.timedelta(hours=h)).isoformat()
C = {"company": "Acme Pay", "sector": "fintech", "startup": "yes", "atlanta": "no", "ats_slug": "acme", "ats": "greenhouse"}
DESC_OK = ("Requirements: Master's degree and 1+ years of experience in analytics. Python, SQL, pandas, scikit-learn, "
           "machine learning, statistics, forecasting. We sponsor H-1B visas. " * 3)


class FakeHTTP:
    def __init__(self, payloads):
        self.payloads = payloads
        self.calls = []

    def __call__(self, url, data=None, **kw):
        self.calls.append(url)
        for key, body in self.payloads.items():
            if key in url:
                return 200, json.dumps(body), "application/json"
        return 404, "", ""


def J(**kw):
    d = {"company": "Acme Pay", "title": "Data Scientist", "location": "Atlanta, GA", "url": "https://boards.greenhouse.io/acme/jobs/123456",
         "apply_url": "https://boards.greenhouse.io/acme/jobs/123456", "posted_at": H(5), "description": DESC_OK,
         "sector": "fintech", "source": "Employer job board", "source_group": "Employer job board", "req_id": "R1"}
    d.update(kw)
    return d


class Fetchers(unittest.TestCase):
    def setUp(self):
        self.orig = jp.http

    def tearDown(self):
        jp.http = self.orig

    def test_greenhouse_lever_ashby(self):
        jp.http = FakeHTTP({
            "boards-api.greenhouse.io": {"jobs": [{"title": "Data Scientist, Risk", "location": {"name": "New York, NY"},
                                                   "absolute_url": "https://x/1", "requisition_id": "R123", "id": 9,
                                                   "first_published": H(5), "content": "&lt;p&gt;2+ years.&lt;/p&gt;"}]},
            "api.lever.co": [{"text": "ML Engineer", "categories": {"location": "Remote - US"}, "hostedUrl": "https://l/1", "id": "abc",
                              "createdAt": int((NOW - dt.timedelta(hours=3)).timestamp() * 1000), "descriptionPlain": "Build",
                              "lists": [{"content": "<li>Python</li>"}]}],
            "api.ashbyhq.com": {"jobs": [{"title": "FDE", "location": "SF, CA", "jobUrl": "https://a/1", "id": "z",
                                          "publishedAt": H(2), "isListed": True}, {"title": "Hidden", "isListed": False}]}})
        _, gh = jp.fetch_greenhouse(C)
        _, lv = jp.fetch_lever(C)
        _, ab = jp.fetch_ashby(C)
        self.assertEqual(gh[0]["req_id"], "R123")
        self.assertIn("2+ years", gh[0]["description"])
        self.assertEqual(gh[0]["apply_url"], "https://x/1")
        self.assertIn("Python", lv[0]["description"])
        self.assertEqual(len(ab), 1)

    def test_workday_and_detail_enrichment(self):
        jp.http = FakeHTTP({"/jobs": {"jobPostings": [{"title": "Model Validation Analyst", "externalPath": "/job/Atlanta/MVA_R555",
                                                       "locationsText": "Atlanta, GA", "postedOn": "Posted 2 Days Ago", "bulletFields": ["R555"]}]},
                            "/job/Atlanta/MVA_R555": {"jobPostingInfo": {"jobDescription": "<p>Python required</p>",
                                                                         "startDate": (NOW - dt.timedelta(days=3)).date().isoformat(), "jobReqId": "R555"}}})
        c = {"company": "Truist", "sector": "finance", "workday_url": "https://truist.wd1.myworkdayjobs.com/Careers", "ats": "workday"}
        st, jobs = jp.fetch_workday(c, queries=["data"])
        self.assertEqual(st, 200)
        self.assertEqual(jobs[0]["req_id"], "R555")
        self.assertAlmostEqual(jp.age_hours(jobs[0]["posted_at"]), 48, delta=1)
        jp.enrich_workday(jobs[0])
        self.assertIn("Python", jobs[0]["description"])
        self.assertGreater(jp.age_hours(jobs[0]["posted_at"]), 60)  # stricter of relative age and exact date

    def test_oracle(self):
        jp.http = FakeHTTP({"recruitingCEJobRequisitions": {"items": [{"requisitionList": [
            {"Id": "210612345", "Title": "Quant Analytics Associate", "PostedDate": (NOW - dt.timedelta(hours=20)).date().isoformat(),
             "PrimaryLocation": "New York, NY, United States", "ShortDescriptionStr": "Model credit risk"}]}]},
            "recruitingCEJobRequisitionDetails": {"items": [{"ExternalDescriptionStr": "<p>3+ years of experience in analytics</p>",
                                                             "ExternalQualificationsStr": "Python"}]}})
        c = {"company": "JPMorgan Chase", "sector": "finance", "ats": "oracle", "ats_slug": "jpmc.fa.oraclecloud.com|CX_1001"}
        st, jobs = jp.fetch_oracle(c, queries=["data"])
        self.assertEqual(st, 200)
        self.assertEqual(jobs[0]["url"], "https://jpmc.fa.oraclecloud.com/hcmUI/CandidateExperience/en/sites/CX_1001/job/210612345")
        self.assertEqual(jp.apply_link_problem(jobs[0]["url"]), "")
        jp.enrich_oracle(jobs[0])
        self.assertIn("3+ years", jobs[0]["description"])

    def test_multi_board_company(self):
        fake = FakeHTTP({"boards/main/jobs": {"jobs": [{"title": "Quant Analyst", "absolute_url": "https://x/m1", "id": 1}]},
                         "boards/campus/jobs": {"jobs": [{"title": "Quant Researcher New Grad", "absolute_url": "https://x/c1", "id": 2}]}})
        jp.http = fake
        r = jp.fetch_company({"company": "CTC", "ats": "greenhouse", "ats_slug": "main;campus", "sector": "quant"})
        self.assertEqual(r["status"], 200)
        self.assertEqual({j["url"] for j in r["jobs"]}, {"https://x/m1", "https://x/c1"})

    def test_speedyapply_and_simplify(self):
        md = ("### FAANG+\n| Company | Position | Location | Salary | Posting | Age |\n|---|---|---|---|---|---|\n"
              '| <a href="https://www.nvidia.com"><strong>NVIDIA</strong></a> | Software Engineer - New College Grad 2026 | Santa Clara, CA | $172k/yr | '
              '<a href="https://nvidia.wd5.myworkdayjobs.com/en-US/x/job/US-CA/SE_JR2012979"><img src="i" alt="Apply"/></a> | 1d |\n')
        rows = jp.parse_speedyapply(md)
        self.assertEqual(rows[0]["company"], "NVIDIA")
        self.assertIn("myworkdayjobs", rows[0]["apply_url"])
        self.assertAlmostEqual(jp.age_hours(rows[0]["posted_at"]), 24, delta=1)
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
            json.dump([{"company_name": "Ramp", "title": "Software Engineer, New Grad", "locations": ["New York, NY"], "url": "https://jobs.ashbyhq.com/ramp/abc-123",
                        "date_posted": int((NOW - dt.timedelta(hours=10)).timestamp()), "active": True, "is_visible": True,
                        "sponsorship": "U.S. Citizenship is Required", "category": "Software", "id": "x"}], f)
        _, sj = jp.fetch_simplify(f.name)
        os.unlink(f.name)
        s = jp.screen_one(sj[0], 30)
        self.assertIn("citizenship required", s["exclude_reasons"])


class Ages(unittest.TestCase):
    def test_relative_ages(self):
        cases = {"Today": 0, "Just posted": 0, "Posted Today": 0, "Yesterday": 24, "3h": 3, "2d": 48, "1 day ago": 24,
                 "Posted 3 Days Ago": 72, "2 weeks ago": 336, "45 minutes ago": 0.75}
        for t, h in cases.items():
            self.assertAlmostEqual(jp.parse_age(t), h, places=2, msg=t)
        self.assertGreater(jp.parse_age("Posted 30+ Days Ago"), 30 * 24)
        self.assertIsNone(jp.parse_age("New York, NY"))
        self.assertIsNone(jp.parse_age("$120k"))

    def test_relative_age_is_kept_not_dropped(self):  # rule 19 (Oct 27): 'Yesterday' is a date
        s = jp.screen_one(J(posted_at="", age_text="Yesterday", source="Built In", source_group="Built In"), 30)
        self.assertEqual(s["screen"], "pass")
        self.assertTrue(any("relative age" in f for f in s["flags"]))
        s2 = jp.screen_one(J(posted_at="", age_text="5 days ago", source="Built In"), 30)
        self.assertTrue(any(r.startswith("stale") for r in s2["exclude_reasons"]))


class Classify(unittest.TestCase):
    def cls(self, t, **kw):
        return jp.classify({"title": t, **kw})

    def test_programs(self):
        self.assertEqual(self.cls("Data Scientist"), ("professional", "data_science"))
        self.assertEqual(self.cls("Software Engineer, New Grad 2026"), ("newgrad", "sde"))
        self.assertEqual(self.cls("Machine Learning Engineer I"), ("newgrad", "mle"))
        self.assertEqual(self.cls("Machine Learning Engineer"), ("professional", "ml_ai_engineering"))
        self.assertEqual(self.cls("AI Engineer - Early Career"), ("newgrad", "ai"))
        self.assertEqual(self.cls("Quantitative Researcher, New Grad"), ("newgrad", "quant"))
        self.assertEqual(self.cls("Data Scientist, New Grad"), ("professional", "data_science"))  # entry DS stays in the main tracker
        self.assertEqual(self.cls("Software Engineer"), ("", ""))  # experienced SWE is not a target
        self.assertEqual(self.cls("Software Engineer", program_hint="newgrad"), ("newgrad", "sde"))
        self.assertEqual(self.cls("Senior Applied AI Engineer I")[0], "professional")  # not a new-grad role (Oct 2026)

    def test_her_applied_titles_are_on_target(self):  # Oct 2026: she applied to these; v1 called them off-target
        for t in ["Senior Analyst, Pricing & Monetization", "Trading Risk and Control, Analyst",
                  "Mortgage Servicing Right Hedging – Analyst", "Sr. Financial Analyst", "Principal Associate, Data Science"]:
            self.assertTrue(self.cls(t)[0], t)


class Gates(unittest.TestCase):
    def sen(self, t, prog="professional", text=""):
        return jp.seniority_reason(t, prog, text)

    def test_title_gate(self):
        for t in ["Sr Machine Learning Engineer", "Senior Data Engineer", "Manager, Risk Management", "Manager, Economic Modeling",
                  "Principal Associate, Data Science", "Staff Data Scientist", "Machine Learning Engineer 4", "Data Science Intern"]:
            self.assertTrue(self.sen(t), t)
        for t in ["Senior Associate, Data Scientist", "Senior Data Analyst", "Data Scientist III", "Machine Learning Engineer II",
                  "Senior Consultant - Data & Analytics", "Senior Data Scientist", "Member of Technical Staff", "Sr. Quantitative Finance Analyst"]:
            self.assertEqual(self.sen(t), "", t)
        self.assertTrue(self.sen("Software Engineer II", "newgrad"))
        self.assertEqual(self.sen("Software Engineer I, New Grad", "newgrad"), "")

    def test_newgrad_title_gate_is_word_bounded(self):  # Oct 28 migration re-check
        for t in ["Graduate - Engineer/Analyst for ISRT and BMC2 Applications", "Software Engineer - Internal Tools",
                  "Software Engineer - Level 1, 2, or 3", "Software Engineer I/II", "AI Labs Rotational Fellow - Emerging Talent"]:
            self.assertEqual(self.sen(t, "newgrad"), "", t)
        for t in ["Sr. Software Engineer", "Software Engineer 2", "Staff Software Engineer (Masters)", "Senior Fellow"]:
            self.assertTrue(self.sen(t, "newgrad"), t)
        self.assertTrue(self.sen("Distinguished Fellow, AI"))

    def test_years_by_function(self):
        g = jp.years_analysis("Requirements: 4+ years of data engineering experience with SQL, Python and cloud data warehouses.")
        self.assertTrue(g["gate"].startswith("too senior"))  # SoFi, Oct 2026
        g = jp.years_analysis("Basic qualifications: 3+ years of experience in data analytics or data science.")
        self.assertEqual(g["gate"], "")
        self.assertEqual(g["points"], 20)  # analytics row = 3
        g = jp.years_analysis("Minimum: 5+ years of experience in data science / machine learning in fraud or risk.")
        self.assertEqual(g["gate"], "")  # soft: Circle Senior DS was applied to
        self.assertLessEqual(g["points"], 10)
        self.assertTrue(any("gap" in f for f in g["flags"]))
        g = jp.years_analysis("Bachelor's degree and 5 years of financial analysis experience, or Master's degree and 3 years of "
                              "financial analysis experience.")
        self.assertIn("3 yrs finance modeling", g["ev"])
        g = jp.years_analysis("Required: 1+ year of experience with Python. Preferred: 5+ years of software engineering.")
        self.assertEqual(g["gate"], "")

    def test_exclusions(self):
        for d, reason in [("We do not sponsor visas.", "no sponsorship"), ("Must be authorized to work in the U.S. without sponsorship.", "no sponsorship"),
                          ("US citizens only.", "citizenship required"), ("Active TS/SCI clearance.", "clearance required"),
                          ("PhD required in statistics.", "PhD required")]:
            self.assertIn(reason, jp.screen_one(J(description=DESC_OK + d), 30)["exclude_reasons"], d)
        s = jp.screen_one(J(company="Booz Allen Hamilton"), 30)
        self.assertIn("defense / clearance employer", s["exclude_reasons"])  # Oct 2026 new-grad tracker leak
        self.assertIn("non-US location", jp.screen_one(J(location="London, UK"), 30)["exclude_reasons"])
        self.assertTrue(any("pay below floor" in r for r in jp.screen_one(J(pay="$60,000 - $70,000"), 30)["exclude_reasons"]))

    def test_repost_memory(self):
        seen = {jp.fingerprint("Acme Pay", "Data Scientist", "Atlanta, GA"): H(24 * 10)}
        s = jp.screen_one(J(), 30, seen)
        self.assertTrue(any("reposted" in r for r in s["exclude_reasons"]))


class Prescore(unittest.TestCase):
    def test_components_sum_and_bounds(self):
        s = jp.screen_one(J(), 30)
        self.assertEqual(s["screen"], "pass")
        self.assertEqual(sum(s["pre_components"].values()), s["pre_score"])
        for k, v in s["pre_components"].items():
            self.assertLessEqual(v, jp.RUBRIC["professional"][k])
        self.assertGreaterEqual(s["pre_score"], 70)  # strong DS fit, fintech, sponsors
        bad = jp.screen_one(J(title="Software Engineer, Frontend New Grad", description="React, TypeScript, Angular, CSS. " * 30,
                              program_hint="newgrad"), 30)
        self.assertLess(bad["pre_score"], s["pre_score"])

    def test_sponsorship_levels(self):
        jp._H1B = {"acme pay": {"approvals": 150, "fy": "2025"}}
        try:
            s = jp.screen_one(J(description=DESC_OK.replace("We sponsor H-1B visas.", "")), 30)
            self.assertEqual(s["sponsorship_level"], "h1b_100")
        finally:
            jp._H1B = None


class Links(unittest.TestCase):
    def test_apply_link_rule18(self):
        bad = ["https://www.linkedin.com/jobs/view/4482333354/", "https://careers.smbcgroup.com/", "https://builtin.com/job/x/123456",
               "https://hiring.cafe/job/abc", "https://boards.greenhouse.io/acme", "https://simplify.jobs/p/1234-abc",
               "https://gatech-csm.symplicity.com/students/app/jobs/123"]
        for u in bad:
            self.assertTrue(jp.apply_link_problem(u), u)
        for u in ["https://boards.greenhouse.io/acme/jobs/123456", "https://capitalone.wd12.myworkdayjobs.com/Capital_One/job/McLean/Data-Scientist_R229000",
                  "https://jobs.ashbyhq.com/ramp/1b2c3d4e-1111-2222-3333-444455556666", "https://www.amazon.jobs/en/jobs/2876543/data-scientist"]:
            self.assertEqual(jp.apply_link_problem(u), "", u)

    def test_linkedin_old_id(self):  # Oct 2026: FHLB Indianapolis 4432727169 ~100 days older
        pool = [{"url": "https://www.linkedin.com/jobs/view/4483800000/"}, {"url": "https://www.linkedin.com/jobs/view/4432727169/"}]
        jp.linkedin_age_check(pool)
        self.assertNotIn("exclude_reasons", pool[0])
        self.assertTrue(pool[1]["exclude_reasons"])


class Dedupe(unittest.TestCase):
    def test_relx_alias_and_req(self):  # Oct 2026: RELX applied twice
        trk = [{"company": "LexisNexis Risk Solutions", "title": "Data Scientist III", "location": "Alpharetta, GA", "status": "Applied", "req_id": ""}]
        out = jp.do_dedupe([{"company": "LexisNexis Risk Solutions (RELX)", "title": "Data Scientist III", "location": "Alpharetta, GA"}], trk)
        self.assertEqual(out[0]["screen"], "excluded")
        out = jp.do_dedupe([{"company": "Capital One", "title": "X", "location": "", "req_id": "R229000"}],
                           [{"company": "Capital One Services", "title": "Y", "req_id": "R229000", "status": "Applied"}])
        self.assertEqual(out[0]["screen"], "excluded")

    def test_us_bank_not_collapsed(self):
        self.assertNotEqual(jp.norm_company("U.S. Bank"), jp.norm_company("Bank"))
        self.assertEqual(jp.norm_company("SMBC Group"), jp.norm_company("SMBC"))

    def test_resolve_linkedin_to_feed(self):
        feed = [J(title="Data Scientist - Fraud", url="https://boards.greenhouse.io/acme/jobs/999999", location="Atlanta, GA")]
        pool = [{"company": "Acme Pay", "title": "Data Scientist, Fraud", "location": "Atlanta, GA", "source": "LinkedIn",
                 "url": "https://www.linkedin.com/jobs/view/4483800000/", "apply_url": ""}]
        self.assertEqual(jp.resolve_to_employer(pool, feed), 1)
        self.assertEqual(pool[0]["apply_url"], "https://boards.greenhouse.io/acme/jobs/999999")
        self.assertEqual(pool[0]["also_on_linkedin"], "yes")


class LedgerAndIngest(unittest.TestCase):
    def test_plan_covers_every_site_and_family(self):
        led = jp.build_plan(30, career_pages=False)
        sites = {it["site"] for it in led["items"]}
        for s in ("Google Jobs", "Indeed", "Glassdoor", "Wellfound", "Interstride", "GT CareerBuzz", "Employer feeds",
                  "SimplifyJobs GitHub", "speedyapply"):
            self.assertIn(s, sites)
        self.assertNotIn("LinkedIn", sites)  # Oct 28: LinkedIn is for referrals, not discovery
        order = [it["site"] for it in jp.build_plan(30)["items"]]
        self.assertLess(order.index("Career pages"), order.index("Google Jobs"))  # harvest before discovery
        self.assertIn("htichips=date_posted:3days", [it for it in led["items"] if it["site"] == "Google Jobs"][0]["url"])
        li = [it for it in jp.build_plan(30, sites=["LinkedIn"], career_pages=False)["items"] if it["site"] == "LinkedIn"]
        fams = {(it["program"], it["family"]) for it in li}
        self.assertEqual(len(fams), len(jp.CONFIG["query_families"]["professional"]) + len(jp.CONFIG["query_families"]["newgrad"]))
        self.assertIn("f_TPR=r108000", li[0]["url"])
        self.assertIn("f_E=1%2C2", [it for it in li if it["program"] == "newgrad"][0]["url"])

    def test_ingest_parses_cards_and_marks_done(self):
        led = jp.build_plan(30, sites=["LinkedIn"], career_pages=False)
        item = led["items"][0]["id"]
        cards = ("https://www.linkedin.com/jobs/view/4483812345/?refId=x ¦ Data Scientist ¦ Data Scientist with verification · Capital One · "
                 "McLean, VA (Hybrid) · $120K/yr - $150K/yr · Promoted · Easy Apply · 3 hours ago\n"
                 "https://www.linkedin.com/jobs/view/4470000000/ ¦ Credit Risk Analyst ¦ Truist · Atlanta, GA · Reposted 2 days ago\n")
        with tempfile.TemporaryDirectory() as d:
            pool = os.path.join(d, "pool.jsonl")
            rows = jp.do_ingest(led, item, cards, pool)
            self.assertEqual(len(rows), 2)
            self.assertEqual(rows[0]["company"], "Capital One")
            self.assertEqual(rows[0]["location"], "McLean, VA (Hybrid)")
            self.assertEqual(rows[0]["age_text"], "3 hours ago")
            self.assertEqual(rows[0]["url"], "https://www.linkedin.com/jobs/view/4483812345/")
            self.assertEqual(rows[0]["apply_url"], "")  # LinkedIn is never an apply link
            self.assertTrue(rows[1]["reposted"])
            self.assertEqual(led["items"][0]["status"], "done")
            scr = jp.screen_one(rows[1], 30)
            self.assertIn("reposted", scr["exclude_reasons"])

    def test_discovery_drops_known_companies_and_reads_google_cards(self):
        led = jp.build_plan(30, sites=["Google Jobs"], career_pages=False)
        item = led["items"][0]["id"]
        cards = ("Data Scientist, Credit Risk ¦ Capital One · McLean, VA · via LinkedIn · 3 hours ago\n"
                 "Risk Analytics Associate ¦ Unknown Fintech Co · Atlanta, GA · via Greenhouse · 1 day ago\n")
        with tempfile.TemporaryDirectory() as d:
            rows = jp.do_ingest(led, item, cards, os.path.join(d, "pool.jsonl"))
        self.assertEqual(rows[0]["known_company"], "Capital One")
        self.assertEqual((rows[1]["company"], rows[1]["via"], rows[1]["age_text"]), ("Unknown Fintech Co", "Greenhouse", "1 day ago"))
        self.assertNotIn("known_company", rows[1])
        self.assertIn("known company: harvested directly", jp.screen_one(rows[0], 30)["exclude_reasons"])
        self.assertEqual(jp.screen_one(rows[1], 30)["screen"], "pass")

    def test_audit_needs_every_item(self):
        led = jp.build_plan(30, sites=["LinkedIn", "Wellfound"], career_pages=False)
        ok, _ = jp.do_audit_ledger(led)
        self.assertFalse(ok)
        for it in led["items"]:
            it["status"] = "done"
        jp.mark_item(led, "wellfound*", "blocked", blocker="slow page")  # friction is not a blocker
        self.assertFalse(jp.do_audit_ledger(led)[0])
        jp.mark_item(led, "wellfound*", "blocked", blocker="Security check: verify you are human")
        self.assertTrue(jp.do_audit_ledger(led)[0])


class Finalize(unittest.TestCase):
    def scored(self, **kw):
        j = jp.screen_one(J(), 30)
        j["score"] = {"skills": {"pts": 26, "ev": "Python, SQL, credit models"}, "experience": {"pts": 18, "ev": "Asks 1 yr. Has 3."},
                      "role_fit": {"pts": 23, "ev": "credit risk DS"}, "domain_quals": {"pts": 9, "ev": "fintech, CFA"},
                      "preferences": {"pts": 12, "ev": "sponsors; Atlanta"}}
        j.update(kw)
        return j

    def test_rows_validated_and_routed(self):
        main, ng, na, leads = jp.do_finalize([self.scored()], [], [])
        self.assertEqual(len(main), 1)
        r = main[0]
        self.assertEqual(r["score"], 88)
        self.assertEqual(r["priority_group"], "High")
        self.assertEqual(r["skills_pts"], 26)
        self.assertEqual(r["score_max"]["skills_pts"], 30)
        self.assertEqual(jp.validate_rows([r]), [])

    def test_below_threshold_and_bad_link_not_added(self):
        low = self.scored()
        low["score"] = {k: {"pts": 5} for k in ("skills", "experience", "role_fit", "domain_quals", "preferences")}
        bad = self.scored(apply_url="https://www.linkedin.com/jobs/view/4483812345/", fp="other")
        main, ng, na, leads = jp.do_finalize([low, bad], [], [])
        self.assertEqual(main, [])
        self.assertEqual(na[f"score below {jp.RUBRIC['add_threshold']}"], 1)
        self.assertEqual(na["failed validation"], 1)
        self.assertEqual(len(leads), 1)

    def test_newgrad_row(self):
        j = jp.screen_one(J(title="Software Engineer, New Grad (Backend)", program_hint="newgrad"), 72)
        self.assertEqual(j["tracker"], "newgrad")
        j["score"] = {"skills": 22, "eligibility": 15, "role_fit": 22, "domain_quals": 8, "preferences": 14}
        main, ng, na, _ = jp.do_finalize([j], [], [])
        self.assertEqual(len(ng), 1)
        self.assertEqual(ng[0]["quick_fit"], "High")
        self.assertEqual(ng[0]["category"], "SWE/SDE")
        self.assertEqual(jp.validate_rows(ng, "newgrad"), [])

    def test_high_needs_experience(self):
        j = self.scored()
        j["score"]["experience"] = {"pts": 10}
        j["score"]["skills"] = {"pts": 30}
        j["score"]["preferences"] = {"pts": 15}
        main, *_ = jp.do_finalize([j], [], [])
        self.assertEqual(main[0]["priority_group"], "Medium")


@unittest.skipUnless(jp.load_resumes(), "resumes/ not present (the public feed repo collects only; scoring runs in the skill)")
class JDMatch(unittest.TestCase):
    CREDIT = ("Requirements: Master's in a quantitative field; 1+ years of experience in credit risk modeling; Python, SQL, "
              "XGBoost or LightGBM, statistics, regression; model validation and back-testing; monitor drift and explain models with SHAP. ") * 2
    FRONT = ("Requirements: 2+ years building web apps with React, TypeScript, JavaScript, CSS and HTML; mobile apps (iOS, Android) "
             "a plus; REST APIs; Git. Build UI components and design systems. ") * 2
    ODD = ("Own loss forecasting for the credit card portfolio: segmentation, forecasting models, A/B tests on credit line policy. "
           "Requirements: SQL and Python, statistics, credit risk or consumer finance experience, Tableau dashboards. ") * 2

    def setUp(self):
        self.res = jp.load_resumes()
        self.m = jp.build_matcher([{"description": t} for t in (self.CREDIT, self.FRONT, self.ODD)], self.res)

    def test_reads_resumes_and_ranks(self):
        self.assertEqual(len(self.res), 3)
        c, f = self.m.score(self.CREDIT), self.m.score(self.FRONT)
        self.assertGreater(c["match"], f["match"] + 0.4)
        self.assertEqual(c["resume"], "resume-quant-finance.md")
        self.assertIn("react", f["missing"])
        self.assertIn("python", c["has"])

    def test_title_outside_groups_kept_on_jd(self):
        jp.set_matcher(self.m)
        try:
            s = jp.screen_one(J(title="Portfolio Insights Associate", description=self.ODD), 30)
            self.assertEqual((s["screen"], s["family"]), ("pass", "other"))
            s2 = jp.screen_one(J(title="Account Executive", description=self.ODD), 30)
            self.assertIn("off-target title", s2["exclude_reasons"])
        finally:
            jp.set_matcher(None)


class ScoringIO(unittest.TestCase):
    def test_brief_keeps_requirement_lines(self):
        d = ("About us: we love culture and snacks. Requirements: Master's degree and 2+ years of experience in analytics. "
             "Python and SQL required. We will sponsor H-1B visas. Pay range $120,000 - $150,000. Our office has a gym.")
        b = jp.brief_text(d)
        self.assertIn("2+ years", b)
        self.assertIn("sponsor", b)
        self.assertNotIn("gym", b)

    def test_merge_scores_by_fp(self):
        sl = [{"fp": "a", "title": "x"}, {"fp": "b", "title": "y"}]
        merged, missing = jp.merge_scores(sl, [{"fp": "a", "score": {"skills": 20}}, {"fp": "zz", "drop": "reposted"}])
        self.assertEqual([m["fp"] for m in merged], ["a"])
        self.assertEqual(missing, ["zz"])

    def test_attach_regates_on_full_text(self):
        with tempfile.TemporaryDirectory() as d:
            sl = os.path.join(d, "sl.json")
            j = jp.screen_one(J(description="", source="LinkedIn", url="https://www.linkedin.com/jobs/view/4483812345/", apply_url=""), 30)
            jp.dump_json([j], sl)
            txt = os.path.join(d, "p.txt")
            open(txt, "w").write(DESC_OK + " Candidates must be authorized to work in the U.S. without sponsorship.")
            jp.main(["attach", sl, "--fp", j["fp"], "--text", txt, "--apply-url", "https://boards.greenhouse.io/acme/jobs/123456", "--posted", "Yesterday"])
            out = jp.load_json(sl)[0]
            self.assertEqual(out["screen"], "excluded")
            self.assertIn("no sponsorship", out["exclude_reasons"])


class Registry(unittest.TestCase):
    def test_backfill_never_overwrites(self):
        rows = [{"company": "Figure", "ats": "greenhouse", "ats_slug": "figure", "status": "ok", "notes": ""},
                {"company": "Truist", "ats": "workday", "ats_slug": "", "workday_url": "", "status": "unverified", "notes": ""}]
        for r in rows:
            for k in jp.CSV_FIELDS:
                r.setdefault(k, "")
        links = [("Figure", "https://job-boards.greenhouse.io/figureai/jobs/123"),
                 ("Truist", "https://truist.wd1.myworkdayjobs.com/Careers/job/Atlanta/X_R1"),
                 ("Truist", "https://truist.wd1.myworkdayjobs.com/Careers/job/Atlanta/Y_R2")]
        jp.do_backfill(links, rows, write=False)
        self.assertEqual(rows[0]["ats_slug"], "figure")
        self.assertIn("observed: greenhouse/figureai", rows[0]["notes"])
        self.assertEqual(rows[1]["workday_url"], "https://truist.wd1.myworkdayjobs.com/Careers")
        self.assertEqual(rows[1]["status"], "check")

    def test_h1b_csv(self):
        csv_text = "Fiscal Year,Employer (Petitioner) Name,Initial Approval,Continuing Approval\n2025,CAPITAL ONE SERVICES LLC,300,500\n2024,CAPITAL ONE SERVICES LLC,10,10\n"
        db = jp.parse_h1b_csv(csv_text)
        self.assertEqual(db["capital one"]["approvals"], 800)
        self.assertEqual(db["capital one"]["fy"], "2025")


def make_xlsx(path, rows):
    """Minimal .xlsx (shared strings) without openpyxl, to exercise the stdlib reader."""
    import zipfile
    strings, idx = [], {}

    def si(v):
        if v not in idx:
            idx[v] = len(strings)
            strings.append(v)
        return idx[v]
    cols = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    xml_rows = []
    for ri, row in enumerate(rows, 1):
        cells = "".join(f'<c r="{cols[ci]}{ri}" t="s"><v>{si(str(v))}</v></c>' for ci, v in enumerate(row))
        xml_rows.append(f'<row r="{ri}">{cells}</row>')
    ns = 'xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"'
    with zipfile.ZipFile(path, "w") as z:
        z.writestr("xl/worksheets/sheet1.xml", f'<worksheet {ns}><sheetData>{"".join(xml_rows)}</sheetData></worksheet>')
        z.writestr("xl/sharedStrings.xml", f'<sst {ns}>' + "".join(f"<si><t>{s}</t></si>" for s in strings) + "</sst>")


class Sponsors(unittest.TestCase):
    HEAD = ["CASE_STATUS", "VISA_CLASS", "JOB_TITLE", "SOC_CODE", "FULL_TIME_POSITION", "EMPLOYER_NAME", "WORKSITE_STATE",
            "WAGE_RATE_OF_PAY_FROM", "WAGE_UNIT_OF_PAY", "H-1B_DEPENDENT", "NAICS_CODE"]

    def test_build_and_grow(self):
        rows = [self.HEAD]
        rows += [["Certified", "H-1B", "Data Scientist", "15-2051.00", "Y", "ACME LENDING LLC", "GA", "125000", "Year", "N", "522291"]] * 12
        rows += [["Certified", "H-1B", "Software Engineer", "15-1252.00", "Y", "BIGOUTSOURCE INC", "NJ", "70000", "Year", "Y", "541511"]] * 20
        rows += [["Denied", "H-1B", "Data Scientist", "15-2051.00", "Y", "NOPE CORP", "NY", "1", "Year", "N", "5415"]] * 5
        rows += [["Certified", "H-1B", "Nurse", "29-1141.00", "Y", "HOSPITAL X", "GA", "1", "Year", "N", "6221"]] * 5
        with tempfile.TemporaryDirectory() as d:
            p = os.path.join(d, "LCA_FY2026_Q4.xlsx")
            make_xlsx(p, rows)
            sp = jp.build_sponsors([p])
        self.assertEqual(set(sp), {jp.norm_company("ACME LENDING LLC"), jp.norm_company("BIGOUTSOURCE INC")})
        acme = sp[jp.norm_company("ACME LENDING LLC")]
        self.assertEqual((acme["total"], acme["groups"]["data_science"], acme["fy"], acme["wage_median"]), (12, 12, "2026", 125000))
        cands = jp.grow_candidates(sp, [])
        self.assertEqual([c[1] for c in cands], [jp.norm_company("ACME LENDING LLC")])  # H-1B-dependent outsourcer left out
        added, pending = jp.do_registry_grow(sp, [], probe_boards=False)
        self.assertEqual((len(added), len(pending)), (0, 1))  # no board found -> candidate, not a daily career page
        self.assertEqual(pending[0]["source"], "dol")
        self.assertEqual(pending[0]["sector"], "fintech")  # NAICS 5222 non-bank lending

    def test_discovered_company_additions(self):
        rows = [{"company": "Acme Pay"}]
        new = [{"company": "Valon", "apply_link": "https://jobs.ashbyhq.com/valon/0a1b2c3d-1111-2222-3333-444455556666", "source": "Indeed",
                "title": "Data Scientist", "score": 82, "location": "New York, NY"},
               {"company": "Acme Pay", "apply_link": "https://x/1"}]
        adds = jp.registry_additions_from_rows(new, rows)
        self.assertEqual(len(adds), 1)
        self.assertEqual((adds[0]["ats"], adds[0]["ats_slug"], adds[0]["source"]), ("ashby", "valon", "discovered"))

    def test_sponsor_evidence_in_score(self):
        jp._SPONSORS = {"acme pay": {"name": "ACME PAY", "total": 40, "groups": {"data_science": 30, "software": 10}, "fy": "2026",
                                     "titles": [], "states": [], "wage_median": 1, "dependent_share": 0, "naics": ""}}
        try:
            s = jp.screen_one(J(description=DESC_OK.replace("We sponsor H-1B visas.", "")), 30)
            self.assertEqual(s["sponsorship_level"], "h1b_10")
            self.assertIn("DOL LCA FY2026: 40", s["h1b_history"])
        finally:
            jp._SPONSORS = None


class TrackerSync(unittest.TestCase):
    def test_docs_are_create_only_and_never_carry_her_fields(self):
        rows = [{"id": "a1", "company": "Acme", "title": "DS", "score": 80, "status": "Not applied", "my_note": "x", "referral_status": "To find"},
                {"id": "a2", "company": "Beta", "title": "DS", "score": 70}, {"id": "a2", "company": "Beta", "title": "DS", "score": 70}]
        with tempfile.TemporaryDirectory() as d:
            batches = jp.tracker_docs(rows, {"a2"}, d)
            ws = [w for b in batches for w in b]
            self.assertEqual([(w["op"], w["doc_id"]) for w in ws], [("set", "a1")])
            doc = json.load(open(ws[0]["file_path"]))
            self.assertEqual(doc["status"], "Not applied")
            self.assertNotIn("my_note", doc)
            self.assertNotIn("referral_status", doc)
            self.assertIn("added_at", doc)

    def test_export_reads_flat_out_dir_files(self):
        with tempfile.TemporaryDirectory() as d:
            os.makedirs(os.path.join(d, "roles"))
            json.dump({"title": "DS", "company": "Acme", "status": "Applied"}, open(os.path.join(d, "roles", "r-1.json"), "w"))
            rows = jp.tracker_export(d)
            self.assertEqual((rows[0]["_id"], rows[0]["status"]), ("r-1", "Applied"))

    def test_referral_packs_one_per_company_create_only(self):
        rows = [{"_id": "1", "company": "JPMorgan Chase", "title": "Quant Analyst", "score": 82, "priority_group": "High", "ev_skills": "Python; SQL"},
                {"_id": "2", "company": "JPMorgan Chase & Co.", "title": "Risk DS", "score": 77, "priority_group": "High"},
                {"_id": "3", "company": "Acme", "title": "DS", "score": 79, "priority_group": "High", "status": "Applied"},
                {"_id": "4", "company": "Beta", "title": "DS", "score": 64, "priority_group": "Medium"},
                {"_id": "5", "company": "Gamma", "title": "DS", "score": 90, "priority_group": "High", "referral_status": "Referred"},
                {"_id": "6", "company": "Delta", "title": "DS", "score": 76, "priority_group": "High"}]
        packs = jp.do_referrals(rows, existing_packs={jp.pack_id("Delta")})
        self.assertEqual(len(packs), 1)
        p = packs[0]
        self.assertEqual(p["top_title"], "Quant Analyst")
        self.assertEqual(set(p["companies"]), {"JPMorgan Chase", "JPMorgan Chase & Co."})
        self.assertIn("Quant Analyst", p["draft"])
        self.assertTrue(any("1point3acres" in l["url"] for l in p["links"]))
        self.assertRegex(p["_id"], r"^[A-Za-z0-9_\-.~:@+]+$")


class RunState(unittest.TestCase):
    def test_pack_unpack_round_trip_and_repost_detection(self):
        old = (jp.NOW - dt.timedelta(days=5)).strftime("%Y-%m-%dT%H:%M")
        ancient = (jp.NOW - dt.timedelta(days=90)).strftime("%Y-%m-%dT%H:%M")
        with tempfile.TemporaryDirectory() as d:
            os.makedirs(os.path.join(d, "runstate"))
            with open(os.path.join(d, "runstate", "run-1.json"), "w") as f:
                json.dump({"run_at": old, "seen": {"fpA": old, "fpOld": ancient}}, f)
            st = jp.state_unpack(d)
            self.assertEqual(set(st["seen"]), {"fpA"})  # older than 60 days dropped
            self.assertGreater(jp.age_hours(st["seen"]["fpA"]), 100)  # minute-precision UTC parses
            since = jp.NOW.isoformat()
            st["seen"]["fpB"] = jp.NOW.isoformat()
            did, doc, n = jp.state_pack(st, since)
            self.assertEqual((n, list(doc["seen"])), (1, ["fpB"]))
            self.assertTrue(did.startswith("run-"))


class Calibrate(unittest.TestCase):
    def test_auc(self):
        rows = [{"status": "Applied", "score": 80}, {"status": "Skipped", "score": 60, "notes": "Skipped: reposted listing."},
                {"status": "Applied", "score": 70}, {"status": "Skipped", "score": 75, "notes": "too senior"}]
        c = jp.do_calibrate(rows)
        self.assertEqual(c["auc_score_vs_applied"], 0.75)
        self.assertEqual(c["skip_reasons"]["reposted"], 1)


if __name__ == "__main__":
    unittest.main()
