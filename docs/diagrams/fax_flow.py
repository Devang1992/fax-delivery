from svgkit import Diagram


def build():
    d = Diagram(2720, 1560, "FAX delivery — 3 topics · RightFax · circuit breaker pauses every consumer",
                "Spring Boot 4 · Spring Kafka transactions (publish next task + commit offset atomically) · no DLQ: "
                "terminal ERROR lines carry MID + origin offset + all original headers")

    # ------------------------------------------------------------------ source side
    d.cylinder("src", 40, 110, 170, 92, "source topic\nshared with EMAIL\nkey/value: String", "kafka")
    d.box("srcl", 245, 105, 215, 102, "Source listener\ngroup fax-delivery-source\nmax.poll.records 1\none Kafka tx per record",
          "process", size=12)
    d.diamond("pref", 585, 156, 175, 110, "PREFERENCE\n= FAX?")
    d.diamond("gate", 790, 156, 165, 110, "Breaker\nOPEN?")
    d.arrow([d.nodes["src"]["r"], d.nodes["srcl"]["l"]])
    d.arrow([d.nodes["srcl"]["r"], (497, 156)])
    d.arrow([(672, 156), (707, 156)], "yes", lpos=(690, 146))
    d.pill("filtered", 495, 262, 180, 66, "FILTERED (DEBUG)\ncounter · committed", "log", size=12)
    d.arrow([(585, 211), d.nodes["filtered"]["t"]], "no", lpos=(600, 240), lanchor="start")
    d.box("hold", 700, 262, 185, 92, "HOLD\nrollback · offset kept\nre-read after HALF_OPEN", "breaker", size=12)
    d.arrow([(790, 211), d.nodes["hold"]["t"]], "yes", kind="breaker", lpos=(805, 240), lanchor="start")

    # ------------------------------------------------------------------ SUBMIT group
    d.group(925, 78, 965, 560, "SUBMIT step (SubmitFlow) — first attempt and every retry run the same code",
            color="#6aa9f0", title_color="#a9cdf7")
    d.arrow([(872, 156), (938, 156)], "no", lpos=(905, 146))
    d.diamond("dl", 1022, 156, 165, 110, "now ≥ submit\ndeadline?", size=11.5)
    d.pill("expired", 940, 262, 170, 66, "SUBMIT_EXPIRED\n(ERROR) nothing sent", "error", size=11.5)
    d.arrow([(1022, 211), d.nodes["expired"]["t"]], "yes", kind="error", lpos=(1037, 240), lanchor="start")
    d.box("map", 1130, 108, 175, 96, "Map + validate\nJSON pointers\nrender GPP.txt\nPDFBox → bytes", "process", size=11.5)
    d.arrow([(1105, 156), d.nodes["map"]["l"]], "no", lpos=(1117, 146))
    d.pill("invalid", 1130, 262, 175, 66, "INVALID_MESSAGE /\nFAILED_PERMANENT (ERROR)", "error", size=11)
    d.arrow([d.nodes["map"]["b"], d.nodes["invalid"]["t"]], kind="error")
    d.box("calls", 1330, 96, 255, 120, "Calls 1-3, back to back\n[login only without a session]\nPOST /Attachments → temp URL\n"
                                       "POST /SendJobs → jobId\neach call: breaker + classifier", "external", size=11.5)
    d.arrow([d.nodes["map"]["r"], d.nodes["calls"]["l"]])
    d.diamond("res", 1700, 156, 160, 110, "result?")
    d.arrow([d.nodes["calls"]["r"], (1620, 156)])
    d.pill("unconf", 1590, 250, 285, 60, "201 without jobId → SUBMISSION_UNCONFIRMED\n(ERROR) never retried: would duplicate",
           "error", size=11)
    d.pill("perm", 1590, 322, 285, 56, "4xx → FAILED_PERMANENT (ERROR)", "error", size=11.5)
    d.box("rej", 1590, 390, 285, 56, "breaker rejected → HOLD\n(same as above, nothing requeued)", "breaker", size=11)
    d.box("trans", 1590, 458, 285, 112, "transient · ambiguous · systemic\nunexpected (≤ 3 per message)\n"
                                        "→ fax.retry: attempt+1, notBefore now+60s\nWARN RETRY_SCHEDULED /\n"
                                        "POSSIBLE_DUPLICATE (ambiguous)", "retry", size=11)
    d.arrow([(1700, 211), (1700, 250)], kind="error")
    d.note(945, 585, ["Also from result: a retry that would land past the deadline → SUBMIT_EXPIRED;",
                      "HTTP 500 with max-retries-on-server-error reached → FAILED_EXHAUSTED."], size=11.5,
           color="#9aa3b2")

    # ------------------------------------------------------------------ retry topic + listener
    d.cylinder("retry", 1110, 680, 320, 96, "fax.retry  (fixed delay 60s)\nkey MID · value FaxTask JSON\n"
                                            "headers PREFERENCE, MID, NOT-BEFORE, STAGE", "kafka", size=11.5)
    d.arrow([d.nodes["trans"]["b"], (1732, 728), d.nodes["retry"]["r"]], "publish (in tx)", kind="retry",
            lpos=(1600, 718))
    d.box("retl", 1110, 820, 320, 104, "Retry listener  (group fax-delivery-retry)\n1 filter PREFERENCE first\n"
                                       "2 decode · 3 wait until due (≤ 60s+10s)\n4 breaker gate · 5 SUBMIT step",
          "process", size=11.5)
    d.arrow([d.nodes["retry"]["b"], d.nodes["retl"]["t"]])
    d.arrow([d.nodes["retl"]["l"], (900, 872), (900, 182), (946, 182)], "re-run SUBMIT", kind="retry",
            lpos=(905, 470), lanchor="start")

    # ------------------------------------------------------------------ poll topic + listener
    d.cylinder("poll", 1975, 104, 320, 96, "fax.poll  (fixed delay 30s)\nPOLL_JOB / POLL_DOCS tasks\njobId · pollDeadlineAt stamped",
               "kafka", size=11.5)
    d.arrow([(1780, 156), d.nodes["poll"]["l"]], "jobId", kind="ok", lpos=(1878, 146))
    d.box("polll", 1975, 238, 320, 92, "Poll listener  (group fax-delivery-poll)\nfilter · decode · wait until due\n"
                                       "breaker gate · POLL step", "process", size=11.5)
    d.arrow([d.nodes["poll"]["b"], d.nodes["polll"]["t"]])

    d.group(1935, 365, 760, 860, "POLL step (PollFlow) — one record = ONE check of ONE job, never a wait",
            color="#8e7cc3", title_color="#c9bdf0")
    d.arrow([d.nodes["polll"]["b"], (2135, 405)])
    d.box("gj", 2005, 408, 260, 66, "GET /SendJobs/{jobId}\nsession + breaker", "external", size=12)
    d.pill("scf", 2310, 408, 365, 66, "4xx / 404 → STATUS_CHECK_FAILED (ERROR)\njob unknown to RightFax", "error",
           size=11.5)
    d.arrow([d.nodes["gj"]["r"], d.nodes["scf"]["l"]], kind="error")
    d.diamond("cond", 2135, 560, 175, 100, "condition?")
    d.arrow([d.nodes["gj"]["b"], (2135, 510)])
    d.pill("ff1", 2310, 525, 365, 70, "Failed / Canceled → FAILED_FINAL_STATUS\n(ERROR) with status text", "error",
           size=11.5)
    d.arrow([(2222, 560), d.nodes["ff1"]["l"]], kind="error")
    d.box("gd", 2005, 650, 260, 66, "GET /Documents?filter=job&jobid=…\nsame step, no extra wait", "external",
          size=11.5)
    d.arrow([(2135, 610), d.nodes["gd"]["t"]], "Succeeded", kind="ok", lpos=(2150, 632), lanchor="start")
    d.diamond("docs", 2135, 800, 175, 100, "documents?")
    d.arrow([d.nodes["gd"]["b"], (2135, 750)])
    d.pill("deliv", 2310, 740, 365, 62, "all Succeeded → DELIVERED (INFO)\nnothing published ✔", "log", size=11.5)
    d.arrow([(2222, 790), (2265, 790), (2265, 771), d.nodes["deliv"]["l"]], kind="ok")
    d.pill("ff2", 2310, 816, 365, 62, "any Failed → FAILED_FINAL_STATUS (ERROR)\nper-document status text", "error",
           size=11.5)
    d.arrow([(2222, 810), (2280, 810), (2280, 847), d.nodes["ff2"]["l"]], kind="error")
    d.diamond("nf", 2135, 990, 195, 110, "now ≥ poll\ndeadline?", size=11.5)
    d.arrow([(2135, 850), d.nodes["nf"]["t"]], "pending", kind="retry", lpos=(2150, 900), lanchor="start")
    d.arrow([(2048, 560), (1975, 560), (1975, 990), (2037, 990)], "pending /\ntransient error", kind="retry",
            lpos=(1982, 760), lanchor="start")
    d.pill("to", 2310, 955, 365, 70, "FINAL_TIMEOUT (ERROR)\n_UNREACHABLE when the last call failed", "error",
           size=11.5)
    d.arrow([(2233, 990), d.nodes["to"]["l"]], "yes", kind="error", lpos=(2270, 980))
    d.box("rq", 2005, 1100, 260, 74, "fax.poll {pollCount+1,\nnotBefore now+30s}", "retry", size=12)
    d.arrow([(2135, 1045), d.nodes["rq"]["t"]], "no", kind="retry", lpos=(2150, 1072), lanchor="start")
    d.arrow([d.nodes["rq"]["l"], (1950, 1137), (1950, 128), d.nodes["poll"]["l"][0:1] + (128,)], "requeue (in tx)",
            kind="retry", lpos=(1955, 1010), lanchor="start")
    d.note(2310, 1110, ["breaker rejected → HOLD", "unexpected (≤ 3) → treated as transient",
                        "deadline checked AFTER the call: the last", "check before giving up is always real"],
           size=11.5)

    # ------------------------------------------------------------------ breaker panel
    d.group(30, 960, 1035, 520, "Circuit breaker 'rightfax' — one breaker, all RightFax calls, drives ALL consumers",
            color="#e8913f", title_color="#f2b67c")
    d.box("closed", 70, 1010, 170, 56, "CLOSED\ncalls flow", "process")
    d.note(265, 1012, ["**Counts:** transient · 500 · ambiguous · slow (> 20s) · login 401 · 403",
                       "**Ignored:** permanent (bad data) · first 401 of an expired session",
                       "**Window:** last 10 calls, ≥ 50% failed or ≥ 80% slow, min 5"], size=11.5)
    d.box("open", 70, 1130, 170, 56, "OPEN", "breaker")
    d.arrow([d.nodes["closed"]["b"], d.nodes["open"]["t"]], "threshold hit", kind="breaker", lpos=(165, 1103),
            lanchor="start")
    d.box("pause", 300, 1112, 450, 92, "pause() fax-source · fax-retry · fax-poll\npoll() keeps heartbeating → no rebalance\n"
                                       "the record that hit it was HELD (rolled back)\nERROR CONSUMER_PAUSED",
          "breaker", size=11.5)
    d.arrow([d.nodes["open"]["r"], d.nodes["pause"]["l"]], kind="breaker")
    d.box("half", 70, 1270, 170, 56, "HALF_OPEN", "breaker")
    d.arrow([d.nodes["open"]["b"], d.nodes["half"]["t"]], "60s · automatic", kind="breaker", lpos=(165, 1238),
            lanchor="start")
    d.box("resume", 300, 1252, 450, 92, "resume() all three containers\nheld records are read again\n"
                                        "first RightFax call = probe (1 permitted)\nothers rejected → HOLD, 1s back-off",
          "breaker", size=11.5)
    d.arrow([d.nodes["half"]["r"], d.nodes["resume"]["l"]], kind="breaker")
    d.arrow([(70, 1298), (50, 1298), (50, 1038), (70, 1038)], "probe ok", kind="ok", lpos=(56, 1242),
            lanchor="start")
    d.arrow([(240, 1290), (275, 1290), (275, 1158), (240, 1158)], "probe\nfailed", kind="breaker",
            lpos=(282, 1225), lanchor="start")
    d.note(780, 1125, ["> 6 opens / hour →", "CIRCUIT_FLAPPING (ERROR)", "", "metric:", "resilience4j_circuitbreaker_",
                       "state{name=rightfax}"], size=11.5)
    d.note(70, 1385, ["Nothing is requeued because of the breaker: records wait where they are (source, retry or poll",
                      "topic). Deadlines keep running; a record that outlives its deadline ends as SUBMIT_EXPIRED or",
                      "FINAL_TIMEOUT_UNREACHABLE when it is read again."], size=11.5)

    # ------------------------------------------------------------------ guarantees panel
    d.group(1090, 960, 820, 520, "Guarantees & mechanics")
    d.note(1110, 1000, [
        "**One Kafka transaction per record:** publish ≤ 1 next task + commit the offset, atomically.",
        "A crash mid-step re-runs the step; only an ambiguous/crashed SendJob can duplicate a fax.",
        "",
        "**HOLD:** throw → rollback → after-rollback processor seeks back (1s fixed back-off) →",
        "container already paused → record read again after HALF_OPEN. Never skipped, never requeued.",
        "",
        "**Due time:** fixed delay per topic → records arrive in due order → waiting on the consumer",
        "thread until notBefore is exact (≤ delay + 10s slack); wakes up on container stop.",
        "",
        "**Deadlines stamped on the task:** submit = firstSeenAt + 4h, poll = submittedAt + 2h.",
        "",
        "**Filter first:** PREFERENCE checked before parsing or waiting — another flow sharing",
        "fax.retry can never delay or break fax records (and must do the same for its own).",
        "",
        "**Startup invariants:** https only · poll-interval ≥ wait + records × worst step ·",
        "tx timeout ≥ worst step · max-age ≥ 2 × delay · hold back-off < breaker wait.",
    ], size=12)

    d.legend(40, 1525)
    return d
