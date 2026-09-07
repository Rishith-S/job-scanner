"""JD entry-level binary classifier v0 — pure stdlib, Actions-ready.
Usage: python3 classify.py "paste desc here" --title "Software Engineer"
   or: echo "$DESC" | python3 classify.py --title "..."
Output: ENTRY (0.9) + reason  |  NOT (0.9) + reason
No deps, no model file, no API key.
"""
import html
import re, sys, argparse

ENTRY_SIG = re.compile(
    r"new grad|recent grad|entry.?level|early career|university grad|"
    r"0\s*[-–]\s*2\s*(years?|yrs?)|graduating (dec|may|june|fall|spring|winter)?\s*20\d\d|"
    r"intern conversion|no experience required|students? (are )?encouraged",
    re.I)

SENIOR_TITLE = re.compile(r"\b(senior|staff|principal|lead|manager|architect|distinguished)\b", re.I)

# noise to ignore around a year-number: benefits, pay, hours
NOISE = re.compile(r"401|pto|vacation|benefit|salary|\$|hour|week|day off|holiday", re.I)
# signal that the year-number is about required experience
EXP_CTX = re.compile(r"experi|minimum|required|at least|years? (with|of|in|building)|"
                     r"professional|industry|selling|software development|engineering", re.I)

def min_yoe(desc: str):
    """Return (min_years or None, evidence snippet). Skips benefit noise."""
    best, ev = None, ""
    for m in re.finditer(r"(\d+)\s*\+?\s*(?:-|to\s)?\s*(?:\d+\s*)?(years?|yrs?)\b", desc, re.I):
        n = int(m.group(1))
        if n > 20:  # 401k fragments, "20+ PTO days" etc
            continue
        ctx = desc[max(0, m.start()-60):m.end()+60]
        if NOISE.search(ctx):
            continue
        if n >= 2 and EXP_CTX.search(ctx):
            snip = re.sub(r"\s+", " ", ctx).strip()[:140]
            if best is None or n > best:
                best, ev = n, snip
        elif n >= 5 and not NOISE.search(ctx):
            # bare "5 years building X" without the word experience — still counts
            snip = re.sub(r"\s+", " ", ctx).strip()[:140]
            if best is None or n > best:
                best, ev = n, snip
    return best, ev

def classify(title: str, desc: str):
    t = html.unescape(title or "")
    d = html.unescape(desc or "")
    entry_hit = ENTRY_SIG.search(t + " " + d[:2000])
    senior_hit = SENIOR_TITLE.search(t)
    yoe, ev = min_yoe(d)

    # hard NOTs first — desc ground truth beats title marketing
    if yoe is not None and yoe >= 3:
        return ("NOT", 0.9, f"requires {yoe}y — …{ev}")
    if senior_hit and yoe is not None and yoe >= 2:
        return ("NOT", 0.85, f"senior title + {yoe}y — …{ev}")
    if senior_hit and re.search(r"\b(5|6|7|8|9|10)\+?\s*(years?|yrs?)", d, re.I):
        return ("NOT", 0.8, "senior title + 5y+ mention")
    if entry_hit and (yoe is None or yoe < 3):
        m = re.sub(r"\s+", " ", entry_hit.group(0)).strip()
        return ("ENTRY", 0.9, f"entry signal '{m}'" + (f", max {yoe}y seen" if yoe else ", no yoe demand"))
    if yoe is not None and yoe <= 2:
        return ("ENTRY", 0.8, f"only {yoe}y asked — …{ev}")
    if entry_hit:
        return ("ENTRY", 0.7, "entry signal in title/desc, desc has no 3y+ demand")
    return ("NOT", 0.6, "no entry signal, default NOT (safe side for your search)")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("desc", nargs="?", default="")
    ap.add_argument("--title", default="")
    a = ap.parse_args()
    desc = a.desc or sys.stdin.read()
    label, conf, why = classify(a.title, desc)
    print(f"{label} ({conf}) — {why}")

if __name__ == "__main__":
    main()
