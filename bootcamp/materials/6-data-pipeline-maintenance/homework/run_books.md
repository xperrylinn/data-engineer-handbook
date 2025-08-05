# 📊 Data Engineering Pipeline Ownership Plan

### Team: Data Engineering Group (4 Members)
Members:  
- Xavi (you)  
- Priya  
- Andre  
- Mei  

---

## 🛠️ Pipeline Ownership

| Business Area | Pipeline Purpose                        | Primary Owner | Secondary Owner |
|---------------|-----------------------------------------|----------------|------------------|
| Profit        | Unit-level profit (experiments)         | Mei            | Xavi             |
| Profit        | Aggregate profit (investor reporting)   | Xavi           | Andre            |
| Growth        | Aggregate growth (investor reporting)   | Andre          | Priya            |
| Growth        | Daily growth (experiments)              | Priya          | Mei              |
| Engagement    | Aggregate engagement (investor reporting) | Xavi        | Mei              |

---

## ⏰ On-Call Schedule

| Week         | On-Call Engineer |
|--------------|------------------|
| Week 1       | Xavi             |
| Week 2       | Priya            |
| Week 3       | Andre            |
| Week 4       | Mei              |
| Week 5       | Xavi             |
| Week 6       | Priya            |
| ...          | (Rotates)        |

### ⚠️ On-Call Notes:
- Swaps allowed with 48 hours notice.
- If on-call week overlaps with a holiday, next person in rotation picks up the shift OR we pre-plan coverage.
- PagerDuty alerts routed to on-call Slack channel + SMS fallback.

---

## 📚 Runbooks for Investor-Facing Pipelines

### 1. **Aggregate Profit Reporting**

- **Schedule:** Daily @ 6AM UTC
- **Source:** Internal sales DB, external transaction API
- **Steps:**
  - Pull from DB/API
  - Join and clean records
  - Aggregate by SKU, region
  - Load to investor metrics dashboard (Redash)
- **Validation:** Check for 0 rows, nulls in `profit`, compare to previous day
- **Alerting:** Fails if rows < 80% of previous day OR API error

---

### 2. **Aggregate Growth Reporting**

- **Schedule:** Daily @ 5AM UTC
- **Source:** Customer signup & activation logs
- **Steps:**
  - Ingest from Kafka
  - Windowed aggregations by day
  - Load to data warehouse
- **Validation:** Check daily row count, 7-day moving average
- **Alerting:** Slack + PagerDuty if daily count drops > 20% w/o reason

---

### 3. **Aggregate Engagement Reporting**

- **Schedule:** Every 6 hours
- **Source:** User interaction logs
- **Steps:**
  - Sessionize web events
  - Calculate engagement score (clicks + time)
  - Store in Postgres, visualize in Metabase
- **Validation:** Ensure engagement score in range [0, 100]
- **Alerting:** If score distribution skews unusually or flatlines

---

## 💥 Potential Failure Modes

| Pipeline                        | Potential Issues |
|---------------------------------|------------------|
| Unit-level Profit               | Delayed or missing experiment tagging, bad SKU joins |
| Aggregate Profit                | External API timeout, schema drift, currency rate errors |
| Aggregate Growth                | Missed Kafka events, bad timestamp formatting |
| Daily Growth                    | Low data volume, incorrect user activation logic |
| Engagement Reporting            | Session window failure, misparsed event times, bot traffic spikes |

---

## ✅ Summary

- Each pipeline has primary/secondary ownership for redundancy.
- On-call is rotated weekly, with holiday flexibility.
- Investor-facing pipelines have written runbooks and alerting.
- Risks are documented but not actioned (per assignment).
