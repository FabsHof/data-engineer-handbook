# Data Pipeline Management Plan

## Overview
This document outlines the management structure, on-call schedules, and runbooks for the data pipelines managed by a team of 4 data engineers. These pipelines cover critical business areas such as Profit, Growth, and Engagement, with aggregate reports provided to investors and experiment teams.

---

## Pipeline Ownership

| Pipeline                        | Primary Owner                 | Secondary Owner         |
|---------------------------------|-------------------------------|-------------------------|
| **Profit**                      | Finance/Risk Team             | Data Engineering Team   |
| **Unit-level Profit (experiments)** | Finance/Risk Team             | Data Engineering Team   |
| **Aggregate Profit (investors)**| Business Analytics Team       | Data Engineering Team   |
| **Growth**                      | Accounts Team                 | Data Engineering Team   |
| **Daily Growth (experiments)**  | Data Science Team             | Data Engineering Team   |
| **Aggregate Growth (investors)**| Business Analytics Team       | Data Engineering Team   |
| **Engagement**                  | Software Frontend Team        | Data Engineering Team   |
| **Aggregate Engagement (investors)**| Business Analytics Team   | Data Engineering Team   |

---

## On-Call Schedule

- **Rotation Frequency**: Weekly
- **Holiday Coverage**: Rotate with even distribution to account for public holidays. If a holiday falls within an engineer's rotation, they are compensated with a day off during their next rotation.
- **Schedule**:
  - Week 1: Engineer A
  - Week 2: Engineer B
  - Week 3: Engineer C
  - Week 4: Engineer D

---

## Runbooks for Investor-Related Pipelines

### 1. Profit
- **Data Types**: Revenue, expenses, aggregated salaries.
- **Common Issues**:
  - Discrepancy in numbers across accounts and filings.
  - Numbers requiring external accountant verification.
- **SLA**: Monthly review by account teams.
- **On-Call**: Weekly rotation monitored by BI on the profit team.

---

### 2. Growth
- **Data Types**: License changes, subscription updates.
- **Common Issues**:
  - Missing time-series data due to incomplete steps.
- **SLA**: Updated account statuses by end of week.
- **On-Call**: Debugging during business hours.

---

### 3. Engagement
- **Data Types**: User clicks, time spent metrics.
- **Common Issues**:
  - Late data arrival in Kafka queues.
  - Kafka downtime causing data loss.
  - Duplicate events requiring deduplication.
- **SLA**: Data to be available within 48 hours; issues resolved within one week.
- **On-Call**: Weekly rotation with a 30-minute onboarding session.

---

### 4. Aggregate Data for Executives and Investors
- **Data Types**: Merged data from Profit, Growth, and Engagement pipelines.
- **Common Issues**:
  - Spark joins failing due to large data volumes (OOM errors).
  - Stale data causing queue backfill delays.
  - Missing data leading to NA or divide-by-zero errors.
- **SLA**: Issues resolved by the end of the month before executive/investor reports.
- **On-Call**: DEs monitor pipelines during the last week of the month.

---

## Notes
- Experiment-related pipelines (unit-level profit and daily growth) are used by the Data Science team for A/B testing and other analyses. These pipelines have a looser SLA as they are not directly tied to investor reporting.
- The above on-call rotation ensures fairness while accounting for holidays, providing sufficient coverage for all critical pipelines.