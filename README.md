# Transaction Monitoring Subsystem

A fraud detection system that analyzes transaction CSV files and flags suspicious activities using 5 rule-based detection algorithms.

The higher risk number refers to the system's confidence in this transaction being fraudulent. The higher the number, the more suspicious.

## Features

### 5 Fraud Detection Rules

1. **High-Velocity Transaction Burst** (Risk: 80/100)
   - Detects: ≥5 transactions within 10-minute window
   - Prevents: Card testing, automated fraud bots

2. **Statistical Amount Anomaly** (Risk: 70/100)
   - Detects: Amount > (user_mean + 3×std_dev) AND > $500
   - Prevents: Unusually large fraudulent purchases

3. **Cumulative Spending Spike** (Risk: 75/100)
   - Detects: 24-hour sum > $5,000 OR > 10× user's daily average
   - Prevents: Account takeover spending sprees

4. **First-Time Merchant + High Amount** (Risk: 60/100)
   - Detects: New merchant AND amount > $300 AND > 2× user average
   - Prevents: Fraudsters using unfamiliar merchants

5. **Nocturnal High-Value Transaction** (Risk: 55/100)
   - Detects: Hour between 2am-6am AND amount > user's 75th percentile
   - Prevents: Automated fraud scripts, off-hours account takeover

## Usage

```bash
python3 main.py --input data/input/sample_transactions.csv --output data/output/flagged_transactions.csv
```

## Input Format

CSV file with columns:
- `user_id`: User identifier
- `timestamp`: Transaction timestamp (ISO format or epoch)
- `merchant_name`: Merchant name
- `amount`: Transaction amount (numeric)

## Output Format

CSV file with original columns plus:
- `risk_score`: Aggregate risk score (0-100)
- `triggered_rules`: Comma-separated list of triggered rule names
- `explanation`: Human-readable explanation of why flagged
