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

## Installation
```zsh
pip3 install -r requirements.txt
```

## Usage


```zsh
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

## Example Output

Here's what the system detects from real transaction data:

```csv
user_id,timestamp,merchant_name,amount,risk_score,triggered_rules,explanation
user005,2024-01-06 04:20:00,luxury_watches_intl,3500.0,54.41,"Rule2:AmountAnomaly,Rule4:NewMerchant,Rule5:Nocturnal",Amount exceeds user pattern (>3 std dev); First-time merchant with high amount; High-value transaction during 2am-6am
user001,2024-01-06 10:14:00,unknown_merchant5,395.0,41.18,"Rule1:Velocity,Rule4:NewMerchant",Multiple transactions in 10 minutes; First-time merchant with high amount
user004,2024-01-06 14:00:00,apple_store,999.0,38.24,"Rule2:AmountAnomaly,Rule4:NewMerchant",Amount exceeds user pattern (>3 std dev); First-time merchant with high amount
user003,2024-01-06 02:45:00,international_electronics,1250.0,33.82,"Rule4:NewMerchant,Rule5:Nocturnal",First-time merchant with high amount; High-value transaction during 2am-6am
```

**Key Detection Examples:**
- **Highest Risk (54.41)**: User005 made a $3,500 purchase at 4:20 AM from a new luxury watch merchant - triggering 3 rules
- **Velocity Attack (41.18)**: User001 had multiple rapid transactions within 10 minutes at unknown merchants
- **Nocturnal Fraud (33.82)**: Multiple users making high-value purchases between 2-6 AM from new merchants


## Future Enhancements

### 1. Machine Learning Integration
- Train ensemble models (Random Forest, XGBoost, Neural Networks) on labeled transaction data
- Combine rule-based + ML predictions for hybrid fraud detection
- SHAP/LIME explainability for model transparency
- Active learning pipeline with human-in-the-loop feedback

### 2. Real-Time Processing & API
- REST API for live transaction scoring
- Stream processing with Kafka/Redis for sub-100ms latency
- WebSocket alerts for immediate fraud notifications
- Interactive dashboard with real-time metrics and visualizations

### 3. Advanced Behavioral Analytics
- Graph analysis to detect fraud rings and money mule networks
- Impossible travel detection (geographic velocity checks)
- Device fingerprinting and IP reputation scoring
- Merchant category risk modeling and peer group analysis