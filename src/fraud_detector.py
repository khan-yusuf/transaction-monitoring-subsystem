"""
Fraud detection rule engine.
Implements 5 rule-based fraud detection algorithms.
"""

import pandas as pd
from typing import Dict


class FraudDetector:
    """Evaluates transactions against fraud detection rules."""

    def __init__(self, config: Dict):
        """
        Initialize fraud detector with configuration.

        Args:
            config: Dictionary with rule thresholds and weights
        """
        self.config = config

    def detect_fraud(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Evaluate all fraud rules and flag suspicious transactions.

        Args:
            df: DataFrame with engineered features

        Returns:
            DataFrame with fraud flags and risk scores
        """
        print("Running fraud detection rules...")

        df = df.copy()

        # Initialize fraud detection columns
        df['fraud_flag'] = 0
        df['risk_score'] = 0
        df['triggered_rules'] = ''
        df['explanation'] = ''

        # Apply each rule
        df = self._apply_rule1_velocity(df)
        df = self._apply_rule2_amount_anomaly(df)
        df = self._apply_rule3_spending_spike(df)
        df = self._apply_rule4_new_merchant(df)
        df = self._apply_rule5_nocturnal(df)

        # Aggregate results
        df = self._aggregate_scores(df)

        # Count flagged transactions
        flagged_count = df['fraud_flag'].sum()
        print(f"Flagged {flagged_count} suspicious transactions ({flagged_count/len(df)*100:.2f}%)")

        return df

    def _apply_rule1_velocity(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Rule 1: High-Velocity Transaction Burst
        Flag if ≥5 transactions within 10-minute window
        """
        threshold = self.config['rule1_velocity_threshold']

        df['rule1_triggered'] = (
            df['rolling_10min_count'] >= threshold
        ).astype(int)

        # Add to triggered rules
        mask = df['rule1_triggered'] == 1
        df.loc[mask, 'triggered_rules'] += 'Rule1:Velocity,'
        df.loc[mask, 'explanation'] += 'Multiple transactions in 10 minutes; '

        return df

    def _apply_rule2_amount_anomaly(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Rule 2: Statistical Amount Anomaly
        Flag if amount > (user_mean + 3×std_dev) AND > $500
        """
        sigma_multiplier = self.config['rule2_sigma_multiplier']
        min_amount = self.config['rule2_min_amount']

        # Calculate threshold per user
        df['rule2_threshold'] = df['user_mean'] + (sigma_multiplier * df['user_std'])

        df['rule2_triggered'] = (
            (df['amount'] > df['rule2_threshold']) &
            (df['amount'] > min_amount) &
            (df['user_count'] >= 5)  # Need at least 5 transactions for meaningful stats
        ).astype(int)

        # Add to triggered rules
        mask = df['rule2_triggered'] == 1
        df.loc[mask, 'triggered_rules'] += 'Rule2:AmountAnomaly,'
        df.loc[mask, 'explanation'] += 'Amount exceeds user pattern (>3 std dev); '

        return df

    def _apply_rule3_spending_spike(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Rule 3: Cumulative Spending Spike
        Flag if 24-hour sum > $5,000 OR > 10× user's daily average
        """
        absolute_threshold = self.config['rule3_absolute_threshold']
        relative_multiplier = self.config['rule3_relative_multiplier']

        df['rule3_relative_threshold'] = df['user_daily_avg'] * relative_multiplier

        df['rule3_triggered'] = (
            (df['rolling_24h_sum'] > absolute_threshold) |
            (df['rolling_24h_sum'] > df['rule3_relative_threshold'])
        ).astype(int)

        # Add to triggered rules
        mask = df['rule3_triggered'] == 1
        df.loc[mask, 'triggered_rules'] += 'Rule3:SpendingSpike,'
        df.loc[mask, 'explanation'] += 'High spending in 24-hour period; '

        return df

    def _apply_rule4_new_merchant(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Rule 4: First-Time Merchant + High Amount
        Flag if new merchant AND amount > $300 AND > 2× user average
        """
        
        min_amount = self.config['rule4_min_amount']
        relative_multiplier = self.config['rule4_relative_multiplier']

        df['rule4_relative_threshold'] = df['user_mean'] * relative_multiplier

        df['rule4_triggered'] = (
            (df['is_first_time_merchant'] == 1) &
            (df['amount'] > min_amount) &
            (df['amount'] > df['rule4_relative_threshold']) &
            (df['user_count'] >= 3)  # Need history to determine "first time"
        ).astype(int)

        # Add to triggered rules
        mask = df['rule4_triggered'] == 1
        df.loc[mask, 'triggered_rules'] += 'Rule4:NewMerchant,'
        df.loc[mask, 'explanation'] += 'First-time merchant with high amount; '

        return df

    def _apply_rule5_nocturnal(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Rule 5: Nocturnal High-Value Transaction
        Flag if hour between 2am-6am AND amount > user's 75th percentile
        """

        start_hour = self.config['rule5_start_hour']
        end_hour = self.config['rule5_end_hour']

        df['rule5_triggered'] = (
            (df['hour'] >= start_hour) &
            (df['hour'] <= end_hour) &
            (df['amount'] > df['user_p75']) &
            (df['user_count'] >= 5)  # Need history for percentile calculation
        ).astype(int)

        # Add to triggered rules
        mask = df['rule5_triggered'] == 1
        df.loc[mask, 'triggered_rules'] += 'Rule5:Nocturnal,'
        df.loc[mask, 'explanation'] += 'High-value transaction during 2am-6am; '

        return df

    def _aggregate_scores(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Aggregate individual rule scores into overall risk score.

        Args:
            df: DataFrame with individual rule flags

        Returns:
            DataFrame with aggregated risk_score and fraud_flag
        """


        # Calculate weighted risk score
        df['risk_score'] = (
            df['rule1_triggered'] * self.config['rule1_weight'] +
            df['rule2_triggered'] * self.config['rule2_weight'] +
            df['rule3_triggered'] * self.config['rule3_weight'] +
            df['rule4_triggered'] * self.config['rule4_weight'] +
            df['rule5_triggered'] * self.config['rule5_weight']
        )

        # Normalize to 0-100 scale (max possible score if all rules trigger)
        max_score = sum([
            self.config['rule1_weight'],
            self.config['rule2_weight'],
            self.config['rule3_weight'],
            self.config['rule4_weight'],
            self.config['rule5_weight']
        ])
        df['risk_score'] = (df['risk_score'] / max_score * 100).round(2)

        # Set fraud flag (any rule triggered)
        df['fraud_flag'] = (
            (df['rule1_triggered'] == 1) |
            (df['rule2_triggered'] == 1) |
            (df['rule3_triggered'] == 1) |
            (df['rule4_triggered'] == 1) |
            (df['rule5_triggered'] == 1)
        ).astype(int)

        # Clean up triggered_rules (remove trailing comma)
        df['triggered_rules'] = df['triggered_rules'].str.rstrip(',')
        df['explanation'] = df['explanation'].str.rstrip('; ')

        return df

    def get_detection_stats(self, df: pd.DataFrame) -> Dict:
        """
        Get statistics about fraud detection results.

        Args:
            df: DataFrame with fraud detection results

        Returns:
            Dictionary with detection statistics
        """

        total = len(df)
        flagged = df['fraud_flag'].sum()

        stats = {
            'total_transactions': total,
            'flagged_transactions': flagged,
            'flagged_percentage': (flagged / total * 100) if total > 0 else 0,
            'rule1_triggers': df['rule1_triggered'].sum(),
            'rule2_triggers': df['rule2_triggered'].sum(),
            'rule3_triggers': df['rule3_triggered'].sum(),
            'rule4_triggers': df['rule4_triggered'].sum(),
            'rule5_triggers': df['rule5_triggered'].sum(),
            'avg_risk_score': df[df['fraud_flag'] == 1]['risk_score'].mean() if flagged > 0 else 0,
            'max_risk_score': df['risk_score'].max()
        }

        return stats
