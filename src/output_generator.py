"""
Output generator for fraud detection results.
Writes flagged transactions to CSV with detailed information.
"""

import pandas as pd


class OutputGenerator:

    OUTPUT_COLUMNS = [
        'user_id',
        'timestamp',
        'merchant_name',
        'amount',
        'risk_score',
        'triggered_rules',
        'explanation'
    ]

    def save_flagged_transactions(
        self,
        df: pd.DataFrame,
        output_path: str,
        include_all: bool = False
    ) -> None:
        """
        Save flagged transactions to CSV.

        Args:
            df: DataFrame with fraud detection results
            output_path: Path to save output CSV
            include_all: If True, include all transactions; if False, only flagged ones
        """

        # Filter to flagged transactions unless include_all is True
        if not include_all:
            output_df = df[df['fraud_flag'] == 1].copy()
            print(f"Saving {len(output_df)} flagged transactions...")
        else:
            output_df = df.copy()
            print(f"Saving all {len(output_df)} transactions...")

        if len(output_df) == 0:
            print("No transactions to save!")
            return

        # Select and order output columns
        available_cols = [col for col in self.OUTPUT_COLUMNS if col in output_df.columns]
        output_df = output_df[available_cols]

        # Sort by risk score (highest first)
        if 'risk_score' in output_df.columns:
            output_df = output_df.sort_values('risk_score', ascending=False)

        # Save to CSV
        output_df.to_csv(output_path, index=False)
        print(f"Results saved to {output_path}")


    def save_detection_report(
        self,
        df: pd.DataFrame,
        stats: dict,
        report_path: str
    ) -> None:
        """
        Save a detailed detection report.

        Args:
            df: DataFrame with fraud detection results
            stats: Dictionary with detection statistics
            report_path: Path to save report file
        """

        with open(report_path, 'w') as f:
            f.write("=" * 60 + "\n")
            f.write("TRANSACTION FRAUD DETECTION REPORT\n")
            f.write("=" * 60 + "\n\n")

            # Overall statistics
            f.write("OVERALL STATISTICS:\n")
            f.write("-" * 60 + "\n")
            f.write(f"Total Transactions:        {stats['total_transactions']:,}\n")
            f.write(f"Flagged Transactions:      {stats['flagged_transactions']:,}\n")
            f.write(f"Flagged Percentage:        {stats['flagged_percentage']:.2f}%\n")
            f.write(f"Average Risk Score:        {stats['avg_risk_score']:.2f}\n")
            f.write(f"Maximum Risk Score:        {stats['max_risk_score']:.2f}\n\n")

            # Rule-specific statistics
            f.write("RULE TRIGGER COUNTS:\n")
            f.write("-" * 60 + "\n")
            f.write(f"Rule 1 (Velocity):         {stats['rule1_triggers']:,}\n")
            f.write(f"Rule 2 (Amount Anomaly):   {stats['rule2_triggers']:,}\n")
            f.write(f"Rule 3 (Spending Spike):   {stats['rule3_triggers']:,}\n")
            f.write(f"Rule 4 (New Merchant):     {stats['rule4_triggers']:,}\n")
            f.write(f"Rule 5 (Nocturnal):        {stats['rule5_triggers']:,}\n\n")

            # Top flagged users
            if stats['flagged_transactions'] > 0:
                f.write("TOP 10 USERS BY FLAGGED TRANSACTIONS:\n")
                f.write("-" * 60 + "\n")
                flagged_df = df[df['fraud_flag'] == 1]
                top_users = flagged_df.groupby('user_id').agg({
                    'fraud_flag': 'sum',
                    'risk_score': 'mean'
                }).sort_values('fraud_flag', ascending=False).head(10)

                for user_id, row in top_users.iterrows():
                    f.write(f"{user_id}: {int(row['fraud_flag'])} flagged transactions, ")
                    f.write(f"avg risk score {row['risk_score']:.2f}\n")

                f.write("\n")

                # Risk score distribution
                f.write("RISK SCORE DISTRIBUTION:\n")
                f.write("-" * 60 + "\n")
                risk_bins = [0, 30, 60, 80, 100]
                risk_labels = ['Low (0-30)', 'Medium (31-60)', 'High (61-80)', 'Very High (81-100)']
                flagged_df['risk_category'] = pd.cut(
                    flagged_df['risk_score'],
                    bins=risk_bins,
                    labels=risk_labels,
                    include_lowest=True
                )
                risk_dist = flagged_df['risk_category'].value_counts().sort_index()
                for category, count in risk_dist.items():
                    f.write(f"{category}: {count:,}\n")

        print(f"Detection report saved to {report_path}")


    def print_summary(self, stats: dict) -> None:
        """
        Print a summary of detection results to console.

        Args:
            stats: Dictionary with detection statistics
        """
        
        print("\n" + "=" * 60)
        print("FRAUD DETECTION SUMMARY")
        print("=" * 60)
        print(f"Total Transactions:        {stats['total_transactions']:,}")
        print(f"Flagged Transactions:      {stats['flagged_transactions']:,}")
        print(f"Flagged Percentage:        {stats['flagged_percentage']:.2f}%")
        print(f"Average Risk Score:        {stats['avg_risk_score']:.2f}")
        print("-" * 60)
        print(f"Rule 1 Triggers:           {stats['rule1_triggers']:,}")
        print(f"Rule 2 Triggers:           {stats['rule2_triggers']:,}")
        print(f"Rule 3 Triggers:           {stats['rule3_triggers']:,}")
        print(f"Rule 4 Triggers:           {stats['rule4_triggers']:,}")
        print(f"Rule 5 Triggers:           {stats['rule5_triggers']:,}")
        print("=" * 60 + "\n")
