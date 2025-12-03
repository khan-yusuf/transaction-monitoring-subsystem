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
