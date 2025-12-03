"""
Feature engineering for transaction fraud detection.
Calculates user statistics, rolling windows, merchant history, and temporal features.
"""

import pandas as pd


class FeatureEngineer:

    def engineer_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Generate all features needed for fraud detection.

        Args:
            df: Preprocessed transaction DataFrame

        Returns:
            DataFrame with additional feature columns
        """

        print("Engineering features...")

        df = df.copy()

        df = self._add_user_statistics(df)
        df = self._add_rolling_windows(df)
        df = self._add_merchant_features(df)
        df = self._add_temporal_features(df)

        print("Feature engineering complete")

        return df
    
    
    def _add_user_statistics(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate per-user historical statistics.

        Args:
            df: Transaction DataFrame

        Returns:
            DataFrame with user statistics columns
            
        """

        user_stats = df.groupby('user_id')['amount'].agg([
            ('user_mean', 'mean'),
            ('user_std', 'std'),
            ('user_min', 'min'),
            ('user_max', 'max'),
            ('user_count', 'count'),
            ('user_p25', lambda x: x.quantile(0.25)),
            ('user_p50', lambda x: x.quantile(0.50)),
            ('user_p75', lambda x: x.quantile(0.75)),
            ('user_p95', lambda x: x.quantile(0.95))
        ]).reset_index()

        # fill NaN std with 0 for users with only 1 transaction
        user_stats['user_std'] = user_stats['user_std'].fillna(0)

        # merge back to main dataframe
        df = df.merge(user_stats, on='user_id', how='left')

        # calculate daily average spending
        user_daily_stats = df.groupby('user_id').agg({
            'amount': 'sum',
            'timestamp': lambda x: (x.max() - x.min()).days + 1
        }).reset_index()
        user_daily_stats.columns = ['user_id', 'total_amount', 'days_active']
        user_daily_stats['user_daily_avg'] = (
            user_daily_stats['total_amount'] / user_daily_stats['days_active']
        )

        df = df.merge(
            user_daily_stats[['user_id', 'user_daily_avg']],
            on='user_id',
            how='left'
        )

        return df


    
    def _add_rolling_windows(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate rolling window aggregations.

        Args:
            df: Transaction DataFrame

        Returns:
            DataFrame with rolling window columns
        """

        # Set timestamp as index for rolling operations
        df = df.set_index('timestamp').sort_index()

        # Calculate rolling windows per user
        # 10-minute window: transaction count
        df['rolling_10min_count'] = df.groupby('user_id')['amount'].transform(
            lambda x: x.rolling('10min', closed='left').count()
        )

        # 24-hour window: sum of amounts
        df['rolling_24h_sum'] = df.groupby('user_id')['amount'].transform(
            lambda x: x.rolling('24h', closed='left').sum()
        )

        # 24-hour window: transaction count
        df['rolling_24h_count'] = df.groupby('user_id')['amount'].transform(
            lambda x: x.rolling('24h', closed='left').count()
        )

        # Reset index
        df = df.reset_index()

        # Fill NaN with 0 (first transactions have no history)
        df['rolling_10min_count'] = df['rolling_10min_count'].fillna(0)
        df['rolling_24h_sum'] = df['rolling_24h_sum'].fillna(0)
        df['rolling_24h_count'] = df['rolling_24h_count'].fillna(0)

        # Add 1 to include current transaction
        df['rolling_10min_count'] += 1
        df['rolling_24h_count'] += 1
        df['rolling_24h_sum'] += df['amount']

        return df

    def _add_merchant_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate merchant-related features.

        Args:
            df: Transaction DataFrame

        Returns:
            DataFrame with merchant feature columns
        """

        # For each transaction, check if merchant was seen before
        # This uses cumcount to track how many times this user-merchant combo has occurred
        df['merchant_rank'] = df.groupby(['user_id', 'merchant_name']).cumcount()
        df['is_first_time_merchant'] = (df['merchant_rank'] == 0).astype(int)

        return df

    def _add_temporal_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Extract temporal features from timestamp.

        Args:
            df: Transaction DataFrame

        Returns:
            DataFrame with temporal feature columns
        """
        
        # Extract time components
        df['hour'] = df['timestamp'].dt.hour
        df['day_of_week'] = df['timestamp'].dt.dayofweek
        df['is_weekend'] = (df['day_of_week'] >= 5).astype(int)

        # Flag nocturnal hours (2am-6am)
        df['is_nocturnal'] = ((df['hour'] >= 2) & (df['hour'] <= 6)).astype(int)

        # Calculate time since last transaction per user
        df['time_since_last_tx'] = df.groupby('user_id')['timestamp'].diff()
        df['time_since_last_tx_seconds'] = (
            df['time_since_last_tx'].dt.total_seconds().fillna(0)
        )

        return df
