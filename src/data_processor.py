import pandas as pd

# Processes transaction CSV files for fraud detection
class DataProcessor:

    REQUIRED_COLUMNS = ['user_id', 'timestamp', 'merchant_name', 'amount']

    # @args: chunk_size: Number of rows to process at once for large files
    def __init__(self, chunk_size: int = 100000):
        self.chunk_size = chunk_size

    # @args: filepath: Path to transaction CSV file
    # @returns: Preprocessed DataFrame with validated transactions
    def load_transactions(self, filepath: str) -> pd.DataFrame:
        print(f"Loading transactions from {filepath}...")

        try:
            df = pd.read_csv(filepath)
        except Exception as e:
            raise ValueError(f"Failed to load CSV: {e}")

        self._validate_schema(df)

        df = self._preprocess(df)

        print(f"Loaded {len(df)} transactions for {df['user_id'].nunique()} users")

        return df

    # Validate that DataFrame has required columns.
    def _validate_schema(self, df: pd.DataFrame) -> None:
        missing_cols = set(self.REQUIRED_COLUMNS) - set(df.columns)
        if missing_cols:
            raise ValueError(f"Missing required columns: {missing_cols}")

        if len(df) == 0:
            raise ValueError("CSV file is empty")

    # Preprocesses and returnscleaned transaction data.
    def _preprocess(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()

        initial_count = len(df)
        df = df.dropna(subset=self.REQUIRED_COLUMNS)
        dropped = initial_count - len(df)
        if dropped > 0:
            print(f"Dropped {dropped} rows with missing values")

        # Converts timestamps to datetime objects and removes rows with missing values.
        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
        df = df.dropna(subset=['timestamp'])

        # Remove negative or zero amounts
        df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
        df = df[df['amount'] > 0] 

        # Normalizes merchant names to lowercase and removes whitespace.
        df['merchant_name'] = df['merchant_name'].astype(str).str.strip().str.lower()

        # Convert user_id to string for consistency
        df['user_id'] = df['user_id'].astype(str)

        # Sort by user and timestamp for efficient processing
        df = df.sort_values(['user_id', 'timestamp']).reset_index(drop=True)

        if len(df) == 0:
            raise ValueError("No valid transactions after preprocessing")

        return df
