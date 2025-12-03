import argparse
import yaml
import sys
import os
from pathlib import Path

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from data_processor import DataProcessor
from feature_engineer import FeatureEngineer
from fraud_detector import FraudDetector


def load_config() -> dict:
    """
    Load configuration from YAML file.

    Returns:
        Configuration dictionary or None if file doesn't exist
    """

    config_path = Path(__file__).parent / 'config' / 'rules_config.yaml'
    if config_path.exists():
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    return None


def main():

    # Parse command-line arguments
    parser = argparse.ArgumentParser(
        description='Transaction Fraud Detection System',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Example:
  python3 main.py --input data/input/sample_transactions.csv --output data/output/flagged_transactions.csv
        """
    )

    parser.add_argument(
        '--input',
        required=True,
        help='Path to input transaction CSV file'
    )

    parser.add_argument(
        '--output',
        required=True,
        help='Path to output flagged transactions CSV file'
    )

    args = parser.parse_args()

    # Validate input file
    if not os.path.exists(args.input):
        print(f"Error: Input file not found: {args.input}")
        sys.exit(1)

    # Create output directory if it doesn't exist
    output_dir = os.path.dirname(args.output)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)

    print("=" * 60)
    print("TRANSACTION FRAUD DETECTION SYSTEM")
    print("=" * 60)
    print(f"Input:  {args.input}")
    print(f"Output: {args.output}")
    print("=" * 60 + "\n")

    try:
        config = load_config()
        if not config:
            print("Error: Configuration file not found at config/rules_config.yaml")
            sys.exit(1)
        print("Loaded configuration from file")

        data_processor = DataProcessor()
        engineer = FeatureEngineer()
        detector = FraudDetector(config)

        df = data_processor.load_transactions(args.input)
        df = engineer.engineer_features(df)
        df = detector.detect_fraud(df)
        stats = detector.get_detection_stats(df)
        print(df.head())
        print(stats)

    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()