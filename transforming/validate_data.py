import logging

import pandas as pd
import yaml

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()


def read_yaml_from_local(config_path):
    """Read YAML configuration file from local disk."""
    logger.info(f"Reading YAML configuration from {config_path}")
    try:
        with open(config_path, "r") as file:
            return yaml.safe_load(file)
    except Exception as e:
        logger.error(f"Error reading YAML file {config_path}: {e}")
        raise


def read_dataset_from_local(dataset_path):
    """Read dataset from local disk into a Pandas DataFrame."""
    logger.info(f"Reading dataset from {dataset_path}")
    file_extension = dataset_path.split(".")[-1]

    if file_extension == "parquet":
        try:
            df = pd.read_parquet(dataset_path)
            logger.info("Dataset loaded successfully.")
            return df
        except Exception as e:
            logger.error(f"Error reading dataset {dataset_path}: {e}")
            raise
    else:
        logger.error("Unsupported file format. Use Parquet.")
        raise ValueError("Unsupported file format. Use Parquet.")


def clean_dataset(df: pd.DataFrame, dataset_name, config):
    """Clean dataset based on YAML rules."""
    logger.info(f"Cleaning dataset: {dataset_name}")
    dataset_config = config["datasets"].get(dataset_name, {})

    # Get columns to keep and critical columns
    columns_to_keep = dataset_config.get("columns_to_keep", [])
    critical_columns = dataset_config.get("critical_columns", [])

    logger.info(f"Columns to keep: {columns_to_keep}")
    logger.info(f"Critical columns: {critical_columns}")

    # Drop duplicates, keeping first
    df = df.drop_duplicates()
    logger.info("Duplicates dropped.")

    # Convert column names
    df.columns = ["_".join(col.replace("'", "").strip("()").split(", ")) for col in df.columns]
    logger.info("Column names cleaned.")

    # Check for missing values in critical columns
    if critical_columns and df[critical_columns].isnull().any().any():
        missing_cols = df[critical_columns].isnull().sum()
        logger.error(f"Critical columns contain missing values:\n{missing_cols}")
        raise ValueError(f"Critical columns contain missing values:\n{missing_cols}")

    # Keep only specified columns
    df = df[columns_to_keep]
    logger.info(f"Kept only specified columns: {columns_to_keep}")

    # Fill nulls in kept columns with 0
    df = df.fillna(0)
    logger.info("Filled missing values with 0.")

    return df


def save_dataset_to_local(df, output_path):
    """Save cleaned dataset back to local disk in Parquet format."""
    try:
        df.to_parquet(output_path, index=False)
        logger.info(f"Cleaned dataset saved to {output_path}")
    except Exception as e:
        logger.error(f"Error saving dataset to {output_path}: {e}")
        raise


def main():
    # File paths (local)
    files = ["summary", "defense", "keepers", "misc", "passing", "passing_types", "possession"]
    file_name = f"{files[6]}.parquet"
    CONFIG_PATH = "./validate_config.yml"
    DATASET_PATH = f"../test_data/{file_name}"
    OUTPUT_PATH = f"../test_res_data/{file_name}"
    # Load YAML config from local disk
    try:
        config = read_yaml_from_local(CONFIG_PATH)
    except Exception as e:
        logger.error(f"Failed to load configuration: {e}")
        return

    # Load dataset from local disk
    try:
        df = read_dataset_from_local(DATASET_PATH)
    except Exception as e:
        logger.error(f"Failed to load dataset: {e}")
        return

    # Extract dataset name from the file path (assuming dataset name is the filename)
    dataset_name = DATASET_PATH.split("/")[-1].split(".")[0]

    # Clean dataset
    try:
        df_cleaned = clean_dataset(df, dataset_name, config)
    except Exception as e:
        logger.error(f"Error cleaning dataset: {e}")
        return

    # Save cleaned dataset to local disk
    try:
        save_dataset_to_local(df_cleaned, OUTPUT_PATH)
    except Exception as e:
        logger.error(f"Error saving cleaned dataset: {e}")


if __name__ == "__main__":
    main()
