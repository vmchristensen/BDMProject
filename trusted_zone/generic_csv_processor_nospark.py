import boto3
import pandas as pd
from io import StringIO
import unicodedata
from typing import Union, Dict, List # Changed import
import os

# AWS S3 configuration
s3 = boto3.client('s3')
landing_bucket = 'bdm.project.input'
trusted_bucket = 'bdm.trusted.zone'
trusted_csv_prefix = 'clean_data/csv/'  # Set the correct trusted zone prefix

def normalize_text(text: Union[str, None]) -> Union[str, None]: # Changed type hint
    """
    Normalizes text by stripping, lowercasing, and applying Unicode normalization.
    """
    if pd.isna(text):
        return None
    return unicodedata.normalize("NFKC", str(text).strip().lower())

def standardize_date(date_str: Union[str, None], date_formats: List[str] = ('%m/%d/%Y', '%d/%m/%Y', '%Y-%m-%d')) -> Union[str, None]: # Changed type hint
    """
    Standardizes a date string to the format 'DD/MM/YYYY'.
    """
    if pd.isna(date_str):
        return None
    for fmt in date_formats:
        try:
            return pd.to_datetime(date_str, format=fmt).strftime('%d/%m/%Y')
        except (ValueError, TypeError):
            pass
    return None

def clean_and_transform_df(df: pd.DataFrame, filename: str) -> pd.DataFrame:
    """
    Cleans and transforms a Pandas DataFrame.  This function is designed to be
    more generic, handling different CSV structures.

    Args:
        df: The Pandas DataFrame to clean.
        filename: The name of the CSV file (for logging and conditional logic).

    Returns:
        The cleaned Pandas DataFrame.  Returns an empty DataFrame if no
        appropriate cleaning is performed.
    """
    print(f"Cleaning data from: {filename}")

    # Basic Cleaning (Apply to all DataFrames)
    df = df.drop_duplicates()
    text_cols = df.select_dtypes(include=['object']).columns
    df[text_cols] = df[text_cols].apply(lambda col: col.map(normalize_text))
    print("- Applied basic deduplication and text normalization.")

    # Date Standardization (If date columns are present)
    date_cols = df.select_dtypes(include=['datetime64[ns]']).columns
    if len(date_cols) > 0:
        for col in date_cols:
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%d/%m/%Y')
        print("- Applied date standardization to detected date columns.")

    # File-Specific Cleaning and Transformation (using column check)
    if 'Entity' in df.columns and 'Code' in df.columns and 'Year' in df.columns:
        print("- Applying mental health disorders cleaning")
        df['Year'] = pd.to_numeric(df['Year'], errors='coerce').astype('Int64')
        numeric_cols = ['AlcoholUseDisorders', 'DrugUseDisorders', 'DepressiveDisorders',
                        'BipolarDisorder', 'AnxietyDisorders', 'EatingDisorders',
                        'Schizophrenia', 'TotalPercentageOfPopulation', 'Unemployment',
                        'SuicideDeathsRate']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        if 'TotalPercentageOfPopulation' in df.columns:
            df['TotalPercentageOfPopulation'] = df['TotalPercentageOfPopulation'].clip(0, 100)
        df = df.dropna(subset=['Code', 'Year'], how='all')

    elif 'Period' in df.columns and 'Start' in df.columns and 'End' in df.columns:
        print("- Applying therapy data cleaning")
        date_cols = ['Start', 'End']
        for col in date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%d/%m/%Y')
        numeric_cols = ['NoTherapy', 'Therapy', 'Medication']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').clip(0, 100)
        if 'End' in df.columns and 'Start' in df.columns:
             df = df[pd.to_datetime(df['End'], format='%d/%m/%Y', errors='coerce') >= pd.to_datetime(df['Start'], format='%d/%m/%Y', errors='coerce')]

    else:
        print(f"- No specific cleaning defined for: {filename}.  Applying only basic cleaning.")
        return pd.DataFrame()

    return df



def process_csv(bucket: str, prefix: str, output_prefix: str):
    """
    Processes CSV files in an S3 bucket, cleans them using a generic function,
    and saves the cleaned data to another location in S3.

    Args:
        bucket: The name of the S3 bucket containing the CSV files.
        prefix: The prefix of the CSV files in the S3 bucket.
        output_prefix: The prefix to use when saving the cleaned CSV files in S3.
    """
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)

    for obj in response.get('Contents', []):
        key = obj['Key']
        if not key.endswith('.csv'):
            continue

        print(f"Processing: {key}")
        try:
            file_obj = s3.get_object(Bucket=bucket, Key=key)
            csv_content = file_obj['Body'].read().decode('utf-8')
            df = pd.read_csv(StringIO(csv_content))

            cleaned_df = clean_and_transform_df(df.copy(), filename=key)  # Pass filename

            if cleaned_df.empty:
                print(f"Skipping empty DataFrame for {key}")
                continue

            # Save to trusted zone
            # Construct the new key to save in the correct location
            new_key = os.path.join(output_prefix, os.path.basename(key))
            csv_buffer = StringIO()
            cleaned_df.to_csv(csv_buffer, index=False)
            s3.put_object(
                Bucket=trusted_bucket,
                Key=new_key,
                Body=csv_buffer.getvalue().encode('utf-8')
            )
            print(f"Saved cleaned data to: s3://{trusted_bucket}/{new_key}")

        except Exception as e:
            print(f"Error processing {key}: {e}")



if __name__ == "__main__":
    process_csv(
        bucket='bdm.project.input',
        prefix='data/csv/',
        output_prefix='clean_data/csv/',  # Use the full desired output prefix
    )
    print("CSV data processing complete.")
