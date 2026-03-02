import os
import time
import pandas as pd
from dotenv import load_dotenv
from springernature_api_client import metadata as meta

def fetch_springer_metadata(input_csv, output_csv, api_key_env = "SPRINGER_API_KEY",
                            batch_save_every = 50, daily_limit = 450):
    '''
    Fetch metadata from Springer for all DOIs in input_csv.
    Saves partial results automatically and can resume.
    
    Parameters:
        input_csv (str): Path to input CSV containing 'Item DOI'.
        output_csv (str): Path to output CSV to save metadata.
        api_key_env (str): Name of environment variable with Springer API key.
        batch_save_every (int): Save progress every N entries.
        daily_limit (int): Max queries per run/day (to avoid hitting API limits).
    '''

    # Load API key
    load_dotenv() # Load .env file

    # Access the API key
    SPRINGER_API_KEY = os.getenv("SPRINGER_API_KEY")
    if not SPRINGER_API_KEY:
        raise ValueError("Springer API key not found. Make sure .env exists and contains SPRINGER_API_KEY")

    # Initialize client