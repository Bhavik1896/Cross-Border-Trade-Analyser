import torch
import spacy
import xgboost
import kafka
import pandas as pd
import polars as pl
import joblib
import transformers

print("✅ ENVIRONMENT IS PERFECT!")
print(f"   - Torch: {torch.__version__}")
print(f"   - Pandas: {pd.__version__}")
try:
    nlp = spacy.load("en_core_web_sm")
    print(f"   - Spacy Model: {nlp.meta['name']} loaded.")
except Exception as e:
    print(f"   ❌ Spacy Error: {e}")