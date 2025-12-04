# Install dependencies if not already:
# pip install requests trafilatura transformers torch pandas schedule streamlit

import requests
import trafilatura
import pandas as pd
import schedule
import time
from transformers import AutoTokenizer, AutoModelForSequenceClassification
import torch
import torch.nn.functional as F
import streamlit as st

# =====================================
# 1. Setup FinBERT for sentiment
# =====================================
tokenizer = AutoTokenizer.from_pretrained("yiyanghkust/finbert-tone")
model = AutoModelForSequenceClassification.from_pretrained("yiyanghkust/finbert-tone")

def analyze_sentiment(text):
    """Returns sentiment score: Positive=1, Neutral=0, Negative=-1"""
    inputs = tokenizer(text, return_tensors="pt", truncation=True, max_length=512)
    with torch.no_grad():
        outputs = model(**inputs)
        probs = F.softmax(outputs.logits, dim=1)
        sentiment = torch.argmax(probs).item()
        # FinBERT mapping: 0=neutral, 1=positive, 2=negative
        if sentiment == 1:
            return 1
        elif sentiment == 2:
            return -1
        else:
            return 0

# =====================================
# 2. Sector Mapping
# =====================================
SECTOR_KEYWORDS = {
    "Agriculture": ["avocado", "tomato", "wheat", "corn", "soybean", "farm"],
    "Technology": ["chip", "semiconductor", "electronics", "tech", "AI", "software"],
    "Energy": ["oil", "gas", "petroleum", "coal", "energy", "solar", "wind"],
    "Automotive": ["car", "automobile", "vehicle", "truck", "auto parts"],
    "Finance": ["bank", "finance", "investment", "capital", "stock", "bond"],
}

def assign_sector(text):
    """Return a list of sectors the text belongs to"""
    sectors = []
    text_lower = text.lower()
    for sector, keywords in SECTOR_KEYWORDS.items():
        if any(k in text_lower for k in keywords):
            sectors.append(sector)
    return sectors if sectors else ["Other"]

# =====================================
# 3. Fetch News and Compute Sector Risk
# =====================================
API_KEY = "YOUR_NEWSAPI_KEY"
QUERY = "tariff OR sanction OR trade conflict"

def fetch_and_compute_risk():
    url = 'https://newsapi.org/v2/everything'
    params = {
        'q': QUERY,
        'language': 'en',
        'sortBy': 'publishedAt',
        'pageSize': 10,
        'apiKey': API_KEY
    }
    response = requests.get(url, params=params).json()
    articles = response.get('articles', [])

    results = []
    for article in articles:
        article_url = article['url']
        html = requests.get(article_url, headers={'User-Agent':'Mozilla/5.0'}).text
        text = trafilatura.extract(html)
        if not text:
            continue
        sentiment_score = analyze_sentiment(text[:512])
        sectors = assign_sector(text)
        for sector in sectors:
            results.append({
                "title": article['title'],
                "url": article_url,
                "publishedAt": article['publishedAt'],
                "sector": sector,
                "sentiment_score": sentiment_score
            })
    return pd.DataFrame(results)

# =====================================
# 4. Compute Sector-Level Risk
# =====================================
def compute_sector_risk(df):
    if df.empty:
        return pd.DataFrame(columns=["sector", "risk_score", "articles_count"])
    sector_summary = df.groupby("sector")["sentiment_score"].mean().reset_index()
    sector_summary.rename(columns={"sentiment_score": "risk_score"}, inplace=True)
    sector_summary["articles_count"] = df.groupby("sector")["title"].count().values
    # Higher negative sentiment → higher risk
    sector_summary["risk_score"] = -sector_summary["risk_score"]
    return sector_summary.sort_values(by="risk_score", ascending=False)

# =====================================
# 5. Streamlit Dashboard
# =====================================
st.title("🌐 Cross-Border Trade Risk Analyzer (Real-Time)")

st.write("Fetching the latest trade news and computing sector-level risk...")

df_articles = fetch_and_compute_risk()
df_risk = compute_sector_risk(df_articles)

st.subheader("Top Risky Sectors")
st.dataframe(df_risk)

st.subheader("Latest News Articles")
st.dataframe(df_articles[["publishedAt","sector","title","url","sentiment_score"]])
