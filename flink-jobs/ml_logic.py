import pandas as pd
import joblib
import re
import spacy
import torch
import pycountry
from transformers import AutoTokenizer, AutoModelForSequenceClassification
from datetime import datetime
from db_logic import save_and_get_trends, update_final_score
from geo_resolver import resolve_region_and_country

# ==========================================
# 1. LOAD AI MODELS
# ==========================================
print("Loading AI Models...")

ifi_model = joblib.load("/opt/flink/jobs/ifi_xgb_model.pkl")
nlp = spacy.load("en_core_web_sm")
bert_tokenizer = AutoTokenizer.from_pretrained("ProsusAI/finbert")
bert_model = AutoModelForSequenceClassification.from_pretrained("ProsusAI/finbert")

print("All Models Loaded.")

# ==========================================
# 2. CONSTANTS
# ==========================================
SECTOR_GROUPS = {
    "Energy": ["ENV", "POWER", "PIPELINE", "COAL", "OIL", "MINING", "GAS", "FUEL"],
    "Manufacturing": ["INDUSTRY", "TRADE", "EXPORT", "IMPORT", "PRODUCTION", "FACTORY", "TARIFFS"],
    "Technology": ["TECH", "CYBER", "INTERNET", "INFO", "SOFTWARE", "AI", "DIGITAL"],
    "Finance": ["ECON", "BANK", "FINANCE", "TAX", "EPU", "MARKET", "MONEY"],
    "Healthcare": ["HEALTH", "MED", "DRUG", "PHARMA", "HOSPITAL", "VIRUS"],
    "Defense": ["MIL", "SECURITY", "DEFENSE", "WAR", "ARMY", "WEAPON"],
    "Agriculture": ["FOOD", "WATER", "AGRICULTURE", "FARM", "CROP", "WHEAT"],
    "Public Sector": ["POL", "GOV", "STATE", "PUBLIC", "LAW", "ELECTION"],
    "Infrastructure": ["ROAD", "RAIL", "URBAN", "INFRA", "BUILDING", "CONSTRUCTION"],
    "Education": ["EDU", "SCHOOL", "UNIV", "COLLEGE", "STUDENT", "TEACHER", "ACADEMIC", "DEGREE", "LEARNING", "CAMPUS","RESEARCH"],
    "Transportation": ["TRANS", "LOGISTICS", "AVIATION", "AIRLINE", "CARGO", "FREIGHT", "SHIPPING", "PORT", "VEHICLE","TRANSIT"],
    "Retail & Consumer": ["RETAIL", "SHOP", "CONSUMER", "COMMERCE", "MALL", "FASHION", "LUXURY", "GOODS", "SALES","BRAND"],
    "Media & Entertainment": ["MEDIA", "NEWS", "PRESS", "FILM", "MOVIE", "MUSIC", "ENTERTAINMENT", "STREAMING","GAMING", "SPORT", "ARTS"],
    "Real Estate": ["HOUSING", "PROPERTY", "REALESTATE", "RENT", "ESTATE", "COMMERCIAL", "RESIDENTIAL", "MORTGAGE","TENANT"],
    "Environment": ["CLIMATE", "POLLUTION", "CARBON", "EMISSION", "WASTE", "SUSTAINABILITY", "CONSERVATION", "DISASTER","ECO", "WARMING"],
    "Labor & Employment": ["LABOR", "JOB", "WORKFORCE", "UNEMPLOYMENT", "UNION", "STRIKE", "WAGE", "EMPLOYEE", "HIRING","HR"],
    "Legal & Justice": ["LAW", "COURT", "LEGAL", "JUDGE", "CRIME", "POLICE", "PRISON", "JUSTICE", "LAWSUIT", "TRIAL","RIGHTS"],
    "Science & Space": ["SCIENCE", "SPACE", "NASA", "ASTRONOMY", "PHYSICS", "CHEMISTRY", "BIOLOGY", "SATELLITE","LAUNCH", "EXPLORATION"],
    "Tourism & Hospitality": ["TOURISM", "TRAVEL", "HOTEL", "HOSPITALITY", "RESORT", "VACATION", "TRIP", "TOURIST","RESTAURANT", "VISA"]
}

LEADER_COUNTRY_MAP = {
    "biden": "US",
    "joe biden": "US",
    "trump": "US",
    "putin": "RU",
    "vladimir putin": "RU",
    "xi": "CN",
    "xi jinping": "CN",
    "macron": "FR",
    "rishi sunak": "GB",
    "modi": "IN",
    "narendra modi": "IN",
    "zelensky": "UA",
}
DEMONYM_MAPPER = {
    "Indian": "India",
    "American": "United States",
    "Russian": "Russia",
    "Chinese": "China",
    "British": "United Kingdom",
    "French": "France",
    "German": "Germany",
}


TRADE_BARRIERS = [
    "tariff", "tax", "sanction", "ban", "embargo",
    "penalty", "duty", "levy", "restriction", "hike"
]

# ==========================================
# 3. HELPER FUNCTIONS
# ==========================================

def extract_gdelt_countries(text: str):
    """
    Extract ISO-2 country codes from GDELT-style structured text
    Example token: #US#TX#Texas#
    """
    countries = []
    tokens = text.split("#")

    for token in tokens:
        if len(token) == 2 and token.isupper():
            try:
                pycountry.countries.get(alpha_2=token)
                countries.append(token)
            except:
                pass

    return list(dict.fromkeys(countries))  # preserve order, remove duplicates

def extract_country_keywords(text: str):
    text_lower = f" {text.lower()} "
    matches = set()

    COUNTRY_HINTS = {
        " india ": "IN",
        " china ": "CN",
        " russia ": "RU",
        " iran ": "IR",
        " usa ": "US",
        " united states ": "US",
        " uk ": "GB",
        " britain ": "GB",
        " germany ": "DE",
        " france ": "FR",
        " israel ": "IL",
        " ukraine ": "UA",
    }

    for k, v in COUNTRY_HINTS.items():
        if k in text_lower:
            matches.add(v)   # ✅ ISO CODE ONLY

    return list(matches)

def extract_sectors_multi(text: str):
    text_upper = text.upper()
    sectors = []

    for sector, keywords in SECTOR_GROUPS.items():
        if re.search(r"|".join(re.escape(k) for k in keywords), text_upper):
            sectors.append(sector)

    return sectors

def extract_locations_with_roles(text: str):
    roles = []

    structured = extract_gdelt_countries(text)

    # PRIMARY country
    if structured:
        roles.append((structured[0], "PRIMARY"))

        # ACTOR countries
        for cc in structured[1:]:
            roles.append((cc, "ACTOR"))

    # AFFECTED via keywords
    keyword_matches = extract_country_keywords(text)
    for cc in keyword_matches:
        if cc not in structured:
            roles.append((cc, "AFFECTED"))

    if not roles:
        roles.append(("GLOBAL", "PRIMARY"))

    return roles

def estimate_tone_fast(text: str) -> float:
    inputs = bert_tokenizer(text, return_tensors="pt", truncation=True)
    with torch.no_grad():
        probs = torch.softmax(bert_model(**inputs).logits, dim=1).numpy()[0]
    return float(probs[0] - probs[1])

def adjust_sentiment_rules(text: str, tone: float) -> float:
    text_lower = text.lower()
    if any(w in text_lower for w in TRADE_BARRIERS):
        if any(v in text_lower for v in ["impose", "hike", "raise", "increase", "against"]):
            return -2.0
    return tone

def pretty_locations(locations):
    """
    For logging only — does NOT affect DB or ML
    """
    output = []
    for cc, role in locations:
        name = resolve_region_and_country(cc)["country_name"]
        output.append((name, role))
    return output

from db_logic import save_relation




# ==========================================
# 4. CORE PIPELINE FUNCTION
# ==========================================
def get_india_business_verdict(text: str, news_url: str | None = None):
    final_results = []
    current_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    sectors = extract_sectors_multi(text)
    if not sectors:
        return []

    # ✅ USE ROLE-BASED EXTRACTION
    locations = extract_locations_with_roles(text)

    # PRIMARY country for scoring
    primary_country = next(cc for cc, role in locations if role == "PRIMARY")

    assert primary_country == "GLOBAL" or len(primary_country) == 2, \
        f"INVALID COUNTRY CODE IN ML PIPELINE: {primary_country}"

    base_tone = estimate_tone_fast(text)
    adj_tone = adjust_sentiment_rules(text, base_tone)

    for sector in sectors:
        tone_7d, tone_30d, real_trend = save_and_get_trends(
            country=primary_country,
            state="ALL",
            sector=sector,
            tone=adj_tone,
            headline=text
        )

        input_df = pd.DataFrame([{
            "Country_Code": primary_country,
            "MacroSector": sector,
            "n_articles": 1,
            "tone_mean": adj_tone,
            "pos_rate": float(adj_tone > 0),
            "neg_rate": float(adj_tone < 0),
            "tone_7d": tone_7d,
            "tone_30d": tone_30d,
            "tone_trend": real_trend
        }])

        score = float(ifi_model.predict(input_df)[0])

        verdict = (
            "YES" if score > 0.05 else
            "NO" if score < -0.05 else
            "MAYBE"
        )

        update_final_score(
            country_code=primary_country,
            state="ALL",
            sector=sector,
            score=score,
            verdict=verdict
        )

        # Store ALL country relations
        for cc, role in locations:
            if role != "PRIMARY":
                save_relation(
                    country_code=cc,
                    role=role,
                    sector=sector,
                    headline=text,
                    news_url=news_url
                )

        primary_name = resolve_region_and_country(primary_country)["country_name"]

        print(
            f"NEWS → {news_url} | PRIMARY={primary_name} | "
            f"Countries={pretty_locations(locations)} | "
            f"{sector} | {verdict} | IFI={score:.4f}"
        )

    return final_results

