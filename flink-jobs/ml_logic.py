import pandas as pd
import joblib
import re
import spacy
import torch
import pycountry
from transformers import AutoTokenizer, AutoModelForSequenceClassification
from datetime import datetime, timedelta
from db_logic import save_and_get_trends, update_final_score, Base, engine, NewsEvent
from geo_resolver import resolve_region_and_country

# ----------------------------------------------------------------

# ==========================================
# 1. LOAD AI MODELS (Global Scope)
# ==========================================
print("Loading AI Models...")

try:
    ifi_model = joblib.load("/opt/flink/jobs/ifi_xgb_model.pkl")
    nlp = spacy.load("en_core_web_sm")
    bert_tokenizer = AutoTokenizer.from_pretrained("ProsusAI/finbert")
    bert_model = AutoModelForSequenceClassification.from_pretrained("ProsusAI/finbert")
    print("All Models Loaded.")
except FileNotFoundError:
    print("Critical: Models failed to load. Check 'ifi_xgb_model.pkl' path.")
    exit(1)

# ==========================================
# 2. CONSTANTS & DICTIONARIES
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
}

DEMONYM_MAPPER = {
    "Russian": "Russia", "Indian": "India", "American": "United States", "Chinese": "China",
    "Japanese": "Japan", "French": "France", "German": "Germany", "British": "United Kingdom"
}
TRADE_BARRIERS = ["tariff", "tax", "sanction", "ban", "embargo", "penalty", "duty", "levy", "restriction", "hike"]


# ==========================================
# 3. HELPER FUNCTIONS (Extraction & Sentiment)
# ==========================================
def extract_sectors_multi(text):
    text_upper = text.upper()
    found = []
    for sector, keywords in SECTOR_GROUPS.items():
        if re.search(r"|".join(re.escape(k) for k in keywords), text_upper):
            found.append(sector)
    return found if found else ["Misc"]


# def extract_countries_multi(text):
    # text_upper = text.upper()
#     found = set()

    # for alias, country_name in COUNTRY_ALIAS_MAP.items():
    #     # Word-boundary to avoid false matches (e.g. "us" vs "US")
    #     if re.search(rf"\b{re.escape(alias)}\b", text_upper):
    #         try:
    #             found.add(pycountry.countries.lookup(country_name).alpha_2)
    #         except:
    #             # EU or non-standard entities
    #             if country_name == "European Union":
    #                 found.add("EU")

    # ️SECOND: SpaCy NER + demonyms
    # doc = nlp(text)
    # for ent in doc.ents:
    #     if ent.label_ in ["GPE", "NORP"]:
    #         txt = ent.text.strip()
    #         if txt in DEMONYM_MAPPER:
    #             txt = DEMONYM_MAPPER[txt]
    #         try:
    #             found.add(pycountry.countries.lookup(txt).alpha_2)
    #         except:
    #             continue
    #
    # return list(found)

def extract_locations(text):
    doc = nlp(text)

    locations = []

    for ent in doc.ents:
        if ent.label_ in ["GPE", "LOC"]:
            country, region = resolve_region_and_country(ent.text)
            if country:
                locations.append((country, region))

    return locations

def estimate_tone_fast(text):
    inputs = bert_tokenizer(text, return_tensors="pt", truncation=True)
    with torch.no_grad():
        probs = torch.softmax(bert_model(**inputs).logits, dim=1).detach().numpy()[0]
    return float(probs[0] - probs[1])


def adjust_sentiment_rules(text, tone):
    text_lower = text.lower()
    if any(w in text_lower for w in TRADE_BARRIERS):
        if any(v in text_lower for v in ["impose", "hike", "raise", "increase", "against"]):
            return -2.0
    return tone

def is_valid_location(country_code, state):
    if not country_code:
        return False
    if state and len(state) < 3:
        return False
    return True


# ==========================================
# 4. CORE EXECUTION FUNCTION (Stateful Pipeline)
# ==========================================

def get_india_business_verdict(text, news_url=None):
    from datetime import datetime

    final_results = []
    current_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    # 1. Extract sectors
    sectors = extract_sectors_multi(text)
    sectors = [s for s in sectors if s != "Misc"]
    if not sectors:
        return []

    # 2. Resolve locations (country + state)
    locations = list(extract_locations(text))
    if not locations:
        locations = [("IN", None)]  # India fallback

    # 3. Sentiment
    base_tone = estimate_tone_fast(text)
    adj_tone = adjust_sentiment_rules(text, base_tone)

    # 4. Core execution
    for country_code, state in locations:
        state = state if state else "ALL"
        if not is_valid_location(country_code, state):
            continue

        for sector in sectors:

            # ---- Save event & compute trends ----
            tone_7d, tone_30d, real_trend = save_and_get_trends(
                country=country_code,
                state=state,
                sector=sector,
                tone=adj_tone,
                headline=text
            )

            # ---- Build ML input (MATCHES TRAINING SCHEMA) ----
            input_df = pd.DataFrame([{
                "Country_Code": country_code,
                "MacroSector": sector,
                "n_articles": 1,
                "tone_mean": adj_tone,
                "pos_rate": 1.0 if adj_tone > 0 else 0.0,
                "neg_rate": 1.0 if adj_tone < 0 else 0.0,
                "tone_7d": tone_7d,
                "tone_30d": tone_30d,
                "tone_trend": real_trend
            }])

            # ---- IFI Prediction ----
            score = float(ifi_model.predict(input_df)[0])

            if score > 0.05:
                verdict = "YES"
            elif score < -0.05:
                verdict = "NO"
            else:
                verdict = "MAYBE"

            # ---- Persist latest score ----
            update_final_score(
                country_code=country_code,
                state=state,
                sector=sector,
                score=score,
                verdict=verdict
            )

            # ---- Console Output ----
            region_label = f"{country_code}/{state}"
            print(
                f"INDIA → {region_label} | {sector} | {verdict} | IFI: {score:.4f}"
            )

            # ---- Collect result ----
            final_results.append((
                current_time,
                text,
                verdict,
                score,
                sector,
                country_code,
                state
            ))

    return final_results
