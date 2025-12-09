import pandas as pd
import joblib
import re
import spacy
import torch
import pycountry
from transformers import AutoTokenizer, AutoModelForSequenceClassification

# ==========================================
# 1. LOAD RESOURCES (Global Scope)
# ==========================================
print("⏳ Loading Models for Real-Time Inference...")

# Path inside Docker container (mapped volume)
MODEL_PATH = "/opt/project/ifi_xgb_model.pkl"

try:
    ifi_model = joblib.load(MODEL_PATH)
except FileNotFoundError:
    # Fallback for local testing if not running in Docker
    ifi_model = joblib.load("ifi_xgb_model.pkl")

# Load NLP Tools
try:
    nlp = spacy.load("en_core_web_sm")
except OSError:
    from spacy.cli import download

    download("en_core_web_sm")
    nlp = spacy.load("en_core_web_sm")

bert_tokenizer = AutoTokenizer.from_pretrained("ProsusAI/finbert")
bert_model = AutoModelForSequenceClassification.from_pretrained("ProsusAI/finbert")

print("✅ All Models Loaded Successfully.")

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
    "Social": ["HUMAN", "GENDER", "CRIME", "UNREST", "PROTEST", "RIGHTS"],
    "Infrastructure": ["ROAD", "RAIL", "URBAN", "INFRA", "BUILDING", "CONSTRUCTION"],
}

DEMONYM_MAPPER = {
    "Russian": "Russia", "Indian": "India", "American": "United States", "Chinese": "China",
    "Japanese": "Japan", "French": "France", "German": "Germany", "British": "United Kingdom"
}

TRADE_BARRIERS = ["tariff", "tax", "sanction", "ban", "embargo", "penalty", "duty", "levy", "restriction", "hike"]


# ==========================================
# 3. HELPER FUNCTIONS
# ==========================================
def extract_sectors_multi(text):
    text_upper = text.upper()
    found = []
    for sector, keywords in SECTOR_GROUPS.items():
        if re.search(r"|".join(re.escape(k) for k in keywords), text_upper):
            found.append(sector)
    return found if found else ["Misc"]


def extract_countries_multi(text):
    doc = nlp(text)
    found = set()
    for ent in doc.ents:
        if ent.label_ in ["GPE", "NORP"]:
            txt = ent.text.strip()
            if txt in DEMONYM_MAPPER: txt = DEMONYM_MAPPER[txt]
            try:
                found.add(pycountry.countries.lookup(txt).alpha_2)
            except:
                continue
    return list(found) if found else ["OTHER"]


def estimate_tone_fast(text):
    inputs = bert_tokenizer(text, return_tensors="pt", truncation=True)
    with torch.no_grad():
        probs = torch.softmax(bert_model(**inputs).logits, dim=1).detach().numpy()[0]
    return float(probs[0] - probs[1])


def adjust_sentiment_rules(text, tone):
    text_lower = text.lower()
    if any(w in text_lower for w in TRADE_BARRIERS):
        if any(v in text_lower for v in ["impose", "hike", "raise", "increase", "against"]):
            return -2.0  # Force negative
    return tone


# ==========================================
# 4. CORE PREDICTION LOGIC
# ==========================================
def predict_raw(text):
    """
    Analyzes text and returns a list of dictionaries with scores.
    """
    sectors = extract_sectors_multi(text)
    countries = extract_countries_multi(text)
    raw_tone = estimate_tone_fast(text)
    adj_tone = adjust_sentiment_rules(text, raw_tone)

    results = []
    for country in countries:
        for sector in sectors:
            # Simulation: If rule triggered (-2.0), force trend to -1.0
            trend = -1.0 if adj_tone == -2.0 else 0.0

            input_df = pd.DataFrame([{
                "Country_Code": country, "MacroSector": sector, "n_articles": 1,
                "tone_mean": adj_tone, "pos_rate": 1.0 if adj_tone > 0 else 0.0,
                "neg_rate": 1.0 if adj_tone < 0 else 0.0,
                "tone_7d": adj_tone, "tone_30d": adj_tone, "tone_trend": trend
            }])

            # Predict using XGBoost
            score = ifi_model.predict(input_df)[0]
            results.append({"Country": country, "Sector": sector, "Score": float(score)})

    return results


def get_india_business_verdict(text):
    """
    Prints the verdict to console (Standard Output).
    Useful for Flink to capture logs.
    """
    print(f"\n📰 NEWS: {text}")
    print("=" * 60)

    try:
        data = predict_raw(text)
    except Exception as e:
        print(f"❌ Error processing text: {e}")
        return

    # Check if India is involved
    all_countries = [d['Country'] for d in data]
    if "IN" not in all_countries:
        print("ℹ️  NOTE: This news does not explicitly mention India.")
        return

    partners = [c for c in all_countries if c != "IN"]
    partner_code = partners[0] if partners else "DOMESTIC"

    if partner_code != "DOMESTIC":
        try:
            partner_name = pycountry.countries.get(alpha_2=partner_code).name.upper()
        except:
            partner_name = partner_code
    else:
        partner_name = "DOMESTIC MARKET"

    india_results = [d for d in data if d['Country'] == "IN"]

    for item in india_results:
        sector = item['Sector']
        score = item['Score']

        print(f"🇮🇳 INDIA vs {partner_name} | 🏭 SECTOR: {sector.upper()}")

        if score > 0.05:
            print(f"   ✅ VERDICT: YES (Recommended) | Score: {score:.4f}")
            print(f"   💡 ACTION:  India SHOULD increase business in {sector}.")
        elif score < -0.05:
            print(f"   ⛔ VERDICT: NO (High Risk) | Score: {score:.4f}")
            print(f"   💡 ACTION:  India SHOULD REDUCE exposure in {sector}.")
        else:
            print(f"   ⚠️ VERDICT: MAYBE (Wait & Watch) | Score: {score:.4f}")
            print(f"   💡 ACTION:  Maintain status quo.")

        print("-" * 60)