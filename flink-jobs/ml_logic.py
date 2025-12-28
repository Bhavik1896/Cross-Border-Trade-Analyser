import pandas as pd
import joblib
import re
import spacy
import torch
import pycountry
from transformers import AutoTokenizer, AutoModelForSequenceClassification
from datetime import datetime, timedelta

# --- FIX: IMPORT NECESSARY FUNCTIONS & SCHEMAS FROM DB_LOGIC ---
from db_logic import save_and_get_trends, update_final_score, Base, engine, NewsEvent

# ----------------------------------------------------------------

# ==========================================
# 1. LOAD AI MODELS (Global Scope)
# ==========================================
print("⏳ Loading AI Models...")

try:
    ifi_model = joblib.load("/opt/flink/jobs/ifi_xgb_model.pkl")
    nlp = spacy.load("en_core_web_sm")
    bert_tokenizer = AutoTokenizer.from_pretrained("ProsusAI/finbert")
    bert_model = AutoModelForSequenceClassification.from_pretrained("ProsusAI/finbert")
    print("✅ All Models Loaded.")
except FileNotFoundError:
    print("❌ Critical: Models failed to load. Check 'ifi_xgb_model.pkl' path.")
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
COUNTRY_ALIAS_MAP = {
    "US": "United States",
    "U.S.": "United States",
    "USA": "United States",
    "UNITED STATES": "United States",
    "WASHINGTON": "United States",
    "WALL STREET": "United States",
    "FEDERAL RESERVE": "United States",
    "FED": "United States",

    "INDIA": "India",
    "NEW DELHI": "India",
    "DELHI": "India",
    "MODI": "India",
    "GOVERNMENT OF INDIA": "India",

    "CHINA": "China",
    "CHINESE": "China",
    "BEIJING": "China",

    "EU": "European Union",
    "EUROPEAN UNION": "European Union",
    "BRUSSELS": "European Union",
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


def extract_countries_multi(text):
    text_upper = text.upper()
    found = set()

    # ️FIRST: Alias-based detection (very important)
    for alias, country_name in COUNTRY_ALIAS_MAP.items():
        # Word-boundary to avoid false matches (e.g. "us" vs "US")
        if re.search(rf"\b{re.escape(alias)}\b", text_upper):
            try:
                found.add(pycountry.countries.lookup(country_name).alpha_2)
            except:
                # EU or non-standard entities
                if country_name == "European Union":
                    found.add("EU")

    # ️SECOND: SpaCy NER + demonyms
    doc = nlp(text)
    for ent in doc.ents:
        if ent.label_ in ["GPE", "NORP"]:
            txt = ent.text.strip()
            if txt in DEMONYM_MAPPER:
                txt = DEMONYM_MAPPER[txt]
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
# 4. CORE EXECUTION FUNCTION (Stateful Pipeline)
# ==========================================
def get_india_business_verdict(text):
    from datetime import datetime, timedelta
    final_results = []
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # 1. Extract Info
    sectors = extract_sectors_multi(text)
    countries = extract_countries_multi(text)
    adj_tone = adjust_sentiment_rules(text, estimate_tone_fast(text))

    # 2. Check Context (Simplified)
    if "IN" not in countries:
        print("NOTE: This news does not explicitly mention India.")

    # Determine Partner
    partners = [c for c in countries if c != "IN"]
    target_country = partners[0] if partners else "IN"

    partner_name = "DOMESTIC MARKET"
    if partners:
        country_obj = pycountry.countries.get(alpha_2=partners[0])
        if country_obj is not None:
            partner_name = country_obj.name.upper()
        else:
            partner_name = partners[0]  # fallback to raw code

    # 3. Process Logic
    if not sectors:
        sectors = ["Misc"]

    for sector in sectors:
        if sector == "Misc": continue

        # --- STATEFUL STEP: Save to DB & Get Real Trends ---
        # This is where the magic happens: Trend is calculated from TimescaleDB history
        print("DEBUG: Writing record to TimescaleDB")
        tone_7d, tone_30d, real_trend = save_and_get_trends(target_country, sector, adj_tone, text)

        # --- PREPARE INPUT FOR XGBOOST ---
        input_df = pd.DataFrame([{
            "Country_Code": target_country, "MacroSector": sector, "n_articles": 1,
            "tone_mean": adj_tone, "pos_rate": 1.0 if adj_tone > 0 else 0.0,
            "neg_rate": 1.0 if adj_tone < 0 else 0.0,
            "tone_7d": tone_7d, "tone_30d": tone_30d, "tone_trend": real_trend
        }])

        # --- PREDICT & SAVE ---
        score = float(ifi_model.predict(input_df)[0])

        if score > 0.05:
            verdict, action = "YES", f"💡 ACTION:  India SHOULD increase business in {sector}."
        elif score < -0.05:
            verdict, action = "NO", f"💡 ACTION:  India SHOULD REDUCE exposure in {sector}."
        else:
            verdict, action = "MAYBE", f"💡 ACTION:  Maintain status quo."

        # Update the API table
        update_final_score(target_country, sector, score, verdict)

        # --- PRINT ---
        print(f"🇮🇳 INDIA vs {partner_name} | 🏭 SECTOR: {sector.upper()}")
        print(f"   ✅ VERDICT: {verdict} | Score: {score:.4f}")
        print(f"   (Trend: {real_trend:.4f} based on 30-day history)")
        print(f"   {action}")
        print("-" * 60)

        final_results.append((
            current_time,
            text,
            verdict,
            score,
            sector,
            target_country,
            partner_name
        ))

    return final_results  # Return a list of tuples for the sink