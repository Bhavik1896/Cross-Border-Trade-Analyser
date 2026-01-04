# geo_resolver.py
import pycountry

# --------------------------------------
# Aliases for NLP / human text
# --------------------------------------
COUNTRY_ALIASES = {
    "USA": "United States",
    "U.S.": "United States",
    "UK": "United Kingdom",
    "UAE": "United Arab Emirates",
    "Russia": "Russian Federation",
    "South Korea": "Korea, Republic of",
    "North Korea": "Korea, Democratic People's Republic of",
}

# --------------------------------------
# GDELT / FIPS → ISO-2 normalization
# --------------------------------------
FIPS_TO_ISO = {
    "GM": "DE",   # Germany
    "EI": "IE",   # Ireland
    "SP": "ES",   # Spain
    "RO": "RO",
    "DA": "DK",
    "NO": "NO",
    "MO": "MA",
    "EZ": "CZ",
    "IZ": "IQ",
    "WE": "PS",   # Palestine
    "GZ": "PS",
    "IS": "IL",
}

ISO_TO_NAME_OVERRIDE = {
    "PS": "Palestine",
}

# --------------------------------------
# 1️⃣ Resolve ISO code safely
# --------------------------------------
def resolve_iso_code(text: str) -> str:
    if not text:
        return "GLOBAL"

    text = text.strip()

    if text == "GLOBAL":
        return "GLOBAL"

    # FIPS → ISO
    if text in FIPS_TO_ISO:
        return FIPS_TO_ISO[text]

    # Alias normalization
    text = COUNTRY_ALIASES.get(text, text)

    try:
        country = pycountry.countries.lookup(text)
        return country.alpha_2
    except LookupError:
        return "GLOBAL"


# --------------------------------------
# 2️⃣ ISO → Full country name
# --------------------------------------
def resolve_country_name(code: str) -> str:
    if code == "GLOBAL":
        return "Global"

    iso = FIPS_TO_ISO.get(code, code)

    if iso in ISO_TO_NAME_OVERRIDE:
        return ISO_TO_NAME_OVERRIDE[iso]

    try:
        country = pycountry.countries.get(alpha_2=iso)
        if country:
            return country.name
    except:
        pass

    return iso


# --------------------------------------
# 3️⃣ Public API used by ML / Flink
# --------------------------------------
def resolve_region_and_country(entity_text: str):
    """
    Stream-safe resolver:
    - No state resolution
    - Always returns clean country_code + country_name
    """
    iso_code = resolve_iso_code(entity_text)
    country_name = resolve_country_name(iso_code)

    return {
        "country_code": iso_code,
        "country_name": country_name
    }
