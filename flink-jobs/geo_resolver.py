from geopy.geocoders import Nominatim

geolocator = Nominatim(user_agent="global-risk-intel")

def resolve_region_and_country(place_name):
    try:
        location = geolocator.geocode(place_name, addressdetails=True)
        if not location:
            return None, None

        address = location.raw.get("address", {})

        country_code = address.get("country_code", None)
        region = (
            address.get("state")
            or address.get("region")
            or address.get("province")
        )

        return (
            country_code.upper() if country_code else None,
            region
        )

    except Exception:
        return None, None
