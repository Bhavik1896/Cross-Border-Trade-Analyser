from flask import Flask, jsonify
from sqlalchemy import create_engine, text
import pandas as pd

app = Flask(__name__)

# --- Configuration ---
DATABASE_URL = "postgresql://user:password@db:5432/gdelt_db"
engine = create_engine(DATABASE_URL)


@app.route('/')
def home():
    return "GDELT Investment Intelligence API is running."


@app.route('/api/v1/scores')
def get_latest_scores():
    """Returns the latest calculated IFI scores for all sectors/countries."""
    try:
        # Reads directly from the final score table updated by the ML script
        query = "SELECT country_code, sector, ifi_score, verdict, last_updated FROM latest_scores ORDER BY last_updated DESC"
        df = pd.read_sql(query, engine)

        # Convert to JSON
        return jsonify(df.to_dict(orient='records'))

    except Exception as e:
        return jsonify({"error": "Database query failed", "details": str(e)}), 500


@app.route('/api/v1/history/<country_code>')
def get_recent_history(country_code):
    """Returns the 50 most recent raw news events for a specific country."""
    try:
        query = text(f"""
            SELECT headline, tone, timestamp 
            FROM news_history 
            WHERE country_code = :c
            ORDER BY timestamp DESC LIMIT 50;
        """)
        df = pd.read_sql(query, engine, params={"c": country_code.upper()})

        return jsonify(df.to_dict(orient='records'))

    except Exception as e:
        return jsonify({"error": "Database query failed", "details": str(e)}), 500


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
