import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, text, PrimaryKeyConstraint
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.dialects.postgresql import insert
from datetime import datetime, timedelta
from sqlalchemy import Column, Integer, String, Float, DateTime, PrimaryKeyConstraint
import time
import os
import pycountry

# --- 1. DATABASE CONFIGURATION ---
DATABASE_URL = "postgresql://user:password@db:5432/gdelt_db"
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()





# --- 1. DATABASE TIME CONFIGURATION ---

def utc_now():
    import datetime
    return datetime.datetime.utcnow()


# --- 2. DATABASE SCHEMA ---
class NewsEvent(Base):
    __tablename__ = "news_history"

    timestamp = Column(DateTime,nullable=False,default=lambda: __import__("datetime").datetime.utcnow())

    country_code = Column(String, index=True)
    state = Column(String, index=True, nullable=True)
    sector = Column(String, index=True)
    tone = Column(Float)
    headline = Column(String)
    source_url = Column(String)

    __table_args__ = (
        PrimaryKeyConstraint("timestamp", "country_code", "state", "sector"),
    )



class CountryScore(Base):
    __tablename__ = "latest_scores"

    country_code = Column(String, nullable=False)
    state = Column(String, nullable=True)
    sector = Column(String, nullable=False)

    ifi_score = Column(Float)
    verdict = Column(String)
    last_updated = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        PrimaryKeyConstraint("country_code", "state", "sector"),
    )

# --- 3. STATEFUL LOGIC (Read/Write) ---
print("DEBUG: save_and_get_trends() called")
def save_and_get_trends(country, sector, tone, headline, state=None, news_url = None):
    from datetime import datetime, timedelta
    """Saves event and calculates 30-day trend from TimescaleDB."""
    session = SessionLocal()
    try:
        new_event = new_event = NewsEvent(country_code=country,state=state,sector=sector,tone=tone,headline=headline[:250],source_url=news_url)
        session.add(new_event)
        session.commit()

        thirty_days_ago = datetime.utcnow() - timedelta(days=30)
        query = text("""
                     SELECT tone, "timestamp"
                     FROM news_history
                     WHERE country_code = :c
                       AND sector = :s
                       AND (:state IS NULL OR state = :state)
                       AND "timestamp" >= :d
                     """)

        df = pd.read_sql(
            query,
            engine,
            params={
                "c": country,
                "s": sector,
                "state": state,
                "d": thirty_days_ago
            }
        )
        if df.empty:
            return tone, tone, 0.0

        # 3. Calculate Trends
        tone_30d = df['tone'].mean()
        seven_days_ago = datetime.utcnow() - timedelta(days=7)
        df_7d = df[df['timestamp'] >= seven_days_ago]
        tone_7d = df_7d['tone'].mean() if not df_7d.empty else tone_30d

        real_trend = tone_7d - tone_30d

        return tone_7d, tone_30d, float(real_trend)

    except Exception as e:
        print(f"DB Error in Save/Query: {e}. ROLLING BACK.")
        session.rollback()
        return tone, tone, 0.0
    finally:
        session.close()

print("DEBUG: update_final_score() called")
from sqlalchemy.dialects.postgresql import insert

def update_final_score(country_code, sector, score, verdict, state=None):
    session = SessionLocal()
    try:
        stmt = insert(CountryScore).values(
            country_code=country_code,
            state = state if state else "ALL",
            sector=sector,
            ifi_score=score,
            verdict=verdict,
            last_updated=datetime.utcnow()
        ).on_conflict_do_update(
            index_elements=["country_code", "state", "sector"],
            set_={
                "ifi_score": score,
                "verdict": verdict,
                "last_updated": datetime.utcnow()
            }
        )

        session.execute(stmt)
        session.commit()
    except Exception as e:
        session.rollback()
        print(f"Failed to update latest_scores: {e}")
    finally:
        session.close()



# --- 4. INITIALIZATION ---
def initialize_db(max_retries=10, delay=5):
    for i in range(max_retries):
        try:
            print(f"Attempting DB initialization (Retry {i + 1}/{max_retries})...")

            # 1. Create tables and run Timescale init
            Base.metadata.create_all(bind=engine)

            conn = engine.connect()
            conn.execute(text("CREATE EXTENSION IF NOT EXISTS timescaledb;"))
            conn.execute(text(
                "SELECT create_hypertable('news_history', 'timestamp', if_not_exists => TRUE);"
            ))
            conn.commit()
            conn.close()
            print("TimescaleDB Hypertable Activated & Tables Ready.")
            return

        except Exception as e:
            print(f"DB Startup Failed: {e}. Retrying in {delay}s...")
            time.sleep(delay)

    print("CRITICAL: Failed to initialize database after all retries. Shutting down.")
    exit(1)


initialize_db()