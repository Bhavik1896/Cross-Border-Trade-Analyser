# Base Flink image (already includes PyFlink)
FROM flink:1.20-java11

USER root

# System deps (including openjdk-11-jdk-headless)
RUN apt-get update -y && \
    apt-get install -y --no-install-recommends \
    openjdk-11-jdk-headless \
    python3 \
    python3-pip \
    python3-dev \
    build-essential \
    libssl-dev \
    libffi-dev \
    curl \
    git \
    postgresql-client \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Python symlink
RUN ln -s /usr/bin/python3 /usr/bin/python

# === CRITICAL JNI FIX FOR PEMJA/PYFLINK BUILD (M1/ARM64) ===
# Clear and re-create the JAVA_HOME/include directory.
RUN rm -rf /opt/java/openjdk/include && mkdir -p /opt/java/openjdk/include

# Find the installed JNI headers and copy them to the expected path.
# We try /usr/lib/jvm/default-java first, then the explicit arm64 path, as a robust fix.
RUN JNI_INCLUDES_DIR=$(find /usr/lib/jvm/ -name jni.h -print -quit | xargs dirname) && \
    if [ -d "$JNI_INCLUDES_DIR" ]; then \
        # Copy headers: jni.h, jvm.h, etc.
        cp -r "$JNI_INCLUDES_DIR"/* /opt/java/openjdk/include/ && \
        # Copy architecture-specific headers: jni_md.h (often in a subfolder like /linux)
        JNI_MD_DIR=$(find /usr/lib/jvm/ -name jni_md.h -print -quit | xargs dirname) && \
        if [ -d "$JNI_MD_DIR" ]; then \
            cp -r "$JNI_MD_DIR"/* /opt/java/openjdk/include/ ; \
        fi \
    else \
        echo "Error: JNI headers directory not found. Pip install will fail."; exit 1; \
    fi
# ==========================================================



# Python libs
RUN pip3 install --no-cache-dir \
    apache-flink==1.20.3 \
    pandas \
    polars \
    pyarrow \
    torch \
    transformers \
    spacy \
    pycountry \
    geopy \
    xgboost \
    scikit-learn \
    kafka-python \
    joblib \
    sqlalchemy \
    psycopg2-binary \
    "numpy<2.0" \
    flask \
    gunicorn \
    protobuf \
    # The 'protobuf' package installs the 'google' namespace, fixing ModuleNotFoundError
    # === NEW ADDITIONS END HERE ===
    ruamel.yaml

# Preload models (optional)
RUN python3 - <<EOF
from transformers import AutoTokenizer, AutoModelForSequenceClassification
AutoTokenizer.from_pretrained("ProsusAI/finbert")
AutoModelForSequenceClassification.from_pretrained("ProsusAI/finbert")
EOF

# === JAVA DEPENDENCY ADDITIONS START HERE ===
# 1. Flink Kafka Connector (Already present)
ADD https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/3.3.0-1.20/flink-connector-kafka-3.3.0-1.20.jar \
    /opt/flink/lib/

# 2. Kafka Client dependency (Fixes java.lang.NoClassDefFoundError for OffsetResetStrategy)
ADD https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.3.0/kafka-clients-3.3.0.jar \
    /opt/flink/lib/

# 3. Flink JDBC Connector
ADD https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/3.3.0-1.20/flink-connector-jdbc-3.3.0-1.20.jar \
    /opt/flink/lib/

# 4. PostgreSQL JDBC Driver
ADD https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar \
    /opt/flink/lib/

# === JAVA DEPENDENCY ADDITIONS END HERE ===

# Spacy model
RUN python3 -m spacy download en_core_web_sm

WORKDIR /opt/project