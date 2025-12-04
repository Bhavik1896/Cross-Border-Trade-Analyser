# 1. Use Official Flink (It automatically detects M1 and pulls the ARM64 version)
FROM flink:1.20-java11

# 2. Switch to root
USER root

# 3. Install System Dependencies (Crucial for M1 Macs)
RUN apt-get update -y && \
    apt-get install -y \
    python3 \
    python3-pip \
    python3-dev \
    build-essential \
    libssl-dev \
    libffi-dev \
    && ln -s /usr/bin/python3 /usr/bin/python

# 4. Upgrade Pip
RUN python3 -m pip install --upgrade pip

# 5. Install Python Libraries
# We ignore installed packages to prevent conflicts with Flink's pre-installed libs
RUN pip3 install --no-cache-dir --ignore-installed \
    notebook \
    jupyter \
    ipykernel \
    pandas \
    polars \
    pyarrow \
    torch \
    transformers \
    spacy \
    pycountry \
    xgboost \
    scikit-learn \
    kafka-python \
    joblib \
    "numpy<2.0"

# 6. Download Spacy Model
RUN python3 -m spacy download en_core_web_sm

# 7. Set Working Directory
WORKDIR /opt/project