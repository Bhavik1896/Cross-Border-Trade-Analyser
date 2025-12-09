# 1. Start fresh from the Official Flink Image
FROM flink:1.20-java11

# Switch to root
USER root

# 2. INSTALL SYSTEM TOOLS & JDK
# We explicitly install 'openjdk-11-jdk' to get the missing headers for pemja/PyFlink
RUN apt-get update -y && \
    apt-get install -y --no-install-recommends \
    openjdk-11-jdk \
    python3 \
    python3-pip \
    python3-dev \
    build-essential \
    libssl-dev \
    libffi-dev \
    curl \
    git \
    && rm -rf /var/lib/apt/lists/*

# 3. LINK PYTHON
RUN ln -s /usr/bin/python3 /usr/bin/python

# 4. SET JAVA_HOME CORRECTLY
# The base image points to /opt/java/openjdk, which is broken for building.
# We point to the apt-installed JDK which contains the 'include' folder.
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-arm64

# 5. UPGRADE PIP & SETUPTOOLS
RUN pip3 install --upgrade pip setuptools wheel --ignore-installed

# 6. INSTALL LIBRARIES
RUN pip3 install --no-cache-dir --ignore-installed \
    apache-flink==1.20.0 \
    notebook \
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

# 7. Download Spacy Model
RUN python3 -m spacy download en_core_web_sm

# 8. Set working directory
WORKDIR /opt/project