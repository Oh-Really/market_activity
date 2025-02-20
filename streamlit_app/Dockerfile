FROM python:3.9

WORKDIR /app

COPY . .
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir streamlit \
    && pip install --no-cache-dir -r requirements.txt \
    && echo "Checking Streamlit installation..." \
    && pip show streamlit  \
    && which streamlit

ENV PATH="/root/.local/bin:$PATH"

CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]