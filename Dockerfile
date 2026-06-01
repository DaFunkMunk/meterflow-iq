FROM python:3.11-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1
ENV STREAMLIT_SERVER_HEADLESS=true
ENV STREAMLIT_BROWSER_GATHER_USAGE_STATS=false
ENV BIGQUERY_PROJECT_ID=project-616f71e8-6bb8-4927-978
ENV BIGQUERY_DATASET_ID=meterflow_iq_curated
ENV BIGQUERY_LOCATION=US

COPY requirements.txt .

RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8080

CMD streamlit run streamlit_app/app.py \
    --server.port=${PORT:-8080} \
    --server.address=0.0.0.0