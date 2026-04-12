FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    HOST=0.0.0.0 \
    PORT=8080

WORKDIR /app

COPY local_db_api.py clean_lead_master_data.py api_handler_*.py ./

EXPOSE 8080

CMD ["python3", "local_db_api.py"]
