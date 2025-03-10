FROM python:3.11-slim

WORKDIR /spain_aribnbs

RUN apt-get update && \
apt-get install -y libpq-dev gcc python3-dev && \
rm -rf /var/lib/apt/lists/*

COPY . /spain_aribnbs

RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 8501

CMD ["streamlit", "run", "app.py"]