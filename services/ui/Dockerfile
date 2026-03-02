FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY . /app

ENV AGENT_API_URL=http://agent_api:8002

CMD ["streamlit", "run", "services/ui/app.py", "--server.address=0.0.0.0", "--server.port=8501"]
