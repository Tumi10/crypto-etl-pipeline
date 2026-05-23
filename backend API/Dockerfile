FROM python:3.12-slim

WORKDIR /app

COPY . .

RUN pip install fastapi uvicorn psycopg2-binary

EXPOSE 8000

CMD ["uvicorn", "api:app", "--host", "0.0.0.0", "--port", "8000"]