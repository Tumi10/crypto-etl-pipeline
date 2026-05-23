FROM python:3.12-slim

WORKDIR /app

COPY dashboard.py .

RUN pip install streamlit requests pandas

EXPOSE 8501

CMD ["streamlit", "run", "dashboard.py", "--server.address", "0.0.0.0"]