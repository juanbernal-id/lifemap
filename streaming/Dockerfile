FROM python:3.12-slim
WORKDIR /app
RUN pip install --no-cache-dir kafka-python-ng==2.2.3 pymongo==4.8.0 python-dotenv==1.0.1
COPY productor.py .
CMD ["python", "productor.py"]
