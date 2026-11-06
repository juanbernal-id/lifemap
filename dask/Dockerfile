FROM python:3.12-slim-bookworm

RUN pip install --no-cache-dir \
    "dask[complete]==2024.8.0" \
    distributed==2024.8.0 \
    dask-expr==1.1.10 \
    pandas==2.2.2 \
    numpy==1.26.4 \
    pyarrow==17.0.0 \
    cloudpickle==3.0.0 \
    msgpack==1.0.8 \
    tornado==6.4.1 \
    toolz==0.12.1 \
    pymongo==4.8.0 \
    python-dotenv==1.0.1 \
    kaggle==1.6.17

WORKDIR /app
