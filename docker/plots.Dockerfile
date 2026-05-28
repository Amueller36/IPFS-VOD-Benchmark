FROM python:3.12-slim

WORKDIR /app

COPY analysis/requirements.txt /app/analysis/requirements.txt
RUN python -m pip install --no-cache-dir -r /app/analysis/requirements.txt

COPY analysis/plot_runs.py /app/analysis/plot_runs.py
COPY analysis/plot_server.py /app/analysis/plot_server.py

ENV RUNS_DIR=/data/runs
ENV PLOT_SCRIPT=/app/analysis/plot_runs.py
ENV PLOT_SERVER_ADDR=0.0.0.0
ENV PLOT_SERVER_PORT=8090

EXPOSE 8090

CMD ["python", "/app/analysis/plot_server.py"]
