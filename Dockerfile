FROM python:3.9-slim

WORKDIR /app

COPY *.py *.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

CMD ["sh", "-c", "python generate_data.py && python export_data.py && python benchmark.py"]