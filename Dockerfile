FROM python:3.12-alpine
WORKDIR /app
COPY modbus_cache_server.py .
USER nobody
EXPOSE 5502
CMD ["python3", "-u", "modbus_cache_server.py"]
