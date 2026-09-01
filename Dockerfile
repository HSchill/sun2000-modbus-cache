FROM python:3.12-alpine
WORKDIR /app
# All three proxy variants ship in the image; select one with the container command
# (see docker-compose.yml). Defaults to the polling server.
COPY modbus_cache_server.py ondemand_modbus_cache_server.py adaptive_modbus_cache_server.py ./
USER nobody
EXPOSE 5502
CMD ["python3", "-u", "modbus_cache_server.py"]
