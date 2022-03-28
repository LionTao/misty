FROM python:3.8-slim-buster

WORKDIR /app
COPY . .

RUN pip install -r requirements.txt

ENTRYPOINT ["python"]
CMD ["demo_actor_service.py"]