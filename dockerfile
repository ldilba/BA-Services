FROM python:3.10-slim

WORKDIR /app

ADD . /app

RUN pip install -r requirements.txt

EXPOSE 80

CMD ["python", "text_similarity.py"]