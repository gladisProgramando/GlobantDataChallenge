FROM python:3.9
WORKDIR /app

COPY requirements.txt /app/requirements.txt

RUN apt update && apt install -y unixodbc

RUN pip install --no-cache-dir -r requirements.txt

COPY . /app

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]

EXPOSE 8000

#to login: docker login
#to create image: docker build -t fastapi-app . 
#to create the container: docker run -p 8000:8000 --name fastapi-container fastapi-app 