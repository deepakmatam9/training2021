FROM python:3.8
WORKDIR /myapp
COPY . /myapp
RUN pip install --upgrade pip
RUN pip install -r requirements.txt
EXPOSE 8080
CMD ["gunicorn", "-t", "0","-b", ":8080", "main:app"]