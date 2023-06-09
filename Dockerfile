# Use a base image with the necessary dependencies
FROM python:3.9

# Set the working directory
WORKDIR /app

# Copy the code file to the container
COPY . /app

# Install the required dependencies
RUN pip install psycopg2 requests pandas 


CMD ["python", "/app/fetch.py"]`