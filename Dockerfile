# Base Image
FROM python:3

# Install build tools
RUN apt-get update && apt-get install -y build-essential

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container
COPY requirements.txt requirements.txt
COPY infrastructure/ infrastructure/
COPY src src
COPY README.md README.md
COPY .env .env

# Install any needed packages specified in requirements.txt
RUN pip install -r requirements.txt
