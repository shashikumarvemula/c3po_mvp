# Use a specific Python version image
FROM python:3.11-bookworm AS base

# Set environment variables
ENV LANG en_US.UTF-8
ENV LANG C.UTF-8

# Install necessary dependencies
RUN apt-get -y update --fix-missing && apt-get install -y git

# Install python3-pip
RUN apt-get update && apt-get install -y python3-pip

# Set the working directory to /app
WORKDIR /app

# Copy the requirements.txt into the container and install dependencies
COPY ./requirements.txt /app/requirements.txt
RUN pip3 install --upgrade pip
RUN pip3 install --upgrade setuptools
RUN pip3 install -r requirements.txt --no-cache-dir

# Copy the entire project into the container
COPY . /app

# Clean up unnecessary dependencies to reduce image size
RUN apt-get remove -y git && apt-get clean

EXPOSE 80

CMD ["chainlit","run","main.py","--host","0.0.0.0","--port","80"]