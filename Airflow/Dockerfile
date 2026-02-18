# Use the same version as your docker-compose file
FROM apache/airflow:3.1.7 

# Copy your requirements file into the container
COPY requirements.txt /requirements.txt

# Install the requirements
# Using --user is standard for the official Airflow image
RUN pip install --no-cache-dir --user -r /requirements.txt