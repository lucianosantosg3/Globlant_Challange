# Use the official Python image from the Docker Hub
FROM python:3.8-slim

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install any needed packages specified in requirements.txt
# If you have a requirements.txt file, uncomment the following line
 RUN pip install --no-cache-dir -r requirements.txt

# Run the Python script when the container launches
# CMD ["python", "test_BigQueryAPI.py"]
CMD ["bash", "-c", " pytest test_BigQueryAPI.py && python BigQueryAPI.py"]