# Use Python 3.9.7 as the base image
FROM python:3.9.7

# Set the working directory to /app
WORKDIR /app

# Copy the requirements file
COPY requirements.txt /app/requirements.txt

# Install dependencies
RUN pip3 install --no-cache-dir -r /app/requirements.txt

# Copy the rest of the application code
COPY . /app

# Grant execution permissions for the run_script.sh script
RUN chmod +x run_script.sh

# Use the script as the default command
CMD ["./run_script.sh"]