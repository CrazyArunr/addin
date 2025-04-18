FROM python:3.10-slim

# Install system dependencies
RUN apt-get update && \
    apt-get install -y curl gnupg apt-transport-https && \
    curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - && \
    curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list && \
    apt-get update && \
    ACCEPT_EULA=Y apt-get install -y msodbcsql17 unixodbc-dev && \
    apt-get clean

# Set work directory
WORKDIR /app

# Copy files
COPY . .

# Install Python deps
RUN pip install -r requirements.txt

# Start your app
CMD ["python", "app.py"]
