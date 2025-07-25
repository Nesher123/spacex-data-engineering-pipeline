# de-assignment

Data Engineering Assignment - Trino + PostgreSQL Setup

## Setup instructions

### Prerequisites

- Docker and Docker Compose installed

### Quick Start

1. Navigate to the project directory:

   ```bash
   cd de-assignment
   ```

2. Start the services:

   ```bash
   cd docker
   docker-compose up -d
   ```

3. Verify services are running:

   ```bash
   docker-compose ps
   ```

### Basic Database Testing

```bash
# Test PostgreSQL connection
docker exec -it postgres psql -U postgres -d mydatabase -c "SELECT version();"

# Create sample data
docker exec -it postgres psql -U postgres -d mydatabase -c "
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(100),
    created_at TIMESTAMP DEFAULT NOW()
);

INSERT INTO users (name, email) VALUES 
    ('John Doe', 'john@example.com'),
    ('Jane Smith', 'jane@example.com');
"
```

### Shutdown

```bash
# Stop services
docker-compose down

# Stop services and remove volumes
docker-compose down -v
```

```bash
# Quick PostgreSQL command
docker exec -it postgres psql -U postgres -d mydatabase -c "SELECT COUNT(*) FROM users;"

# Quick Trino command  
docker exec -it trino trino --execute "SELECT COUNT(*) FROM postgresql.public.users;"
```

### References

- Smaran Rai. "Connecting and Running Trino and Postgres in Local Docker Container." *Medium*, 2022. Available at: [https://medium.com/@smaranraialt/connecting-and-running-trino-and-postgres-in-local-docker-container-72878a9eba2c](https://medium.com/@smaranraialt/connecting-and-running-trino-and-postgres-in-local-docker-container-72878a9eba2c)

<!-- Architecture supports future expansion with additional data sources, transformation pipelines, and analytics workloads -->
