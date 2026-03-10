#!/bin/bash
# TODO: Remove code duplication

echo "POST_BOOTSTRAP: Starting post-bootstrap script for orders database"

# Create replicator user for replication
echo "POST_BOOTSTRAP: Creating replicator user..."
psql -U postgres -c "CREATE USER replicator WITH REPLICATION PASSWORD 'replpassword';" 2>/dev/null || echo "User replicator already exists"

# Create application user
echo "POST_BOOTSTRAP: Creating application user..."
psql -U postgres -c "CREATE USER \"user\" WITH LOGIN PASSWORD 'password' CREATEROLE CREATEDB;" 2>/dev/null || echo "User 'user' already exists"

# Create database (might already exist from Patroni bootstrap)
echo "POST_BOOTSTRAP: Creating orders database..."
psql -U postgres -c "CREATE DATABASE orders OWNER \"user\";" 2>/dev/null || echo "Database orders already exists"

# Grant privileges to user
echo "POST_BOOTSTRAP: Setting up user permissions..."
psql -U postgres -d orders -c "GRANT ALL PRIVILEGES ON DATABASE orders TO \"user\";"

# Run init.sql
echo "POST_BOOTSTRAP: Running init.sql..."
psql -U postgres -d orders -f /tmp/init.sql

# Grant table privileges
echo "POST_BOOTSTRAP: Granting table privileges..."
psql -U postgres -d orders -c "GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO \"user\";"
psql -U postgres -d orders -c "GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO \"user\";"

echo "POST_BOOTSTRAP: Completed successfully"
exit 0