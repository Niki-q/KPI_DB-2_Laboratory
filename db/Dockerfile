FROM postgres:latest

# Copy custom postgresql.conf into container
COPY postgresql.conf /etc/postgresql/postgresql.conf

# Expose PostgreSQL port
EXPOSE 5432

# Start PostgreSQL
CMD ["postgres", "-c", "config_file=/etc/postgresql/postgresql.conf"]

