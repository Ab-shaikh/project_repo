#!/bin/bash

# Go to project root
cd ~/project_repo

# Create dbt folder structure
mkdir -p dbt/models/staging
mkdir -p dbt/models/marts

# Create dbt_project.yml
cat > dbt/dbt_project.yml <<EOL
name: my_dbt_project
version: '1.0'
config-version: 2

profile: default

model-paths: ["models"]
target-path: "target"
clean-targets:
  - "target"
  - "dbt_modules"
EOL

# Create profiles.yml using .env variables
cat > dbt/profiles.yml <<EOL
default:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: "\${SNOWFLAKE_ACCOUNT}"
      user: "\${SNOWFLAKE_USER}"
      password: "\${SNOWFLAKE_PASSWORD}"
      role: "\${SNOWFLAKE_ROLE}"
      warehouse: "\${SNOWFLAKE_WAREHOUSE}"
      database: "\${SNOWFLAKE_DATABASE}"
      schema: "\${SNOWFLAKE_SCHEMA}"
EOL

# Create example SQL models
cat > dbt/models/staging/stg_example.sql <<EOL
-- Example staging model
with raw as (
    select * from {{ source('raw_schema', 'raw_table') }}
)
select * from raw;
EOL

cat > dbt/models/marts/mart_example.sql <<EOL
-- Example transformation model
select *
from {{ ref('stg_example') }}
where some_column is not null;
EOL

echo "✅ dbt folder structure and example files generated successfully!"
