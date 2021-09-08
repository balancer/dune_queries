# Balancer queries on Dune

SQL queries to support Balancer's dashboards on Dune Analytics.
## Filename standard

`<id>_<descriptive_title>.sql`

## SQL Linter

Install `SQL Formatter` extention on your VS Code to take advantage of linting.
## Batch update

Run the script to fetch Dune queries:

```sh
python scripts/update_queries.py
```

Then, use Format Files (VS Code extension) to lint the SQL code.
