 
@echo off
mkdir logs data\temp
echo %DATE% %TIME%: Setup started >> logs\pipeline.log
if not exist data\ecommerce.db (
    python -c "import sqlite3; conn = sqlite3.connect('data/ecommerce.db'); conn.close()"
    echo %DATE% %TIME%: Created SQLite DB >> logs\pipeline.log
)
echo %DATE% %TIME%: Setup done >> logs\pipeline.log