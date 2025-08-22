 
$LogFile = "logs/pipeline.log"
New-Item -ItemType Directory -Path logs,data/temp -Force | Out-Null
"$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss'): Setup started" | Out-File $LogFile
if (-not (Test-Path data/ecommerce.db)) {
    python -c "import sqlite3; conn = sqlite3.connect('data/ecommerce.db'); conn.close()"
    "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss'): Created SQLite DB" | Out-File $LogFile -Append
}
"$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss'): Setup done" | Out-File $LogFile -Append