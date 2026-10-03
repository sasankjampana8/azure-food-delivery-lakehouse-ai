# Local text-to-SQL app

Run this after the ADF pipeline succeeds and the three serverless SQL Gold views return rows.

On macOS install [ODBC Driver 18 for SQL Server](https://learn.microsoft.com/en-us/sql/connect/odbc/linux-mac/install-microsoft-odbc-driver-sql-server-macos). Install Azure CLI and sign in to the correct tenant with `az login --tenant <tenant-id>`.

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
# Edit .env locally with the real endpoint, tenant and OpenAI key.
streamlit run app.py
```

Click **Connect and load schema**, ask a question, review the generated SQL, then click **Run query**. The app uses `AzureCliCredential` from your Azure CLI session and makes a new SQL query when you click Run. Do not commit `.env` or data exports. This app is a local prototype; production deployment would require an appropriate workload identity and more strict query controls.
