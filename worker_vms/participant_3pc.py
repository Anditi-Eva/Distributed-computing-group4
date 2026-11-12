"""3PC participant entry point; state and data are persisted in PostgreSQL."""
import os

os.environ["TRANSACTION_PROTOCOL"] = "3pc"
from participant_service import app, ensure_schema  # noqa: E402

if __name__ == "__main__":
    import sys
    ensure_schema()
    app.run(host="0.0.0.0", port=int(sys.argv[1]) if len(sys.argv) > 1 else 6001)
