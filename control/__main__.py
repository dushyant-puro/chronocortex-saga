"""Start the local console; no migration, provisioning, lease or provider dispatch."""
import argparse
from pathlib import Path
import uvicorn

from control.app import ControlConfig, create_app
from runtime.action_journal import SQLiteActionJournal


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--journal", required=True)
    parser.add_argument("--port", type=int, default=8877)
    parser.add_argument("--tls-cert")
    parser.add_argument("--tls-key")
    args = parser.parse_args()
    if not Path(args.journal).is_file():
        parser.error("An existing journal is required; initialize/migrate offline first")
    if bool(args.tls_cert) != bool(args.tls_key) or not 1024 <= args.port <= 65535:
        parser.error("Provide both TLS files and a port in 1024..65535")
    config = ControlConfig(origin=f"{'https' if args.tls_cert else 'http'}://127.0.0.1:{args.port}")
    uvicorn.run(create_app(SQLiteActionJournal(args.journal), config), host="127.0.0.1", port=args.port,
                ssl_certfile=args.tls_cert, ssl_keyfile=args.tls_key, proxy_headers=False, access_log=False,
                limit_concurrency=32, timeout_keep_alive=5)


if __name__ == "__main__":
    main()
