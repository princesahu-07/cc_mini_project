# CloudVault — Flask + Hadoop HDFS

A lab-ready single-controller cloud file manager. Supports encrypted chunked uploads, downloads, deletion, checksums, live NameNode/DataNode metrics, and metadata-backed file lists.

## Quick start (Windows / Linux / macOS)

1. Install Docker Desktop (or Docker Engine), Python 3.10+, and enable Docker.
2. In this directory: `docker compose up -d`
3. Wait for NameNode & DataNode startup; check `http://localhost:9870` and `docker compose ps`.
4. Create a virtual environment and install dependencies:
   - Windows PowerShell: `py -m venv .venv; .\.venv\Scripts\Activate.ps1; pip install -r requirements.txt`
   - Linux/macOS: `python3 -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt`
5. Run `python app.py`.
6. Open **http://localhost:5000**. Upload a file, download it, delete it, and observe monitoring cards.

## Crucial encryption-key warning

On first start, the controller creates `data/fernet.key`. **Back up this file securely** and do not delete it (or set a stable `ENCRYPTION_KEY` before the first upload). Files written with a previous temporary key from the original program are not decryptable using the new key; if you retained that key, set `ENCRYPTION_KEY` to it before starting. Never commit or share keys.

## Data layout

- `/cloud_storage/objects/<uuid>/00000000.bin`, etc. — encrypted file chunks
- `/cloud_storage/files/<uuid>.json` — committed metadata written last

Upload creates chunks first, then publishes the metadata. Incomplete uploads are not shown in the dashboard. UUIDs allow same-named uploads. The original SHA-256 and size are verified before a downloaded file is returned. Empty files are supported. Deletions remove chunks then metadata.

## LAN testing

The Flask app listens on `0.0.0.0:5000`. Allow inbound TCP 5000 only from your **trusted lab LAN** using your operating-system firewall. On a second computer, browse to `http://<server-LAN-IP>:5000`. The frontend uses **relative API paths**, so it works over LAN. Hadoop port 9870 and DataNode port 9864 should not be exposed to untrusted networks. For an actual multi-host Hadoop cluster you must configure reachable NameNode/DataNode addresses and network routing; this Compose example starts both nodes on a single Docker host.

## Security limitations

This is a **trusted-laboratory demo, not production SaaS**. No authentication or user isolation is implemented; anyone who can reach port 5000 can list, download, upload, or delete files. Plain HTTP on LAN does not protect data in transit. For production use: HTTPS, login/authorization, CSRF protection or bearer auth, rate limiting, secure secrets, audit logging, quotas, and HDFS access controls. Fernet encrypts content but metadata filenames remain visible. Avoid using sensitive files in an untrusted network.

## Metrics

`GET /health` reads the NameNode JMX `FSNamesystemState` and `FSNamesystem` beans. Capacity numbers are **whole-cluster HDFS physical metrics**; the dashboard's 'Your files' value is the **sum of original plaintext sizes**. They will differ due to block storage overhead, metadata, and replication. Metrics refresh every 10 seconds; file list refreshes every 30 seconds.

## API

- `POST /upload` multipart field `file`
- `GET /files` returns committed files and logical size
- `GET /download/<file_id>` verifies SHA-256 and downloads a file
- `DELETE /files/<file_id>` deletes a file
- `GET /health` reports JMX cluster metrics

## Existing data migration

Old files named `/cloud_storage/<filename>_block_0` from the earlier implementation are **not automatically migrated**. They require their original encryption key and a one-time migration tool to re-upload or convert. This version uses a new storage layout and metadata format. Do not delete existing HDFS Docker volumes while migrating.

## Troubleshooting

- `GET /health` offline: inspect NameNode at localhost:9870, container logs, and Docker status.
- WebHDFS redirect to Docker name fails: `HDFS_DATANODE_DOCKER_HOST=datanode` (default) is remapped to `HDFS_DATANODE_HOST=localhost` for the backend on the Docker host. For a different topology, configure this address correctly.
- Download decryption error: verify the **original** Fernet key is configured.
- DataNode shows 0: wait for registration, check `docker compose logs datanode`, and confirm HDFS configuration.
