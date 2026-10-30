import hashlib
import json
import os
import re
import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse, urlunparse

import requests
from cryptography.fernet import Fernet, InvalidToken
from flask import Flask, jsonify, render_template, request, send_file
from hdfs import InsecureClient
from requests.adapters import HTTPAdapter
from werkzeug.utils import secure_filename

BASE_DIR = Path(__file__).resolve().parent
KEY_FILE = Path(os.getenv('KEY_FILE', str(BASE_DIR / 'data' / 'fernet.key')))
HDFS_URL = os.getenv('HDFS_URL', 'http://localhost:9870').rstrip('/')
HDFS_USER = os.getenv('HDFS_USER', 'hadoop')
HDFS_ROOT = '/cloud_storage'
BLOCK_SIZE = 1024 * 1024
MAX_UPLOAD_MB = int(os.getenv('MAX_UPLOAD_MB', '100'))

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = MAX_UPLOAD_MB * 1024 * 1024

class DataNodeRedirectAdapter(HTTPAdapter):
    """Map WebHDFS DataNode redirects onto the host-published Docker port.

    Hadoop often advertises an ephemeral container ID instead of `datanode`.
    The Windows host cannot resolve that internal Docker hostname.
    """
    def send(self, req, **kwargs):
        response = super().send(req, **kwargs)
        if response.is_redirect:
            target = response.headers.get('Location', '')
            parsed = urlparse(target)
            datanode_port = int(os.getenv('HDFS_DATANODE_PORT', '9864'))
            reachable_hostname = os.getenv('HDFS_DATANODE_HOST', 'localhost')
            if (parsed.scheme in ('http', 'https')
                    and parsed.port == datanode_port
                    and parsed.path.startswith('/webhdfs/v1/')
                    and parsed.hostname != reachable_hostname):
                host = reachable_hostname
                if ':' in host and not host.startswith('['):
                    host = f'[{host}]'
                response.headers['Location'] = urlunparse(
                    parsed._replace(netloc=f'{host}:{datanode_port}')
                )
        return response

session = requests.Session()
session.mount('http://', DataNodeRedirectAdapter())
client = InsecureClient(HDFS_URL, user=HDFS_USER, session=session, timeout=10)


def load_key():
    configured = os.getenv('ENCRYPTION_KEY')
    if configured:
        return configured.encode('ascii')
    KEY_FILE.parent.mkdir(parents=True, exist_ok=True)
    try:
        with KEY_FILE.open('xb') as output:
            if os.name != 'nt':
                os.chmod(KEY_FILE, 0o600)
            output.write(Fernet.generate_key())
    except FileExistsError:
        pass
    return KEY_FILE.read_bytes().strip()

cipher = Fernet(load_key())


def ensure_dirs():
    client.makedirs(f'{HDFS_ROOT}/objects')
    client.makedirs(f'{HDFS_ROOT}/files')


def read_metadata(file_id):
    if not re.fullmatch(r'[0-9a-f]{32}', file_id):
        return None
    path = f'{HDFS_ROOT}/files/{file_id}.json'
    if not client.status(path, strict=False):
        return None
    with client.read(path) as reader:
        meta = json.loads(reader.read().decode('utf-8'))
    if meta.get('id') != file_id or meta.get('status') != 'complete':
        return None
    return meta


def public_metadata(meta):
    return {k: meta[k] for k in ('id', 'name', 'size', 'uploaded_at', 'block_count', 'sha256')}


@app.errorhandler(413)
def too_large(_error):
    return jsonify(error=f'File exceeds {MAX_UPLOAD_MB} MB upload limit'), 413


@app.get('/')
def index():
    return render_template('index.html')


@app.post('/upload')
def upload():
    uploaded = request.files.get('file')
    if uploaded is None or not uploaded.filename:
        return jsonify(error='Please select a file.'), 400

    # Reject directory traversal and sanitize the remaining file name.
    original = uploaded.filename.replace('\\', '/')
    if '/' in original or original in ('.', '..'):
        return jsonify(error='Invalid filename.'), 400
    name = secure_filename(original)
    if not name or len(name) > 200:
        return jsonify(error='Invalid or excessively long filename.'), 400

    file_id = uuid.uuid4().hex
    object_path = f'{HDFS_ROOT}/objects/{file_id}'
    digest = hashlib.sha256()
    size = 0
    blocks = 0
    try:
        ensure_dirs()
        client.makedirs(object_path)
        while True:
            chunk = uploaded.stream.read(BLOCK_SIZE)
            if not chunk:
                break
            digest.update(chunk)
            size += len(chunk)
            # Fernet encrypts and authenticates each block independently.
            with client.write(f'{object_path}/{blocks:08d}.bin', overwrite=False) as writer:
                writer.write(cipher.encrypt(chunk))
            blocks += 1

        meta = {
            'id': file_id, 'name': name, 'size': size,
            'uploaded_at': datetime.now(timezone.utc).isoformat(),
            'block_count': blocks, 'sha256': digest.hexdigest(),
            'status': 'complete', 'format_version': 1
        }
        # Publish metadata last so incomplete uploads are not listed.
        tmp_meta = f'{object_path}/metadata.pending.json'
        with client.write(tmp_meta, overwrite=False) as writer:
            writer.write(json.dumps(meta).encode('utf-8'))
        client.rename(tmp_meta, f'{HDFS_ROOT}/files/{file_id}.json')
        return jsonify(message='File encrypted and uploaded successfully.', file=public_metadata(meta)), 201
    except Exception:
        # An incomplete upload has no published metadata. Remove its orphaned chunks.
        try:
            client.delete(object_path, recursive=True)
        except Exception:
            pass
        app.logger.exception('Upload failed')
        return jsonify(error='Upload failed. Check the server logs and HDFS connection.'), 503


@app.get('/files')
def files():
    try:
        ensure_dirs()
        result = []
        for name in client.list(f'{HDFS_ROOT}/files'):
            if not re.fullmatch(r'[0-9a-f]{32}\.json', name):
                continue
            try:
                metadata = read_metadata(name[:-5])
                if metadata:
                    result.append(public_metadata(metadata))
            except Exception:
                app.logger.warning('Skipping malformed metadata: %s', name)
        result.sort(key=lambda x: x['uploaded_at'], reverse=True)
        return jsonify(files=result, count=len(result), logical_bytes=sum(f['size'] for f in result))
    except Exception:
        app.logger.exception('Listing failed')
        return jsonify(error='Unable to list HDFS files.'), 503


@app.get('/download/<file_id>')
def download(file_id):
    tmp_path = None
    successful = False
    try:
        meta = read_metadata(file_id)
        if not meta:
            return jsonify(error='File not found.'), 404
        digest = hashlib.sha256()
        total = 0
        with tempfile.NamedTemporaryFile(delete=False, prefix='hdfs-download-') as tmp:
            tmp_path = tmp.name
            for idx in range(meta['block_count']):
                path = f'{HDFS_ROOT}/objects/{file_id}/{idx:08d}.bin'
                with client.read(path) as reader:
                    plain = cipher.decrypt(reader.read())
                digest.update(plain)
                total += len(plain)
                tmp.write(plain)
        if total != meta['size'] or digest.hexdigest() != meta['sha256']:
            return jsonify(error='File integrity verification failed; download blocked.'), 409
        response = send_file(tmp_path, as_attachment=True, download_name=meta['name'], mimetype='application/octet-stream')
        # Flask's response uses the temp file after the view returns.
        @response.call_on_close
        def cleanup():
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
        successful = True
        return response
    except (InvalidToken, ValueError, KeyError):
        return jsonify(error='Decryption failed or file metadata is invalid.'), 409
    except Exception:
        app.logger.exception('Download failed')
        return jsonify(error='Download failed; HDFS may be unavailable or a block is missing.'), 503
    finally:
        if tmp_path and not successful:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass


@app.delete('/files/<file_id>')
def delete_file(file_id):
    try:
        meta = read_metadata(file_id)
        if not meta:
            return jsonify(error='File not found.'), 404
        object_path = f'{HDFS_ROOT}/objects/{file_id}'
        if client.status(object_path, strict=False):
            client.delete(object_path, recursive=True)
        # Metadata is removed only after the data is successfully deleted.
        client.delete(f'{HDFS_ROOT}/files/{file_id}.json')
        return jsonify(message='File deleted successfully.')
    except Exception:
        app.logger.exception('Deletion failed')
        return jsonify(error='Deletion failed. Check HDFS availability.'), 503


@app.get('/health')
def health():
    try:
        response = requests.get(f'{HDFS_URL}/jmx', params={
            'qry': 'Hadoop:service=NameNode,name=FSNamesystemState'
        }, timeout=5)
        response.raise_for_status()
        beans = response.json().get('beans', [])
        if not beans:
            raise ValueError('NameNode JMX bean unavailable')
        state = beans[0]
        response = requests.get(f'{HDFS_URL}/jmx', params={
            'qry': 'Hadoop:service=NameNode,name=FSNamesystem'
        }, timeout=5)
        response.raise_for_status()
        filesystem = response.json().get('beans', [])
        fs = filesystem[0] if filesystem else {}
        return jsonify(
            status='online', namenode='connected',
            live_datanodes=int(state.get('NumLiveDataNodes', 0)),
            dead_datanodes=int(state.get('NumDeadDataNodes', 0)),
            capacity_total=int(fs.get('CapacityTotal', 0)),
            capacity_used=int(fs.get('CapacityUsed', 0)),
            capacity_remaining=int(fs.get('CapacityRemaining', 0)),
            capacity_available=bool(filesystem),
            checked_at=datetime.now(timezone.utc).isoformat()
        )
    except Exception:
        return jsonify(status='offline', namenode='disconnected', error='NameNode JMX unavailable'), 503


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=False)
