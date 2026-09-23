import sys, logging, argparse, os, psutil, subprocess, textwrap, html, io, queue
from flask import Flask, render_template_string, request, jsonify, Response
from urllib.parse import quote, unquote
from bs4 import BeautifulSoup
from flask_sock import Sock
from pathlib import Path
# pip install psutil flask flask_sock bs4 pygments

parser = argparse.ArgumentParser()
parser.add_argument("db", type=str, nargs='?', default='data.db')
parser.add_argument("-p", "--port", type=int, default=5000)
argv = vars(parser.parse_args())
pycache = Path(__file__).parent / '__pycache__'
os.makedirs(pycache, exist_ok=True)
CURR_DB = argv['db']


if '=== IPC Autosave ===':
    import sqlite3, threading, json, re, signal, atexit, time
    from pathlib import Path

    data_path = Path(CURR_DB).resolve()
    save_path = data_path.parent / 'save.db'
    restore = not save_path.exists()

    data_base = sqlite3.connect(data_path, check_same_thread=False, timeout=10)
    data_base.execute("PRAGMA synchronous=NORMAL")
    data_base.execute("PRAGMA temp_store=MEMORY")
    data_base.execute("""
        CREATE TABLE IF NOT EXISTS exp (
            key TEXT PRIMARY KEY,
            value BLOB,
            meta TEXT DEFAULT '{}'
        )
    """)
    try:
        data_base.execute("ALTER TABLE exp ADD COLUMN meta TEXT DEFAULT '{}'")
        data_base.commit()
    except: pass

    base = sqlite3.connect(save_path, check_same_thread=False)
    base.execute("PRAGMA journal_mode=WAL")
    base.execute("PRAGMA synchronous=NORMAL")
    base.execute("PRAGMA temp_store=MEMORY")
    base.execute("""
        CREATE TABLE IF NOT EXISTS exp (
            key TEXT PRIMARY KEY,
            value TEXT,
            updated_at REAL DEFAULT 0
        )
    """)
    try:
        base.execute("ALTER TABLE exp ADD COLUMN updated_at REAL DEFAULT 0")
        base.commit()
    except: pass

    if restore:
        rows = data_base.execute(
            "SELECT key, value FROM exp WHERE key NOT LIKE '%.%' AND key != '__dirty'"
        ).fetchall()
        now = time.time()
        for key, value in rows:
            text = value.decode('utf-8') if isinstance(value, (bytes, bytearray)) else value
            base.execute("INSERT OR REPLACE INTO exp VALUES (?, ?, ?)", (key, text, now))
        base.commit()

    file_keys = set(
        row[0] for row in data_base.execute("SELECT key FROM exp WHERE key LIKE '%.%'").fetchall()
    )


    lock = threading.Lock()
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
    @atexit.register
    def _close_db():
        got = lock.acquire(timeout=2)
        try:
            base.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        except Exception: pass

        if got: lock.release()
        base.close()

    _SENTINEL = object()
    def db(key=None, value=_SENTINEL, commit=None):
        with lock:
            if commit:
                base.commit()
                data_base.commit()
                return

            if key in (None, '*'):
                rows = base.execute("SELECT key FROM exp").fetchall()
                return [row[0] for row in rows if row[0] not in {'point', '__dirty'}]

            if value is _SENTINEL:
                row = base.execute("SELECT value FROM exp WHERE key=?", (key,)).fetchone()
                return row[0] if row else None

            if value is None:
                base.execute("DELETE FROM exp WHERE key=?", (key,))
                data_base.execute("DELETE FROM exp WHERE key=?", (key,))
                if commit is not False:
                    base.commit()
                    data_base.commit()
                return None

            now = time.time()
            base.execute(
                "INSERT INTO exp (key, value, updated_at) VALUES (?, ?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at",
                (key, value, now)
            )
            data_base.execute(
                "INSERT INTO exp (key, value, meta) VALUES (?, ?, '{}') "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, value)
            )
            if commit is not False:
                base.commit()
                data_base.commit()
            return value


    def meta(key=None, value=_SENTINEL):
        if isinstance(key, dict):
            value = key
            key = db("point")
        elif key in ["*", None]: key = db("point")

        with lock:
            if value is _SENTINEL:
                row = data_base.execute("SELECT meta FROM exp WHERE key=?", (key,)).fetchone()
                if row is None:
                    return {}
                try:
                    return json.loads(row[0])
                except (TypeError, json.JSONDecodeError):
                    return {}

            data_base.execute(
                "UPDATE exp SET meta=? WHERE key=?",
                (json.dumps(value, separators=(',', ':')), key)
            )
            data_base.commit()
            
            base.execute(
                "INSERT OR REPLACE INTO exp (key, value, updated_at) VALUES ('__meta', ?, ?)",
                (key, time.time())
            )
            base.commit()
            return value
    

    def _dirty(path):
        with lock:
            row = data_base.execute("SELECT meta FROM exp WHERE key='__dirty'").fetchone()
            try:
                m = json.loads(row[0]) if row and row[0] else {}
            except json.JSONDecodeError:
                m = {}
            lst = m.get('files', [])
            if path not in lst:
                lst.append(path)
            m['files'] = lst
            data_base.execute(
                "INSERT INTO exp (key, value, meta) VALUES ('__dirty', '', ?) "
                "ON CONFLICT(key) DO UPDATE SET meta=excluded.meta",
                (json.dumps(m, separators=(',', ':')),)
            )
            data_base.commit()
    

    SMALL_FILE = 200 * 1024 * 1024  # 200MB
    def store(path=None, value=None):
        if path is None:
            return sorted(file_keys)

        if value is False:
            with lock:
                data_base.execute("DELETE FROM exp WHERE key=?", (path,))
                data_base.commit()
            file_keys.discard(path)
            _dirty(path)
            return None

        if value is None:
            with lock:
                row = data_base.execute("SELECT LENGTH(value), value FROM exp WHERE key=?", (path,)).fetchone()
            if not row or row[0] is None:
                return None

            size, data = row
            if size <= SMALL_FILE:
                return bytes(data)

            def generate_chunks(chunk_size=16384):
                offset = 1
                while offset <= size:
                    chunk = data_base.execute(
                        "SELECT SUBSTR(value, ?, ?) FROM exp WHERE key=?",
                        (offset, chunk_size, path)
                    ).fetchone()[0]
                    if not chunk: break
                    yield chunk
                    offset += chunk_size
            return generate_chunks()

        blob_data = bytearray()
        while True:
            chunk = value.read(16384)
            if not chunk: break
            blob_data.extend(chunk)

        with lock:
            data_base.execute(
                "INSERT INTO exp (key, value, meta) VALUES (?, ?, '{}') "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (path, bytes(blob_data))
            )
            data_base.commit()
        file_keys.add(path)
        _dirty(path)
        return True



if '=== Code Lexer ===':
    from pygments import highlight
    from pygments.formatters import HtmlFormatter
    from pygments.lexer import bygroups, using
    from pygments.lexers.html import HtmlLexer
    from pygments.lexers.python import PythonLexer
    from pygments.token import Punctuation, Text, Name


    class HostHtmlLexer(HtmlLexer):
        tokens = {
            'root': [
                (r'(<)(\s*)(python)(\s*)',
                bygroups(Punctuation, Text, Name.Tag, Text),
                ('python-content', 'tag')),
            ] + HtmlLexer.tokens['root'],

            'python-content': [
                (r'(<)(\s*)(/)(\s*)(python)(\s*)(>)',
                bygroups(Punctuation, Text, Punctuation, Text, Name.Tag, Text,
                        Punctuation), '#pop'),
                (r'.+?(?=<\s*/\s*python\s*>)', using(PythonLexer)),
                (r'.+?\n', using(PythonLexer), '#pop'),
                (r'.+', using(PythonLexer), '#pop'),
            ],
        }

    
    # другие темы: monokai, dracula, gruvbox-dark, nord, one-dark, solarized-dark, github-dark
    _hl_fmt = HtmlFormatter(nowrap=True, style='monokai')
    _hl_lexer = HostHtmlLexer()

    def _hl_style():
        BG, FACTOR = (0x22, 0x22, 0x22), 0.85 # обязательно ли тут менять фон?

        def dim(hexcode):
            r, g, b = (int(hexcode[i:i+2],   16) for i in (0, 2, 4))
            return ''.join(f'{round(c0 + (c - c0) * FACTOR):02x}' for c, c0 in zip((r, g, b), BG))

        def dim_line(line):
            if not line.startswith('#highlight'):
                return line
            return re.sub(r'(?<![-a-zA-Z])color: #([0-9A-Fa-f]{6})',
                        lambda m: f'color: #{dim(m.group(1))}', line)

        css = _hl_fmt.get_style_defs('#highlight')
        css = '\n'.join(dim_line(l) for l in css.split('\n'))
        css += f'\n#highlight .kn {{ color: #{dim("66D9EF")} }}\n'
        css += '\n#highlight .bracket-match { background: #3a3d41; border-radius: 2px; }\n'
        return css

    _hl_css = _hl_style()



if '=== API Host ===':
    logging.getLogger('werkzeug').setLevel(logging.ERROR)
    app = Flask(__name__)
    app.url_map.merge_slashes = False
    ws = Sock(app)
    clients = {}


    SYNC_WATCHER = textwrap.dedent("""\
        import sqlite3, json, time, sys, threading, ctypes, textwrap, signal as _sig
        from pathlib import Path

        _orig_signal = _sig.signal
        def _safe_signal(sig, handler):
            try:
                return _orig_signal(sig, handler)
            except ValueError:
                pass
        _sig.signal = _safe_signal

        conn = sqlite3.connect(sys.argv[1], check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        pycache = Path(sys.argv[2])

        def db_get(key):
            row = conn.execute("SELECT value FROM exp WHERE key=?", (key,)).fetchone()
            return row[0] if row else None

        _real_out = sys.stdout
        _real_err = sys.stderr
        _thread   = None
        _tid      = None

        def _kill():
            global _thread, _tid
            if _thread and _thread.is_alive():
                ctypes.pythonapi.PyThreadState_SetAsyncExc(
                    ctypes.c_ulong(_tid), ctypes.py_object(SystemExit)
                )
                _thread.join(timeout=2)
            sys.stdout = _real_out
            sys.stderr = _real_err
            _thread = _tid = None

        def _run(code, std):
            f = open(std, 'w', encoding='utf-8', buffering=1)
            f.write("")
            sys.stdout = f
            sys.stderr = f
            _globals = {}
            try:
                import linecache
                src = textwrap.dedent(code)
                lines = src.splitlines(keepends=True)
                linecache.cache['<sync>'] = (len(src), None, lines, '<sync>')
                exec(compile(src, '<sync>', 'exec'), _globals)
            except SystemExit:
                pass
            except Exception:
                import traceback, re
                text = traceback.format_exc()
                text = re.sub(r'line (\\d+)', lambda m: f'?≈{int(m.group(1)) - 216}', text)
                f.write(text)
            finally:
                linecache.cache.pop('<sync>', None)
                # закрываем все sqlite-соединения из exec-пространства
                for v in _globals.values():
                    if isinstance(v, sqlite3.Connection):
                        try: v.close()
                        except: pass
                sys.stdout = _real_out
                sys.stderr = _real_err
                f.close()
        
        prev_point = object()
        prev_content = object()
        prev_sync = object()

        while True:
            time.sleep(0.15)
            try:
                sync  = db_get('sync')
                point = db_get('point') or ''
                content = db_get(point) if point else None

                if point == prev_point and content == prev_content and sync == prev_sync:
                    continue

                _kill()
                std = pycache / 'sync.std'
                std.write_text('', encoding='utf-8')
                prev_point = point
                prev_content = content
                prev_sync = sync

                if sync and isinstance(sync, str):
                    _thread = threading.Thread(target=_run, args=(sync, str(std)), daemon=True)
                    _thread.start()
                    _tid = _thread.ident
            except Exception:
                pass\
    """)


    def _watcher():
        last = time.time()
        while True:
            time.sleep(0.1)
            now = time.time()
            with lock:
                rows = base.execute(
                    "SELECT key, value FROM exp WHERE updated_at > ?", (last,)
                ).fetchall()

                drow = data_base.execute("SELECT meta FROM exp WHERE key='__dirty'").fetchone()
                try:
                    dmeta = json.loads(drow[0]) if drow and drow[0] else {}
                except json.JSONDecodeError:
                    dmeta = {}
                files = dmeta.get('files', [])
                if files:
                    data_base.execute("UPDATE exp SET meta='{}' WHERE key='__dirty'")
                    data_base.commit()

                last = now
                if (rows or files) and clients:
                    data = {k: v for k, v in rows}
                    if files:
                        data['__dirty'] = files
                    if data:
                        msg = json.dumps(data)
                        for q in list(clients.values()):
                            q.put(msg)

    threading.Thread(target=_watcher, daemon=True).start()


    @app.route("/")
    def index():
        with open(Path(__file__).parent / 'app.html', 'r', encoding='utf-8') as f:
            return render_template_string(f.read())


    @app.route("/head")
    def head():
        pattern = r"if '=== IPC Auto" + r"save ===':(.*?)if '=== Code " + r"Lexer ===':"
        with open(__file__, 'r', encoding='utf-8') as f:
            src = re.search(pattern, f.read(), re.DOTALL).group(1)
        src = textwrap.dedent(src)
        src = f"CURR_DB = {str(data_path)!r}\n" + src
        return jsonify(f"<python data-ipc>\n{html.escape(src, quote=False)}\n</python>")


    @app.route("/db/", defaults={'key': ''}, methods=['GET', 'POST'])
    @app.route("/db/<string:key>", methods=['GET', 'POST'])
    def data(key):
        if request.method == 'GET':
            return jsonify(db(key))

        value = request.get_data(as_text=True)
        return jsonify(db(key, value if value else None))


    @app.route("/meta/", defaults={'key': ''}, methods=['GET', 'POST'])
    @app.route("/meta/<string:key>", methods=['GET', 'POST'])
    def info(key):
        if request.method == 'GET':
            return jsonify(meta(key))

        value = request.get_json(silent=True)
        if value is None:
            value = {}
        return jsonify(meta(key, value))


    @ws.route("/ws")
    def update(sock):
        q = queue.Queue()
        cid = id(sock)
        clients[cid] = q

        def sender():
            while True:
                msg = q.get()
                if msg is None: break
                try: sock.send(msg)
                except: break
        threading.Thread(target=sender, daemon=True).start()

        with lock:
            rows = base.execute("SELECT key, value FROM exp").fetchall()

        init = {k: v for k, v in rows}
        q.put(json.dumps(init))

        try:
            while True:
                data = json.loads(sock.receive())
                if not isinstance(data, dict): continue
                for key, value in data.items():
                    db(key, value, commit=False)
                if data:
                    with lock:
                        base.commit()
                        data_base.commit()
        except: pass
        finally:
            clients.pop(cid, None)
            q.put(None)


    @app.route("/store/", defaults={'key': ''}, methods=['GET', 'POST'])
    @app.route("/store/<string:key>", methods=['GET', 'POST'])
    def file(key):
        if key == '' and request.method == 'GET':
            return jsonify(store())
        
        if "." not in key:
            return jsonify({"error": "No file path"}), 400
        try:
            if request.method == 'GET':
                chunks = store(key)
                if chunks is None:
                    return jsonify({"error": "File not found"}), 404
                headers = {"Content-Disposition": f"attachment; filename={quote(key)}"}
                return Response(chunks, mimetype='application/octet-stream', headers=headers)

            if request.content_type == 'application/json' and request.get_json(silent=True) is False:
                store(key, False)
                return jsonify({"status": "deleted", "key": key}), 200

            store(key, request.stream)
            return jsonify({"status": "success", "key": key}), 201
        except Exception as e:
            return jsonify({"error": str(e)}), 500


    running_procs = {}
    @app.route("/py/", defaults={'key': ''}, methods=['GET', 'POST'])
    @app.route("/py/<path:key>", methods=['GET', 'POST'])
    def run(key):
        def close(key):
            pid = running_procs.get(key)
            if pid is not None:
                try:
                    psutil.Process(pid).kill()
                except psutil.NoSuchProcess:
                    pass
                running_procs.pop(key, None)

        key = unquote(request.path[len('/py/'):])
        content = request.get_json(silent=True)   
        std = pycache / f"{key}.std"
        # print('', key, content, '', sep=':')

        if content:
            all_lines = content.split('\n')
            result_lines = [''] * len(all_lines)

            for match in re.finditer(r'<python[^>]*>(.*?)</python>', content, re.DOTALL):
                inner_text = textwrap.dedent(html.unescape(match.group(1)))
                line_offset = content[:match.start(1)].count('\n')
                for i, line in enumerate(inner_text.split('\n')):
                    idx = line_offset + i
                    if idx < len(result_lines):
                        result_lines[idx] = line
            code = '\n'.join(result_lines)

            preamble = textwrap.dedent("""\
            import traceback as _tb, re as _re, sys as _sys

            def _handle_exc(n):
                for ln in _tb.format_exc().splitlines():
                    print(_re.sub(r'line (\\d+)', lambda m: f'line {int(m.group(1)) - n}', ln))

            try:
            """)

            m = re.match(r'<!--po:(\d+)-->', content)
            if m:
                offset = int(m.group(1))
                content = content[m.end():]
            else:
                user_match = next(
                    (m for m in re.finditer(r'<python([^>]*?)>(.*?)</python>', content, re.DOTALL)
                    if 'data-ipc' not in m.group(1) and 'sync' not in m.group(1)),
                    None
                )
                offset = content[:user_match.start(2)].count('\n') if user_match else 0

            total_offset = offset + preamble.count('\n') #523

            indented = '\n'.join('    ' + l for l in code.split('\n'))

            suffix = textwrap.dedent(f"""
            except SystemExit:
                pass
            except Exception:
                _handle_exc({total_offset})
            """)

            code = preamble + indented + suffix

            close(key)
            proc = subprocess.Popen(
                [sys.executable, '-u', '-c', code],
                stdout=open(std, 'w', encoding='utf-8', buffering=1),
                stderr=subprocess.STDOUT,
                env=os.environ | {"PYTHONUTF8": "1"}
            )

            running_procs[key] = proc.pid
        elif content == False:
                close(key)
                os.remove(std)

        if not os.path.exists(std):
            return jsonify('')
        with open(std, 'r', encoding='utf-8') as f:
            return jsonify(f.read())
        

    @app.route("/hl.css")
    def hl_css():
        return _hl_css, 200, {'Content-Type': 'text/css'}


    @app.route("/hl", methods=['POST'])
    def hl():
        raw = request.get_json(silent=True) or ""
        cursor = max(raw.find("\ue000"), 0)
        src = raw.replace("\ue000", "")

        n_lead = len(src) - len(src.lstrip("\n"))
        n_trail = len(src) - len(src.rstrip("\n"))
        src_lex = src.strip("\n") + "\n"
        cursor = max(0, min(len(src_lex), cursor - n_lead))

        tokens = list(_hl_lexer.get_tokens(src_lex))

        cand, pos = [], 0
        for i, (_, val) in enumerate(tokens):
            if len(val) == 1 and val in "()[]{}":
                cand.append((pos, i))
            pos += len(val)

        bracket_pair = {'(':')', '[':']', '{':'}', ')':'(', ']':'[', '}':'{'}
        def find_pair(target):
            idx = next((i for off, i in cand if off == target), None)
            if idx is None: return None
            ch = src_lex[target]
            if ch in "([{":
                depth = 0
                for off2, i2 in cand:
                    if off2 < target: continue
                    c2 = src_lex[off2]
                    if c2 == ch: depth += 1
                    elif c2 == bracket_pair[ch]:
                        depth -= 1
                        if depth == 0: return (idx, i2)
            else:
                depth = 0
                for off2, i2 in reversed(cand):
                    if off2 > target: continue
                    c2 = src_lex[off2]
                    if c2 == ch: depth += 1
                    elif c2 == bracket_pair[ch]:
                        depth -= 1
                        if depth == 0: return (i2, idx)
            return None

        match = find_pair(cursor) or (find_pair(cursor - 1) if cursor > 0 else None)

        if match:
            for i in match:
                ttype, val = tokens[i]
                tokens[i] = (ttype, f"\x01{val}\x02")

        buf = io.StringIO()
        _hl_fmt.format(tokens, buf)
        out = buf.getvalue().replace("\x01", '<span class="bracket-match">').replace("\x02", "</span>")

        out = "\n" * n_lead + out.rstrip("\n") + "\n" * n_trail
        return jsonify(out)


    if __name__ == '__main__':
        for f in pycache.glob("*.std"):
            f.unlink(missing_ok=True)
    
        subprocess.Popen(
            [sys.executable, '-u', '-c', SYNC_WATCHER,
            str(Path(argv['db']).resolve()), str(pycache)],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )

        print(f"running on: http://127.0.0.1:{argv['port']}")
        app.run(debug=False, port=argv['port'], threaded=True) #app.run(debug=False, port=argv['port'])

