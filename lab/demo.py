import tempfile,json
from pathlib import Path
from .source_db import create,mutate,connect
with tempfile.TemporaryDirectory() as d:
    p=create(Path(d)/'source.sqlite3');mutate(p);mutate(p,'unlink');c=connect(p)
    print(json.dumps([dict(x) for x in c.execute('SELECT * FROM changes ORDER BY txid,ordinal')],ensure_ascii=False,indent=2));c.close()
