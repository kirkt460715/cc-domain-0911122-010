import tempfile,unittest
from pathlib import Path
from lab.source_db import create,connect,mutate,prune
class DatabaseSource(unittest.TestCase):
    def test_read_snapshot_and_transaction_log(self):
        with tempfile.TemporaryDirectory() as d:
            p=create(Path(d)/'source.sqlite3');reader=connect(p);reader.execute('BEGIN')
            self.assertEqual(reader.execute('SELECT MAX(txid) FROM transactions').fetchone()[0],1)
            self.assertEqual(mutate(p),2)
            self.assertEqual(reader.execute('SELECT name FROM institutions').fetchone()[0],'合成研究所')
            reader.execute('COMMIT');self.assertEqual(reader.execute('SELECT COUNT(*) FROM changes WHERE txid=2').fetchone()[0],3);reader.close()
            prune(p,2);c=connect(p);self.assertEqual(c.execute('SELECT COUNT(*) FROM changes WHERE txid=1').fetchone()[0],0);c.close()
    def test_relation_delete_preserves_parent(self):
        with tempfile.TemporaryDirectory() as d:
            p=create(Path(d)/'source.sqlite3');mutate(p,'unlink');c=connect(p)
            self.assertEqual(c.execute('SELECT COUNT(*) FROM achievements').fetchone()[0],1);self.assertEqual(c.execute('SELECT COUNT(*) FROM authors').fetchone()[0],0);c.close()
