import sqlite3,json
from pathlib import Path
DDL="""
PRAGMA journal_mode=WAL;
CREATE TABLE institutions(id TEXT PRIMARY KEY,name TEXT NOT NULL);
CREATE TABLE achievements(id TEXT PRIMARY KEY,title TEXT NOT NULL,institution_id TEXT NOT NULL,kind TEXT NOT NULL,year INTEGER);
CREATE TABLE people(id TEXT PRIMARY KEY,name TEXT NOT NULL);
CREATE TABLE authors(achievement_id TEXT,person_id TEXT,ordinal INTEGER,PRIMARY KEY(achievement_id,person_id));
CREATE TABLE transactions(txid INTEGER PRIMARY KEY,committed_at TEXT NOT NULL);
CREATE TABLE changes(txid INTEGER NOT NULL,ordinal INTEGER NOT NULL,table_name TEXT NOT NULL,operation TEXT NOT NULL,key_json TEXT NOT NULL,before_json TEXT,after_json TEXT,PRIMARY KEY(txid,ordinal));
CREATE TABLE retention(singleton INTEGER PRIMARY KEY CHECK(singleton=1),first_tx INTEGER NOT NULL);
INSERT INTO retention VALUES(1,1);
"""
def connect(path):
    c=sqlite3.connect(path,isolation_level=None);c.row_factory=sqlite3.Row;return c
TABLES={'institutions':['id'],'achievements':['id'],'people':['id'],'authors':['achievement_id','person_id']}
def transaction(c,changes):
    c.execute('BEGIN IMMEDIATE')
    try:
        tx=c.execute('SELECT COALESCE(MAX(txid),0)+1 FROM transactions').fetchone()[0]
        c.execute('INSERT INTO transactions VALUES(?,?)',(tx,'2026-11-01T00:00:00Z'))
        for ordinal,item in enumerate(changes):
            table=item['table'];keys=TABLES[table];key=item['key'];where=' AND '.join(k+'=?' for k in keys);values=[key[k] for k in keys]
            old=c.execute('SELECT * FROM '+table+' WHERE '+where,values).fetchone();before=dict(old) if old else None
            new=item.get('value')
            if new is None:c.execute('DELETE FROM '+table+' WHERE '+where,values)
            else:
                columns=list(new);c.execute('INSERT OR REPLACE INTO '+table+' ('+','.join(columns)+') VALUES('+','.join('?' for _ in columns)+')',[new[k] for k in columns])
            c.execute('INSERT INTO changes VALUES(?,?,?,?,?,?,?)',(tx,ordinal,table,'delete' if new is None else 'upsert',json.dumps(key),json.dumps(before,ensure_ascii=False) if before else None,json.dumps(new,ensure_ascii=False) if new else None))
        c.execute('COMMIT');return tx
    except BaseException:c.execute('ROLLBACK');raise

def create(path):
    c=connect(path);c.executescript(DDL)
    transaction(c,[{'table':'institutions','key':{'id':'org1'},'value':{'id':'org1','name':'合成研究所'}},{'table':'achievements','key':{'id':'a1'},'value':{'id':'a1','title':'合成泵组资料','institution_id':'org1','kind':'dataset','year':2026}},{'table':'people','key':{'id':'p1'},'value':{'id':'p1','name':'人员甲'}},{'table':'authors','key':{'achievement_id':'a1','person_id':'p1'},'value':{'achievement_id':'a1','person_id':'p1','ordinal':1}}]);c.close();return Path(path)
def mutate(path,kind='rename'):
    c=connect(path)
    try:
        if kind=='rename':return transaction(c,[{'table':'institutions','key':{'id':'org1'},'value':{'id':'org1','name':'合成研究所新名'}},{'table':'people','key':{'id':'p2'},'value':{'id':'p2','name':'人员乙'}},{'table':'authors','key':{'achievement_id':'a1','person_id':'p2'},'value':{'achievement_id':'a1','person_id':'p2','ordinal':2}}])
        if kind=='unlink':return transaction(c,[{'table':'authors','key':{'achievement_id':'a1','person_id':'p1'}}])
        if kind=='delete':return transaction(c,[{'table':'achievements','key':{'id':'a1'}}])
        raise ValueError('未知变更')
    finally:c.close()
def prune(path,first):
    c=connect(path);c.execute('BEGIN IMMEDIATE');c.execute('DELETE FROM changes WHERE txid<?',(first,));c.execute('UPDATE retention SET first_tx=?',(first,));c.execute('COMMIT');c.close()
if __name__=='__main__':
    import argparse
    p=argparse.ArgumentParser();p.add_argument('path');p.add_argument('action',choices=['init','rename','unlink','delete','prune']);p.add_argument('--first',type=int,default=2);a=p.parse_args()
    print(create(a.path) if a.action=='init' else prune(a.path,a.first) if a.action=='prune' else mutate(a.path,a.action))
