import sqlite3
from pathlib import Path
from urllib.parse import quote
from ..contracts import (
    MeshError,
    bounded_text,
    canonical,
    decode,
    digest,
    fail,
    identifier,
    record_key,
    source_contract,
)

MAPPING_VERSION = 1

TABLES = {
    "institutions": (("id",), ("id", "name")),
    "achievements": (("id",), ("id", "title", "institution_id", "kind", "year")),
    "people": (("id",), ("id", "name")),
    "authors": (
        ("achievement_id", "person_id"),
        ("achievement_id", "person_id", "ordinal"),
    ),
}

EXPORT_TABLES = sorted(TABLES) + ["transactions", "changes", "retention"]

SCHEMA = """
CREATE TABLE IF NOT EXISTS connector_state(
 source TEXT PRIMARY KEY,status TEXT NOT NULL,boundary_txid INTEGER NOT NULL,
 checkpoint_txid INTEGER NOT NULL,generation INTEGER NOT NULL,
 mapping_version INTEGER NOT NULL,failure TEXT);
CREATE TABLE IF NOT EXISTS connector_institutions(
 source TEXT NOT NULL,id TEXT NOT NULL,name TEXT NOT NULL,PRIMARY KEY(source,id));
CREATE TABLE IF NOT EXISTS connector_people(
 source TEXT NOT NULL,id TEXT NOT NULL,name TEXT NOT NULL,PRIMARY KEY(source,id));
CREATE TABLE IF NOT EXISTS connector_achievements(
 source TEXT NOT NULL,id TEXT NOT NULL,title TEXT NOT NULL,institution_id TEXT NOT NULL,
 kind TEXT NOT NULL,year INTEGER,txid INTEGER NOT NULL,PRIMARY KEY(source,id));
CREATE TABLE IF NOT EXISTS connector_authors(
 source TEXT NOT NULL,achievement_id TEXT NOT NULL,person_id TEXT NOT NULL,
 ordinal INTEGER NOT NULL,PRIMARY KEY(source,achievement_id,person_id));
CREATE TABLE IF NOT EXISTS connector_ledger(
 source TEXT NOT NULL,txid INTEGER NOT NULL,kind TEXT NOT NULL,generation INTEGER NOT NULL,
 batch TEXT NOT NULL,records_digest TEXT NOT NULL,mapping_version INTEGER NOT NULL,
 affected TEXT NOT NULL,status TEXT NOT NULL,reason TEXT,PRIMARY KEY(source,txid));
"""


class DatabaseConnector:
    def __init__(self, store, sources):
        self.store = store
        self.sources = sources
        self.store.connection.executescript(SCHEMA)

    def _connect(self, path):
        path = Path(path)
        if path.is_symlink() or not path.is_file():
            fail("path", "export must be a local regular file")
        uri = "file:" + quote(str(path.resolve())) + "?mode=ro"
        try:
            con = sqlite3.connect(uri, uri=True, isolation_level=None)
            con.row_factory = sqlite3.Row
            con.execute("PRAGMA query_only=ON")
            con.execute("SELECT 1")
        except sqlite3.Error as exc:
            fail("source", "export cannot be opened read-only", reason=str(exc))
        return con

    def _check_export_schema(self, con):
        found = {
            row[0]
            for row in con.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        missing = sorted(set(EXPORT_TABLES) - found)
        if missing:
            fail("source", "export schema differs from registration protocol", missing=missing)

    def _read_export(self, path, during_read=None):
        if during_read is not None and not callable(during_read):
            fail("input", "during_read must be a callable rehearsal hook")
        con = self._connect(path)
        try:
            con.execute("BEGIN")
            self._check_export_schema(con)
            boundary = con.execute(
                "SELECT COALESCE(MAX(txid),0) FROM transactions"
            ).fetchone()[0]
            if during_read is not None:
                during_read()
            retention = con.execute(
                "SELECT first_tx FROM retention WHERE singleton=1"
            ).fetchone()
            if retention is None:
                fail("source", "export retention marker missing")
            institutions = {
                row["id"]: row["name"]
                for row in con.execute("SELECT id,name FROM institutions")
            }
            people = {
                row["id"]: row["name"] for row in con.execute("SELECT id,name FROM people")
            }
            achievements = {
                row["id"]: dict(row)
                for row in con.execute(
                    "SELECT id,title,institution_id,kind,year FROM achievements"
                )
            }
            authors = [
                dict(row)
                for row in con.execute(
                    "SELECT achievement_id,person_id,ordinal FROM authors ORDER BY achievement_id,ordinal,person_id"
                )
            ]
            con.execute("COMMIT")
            return {
                "boundary": boundary,
                "first_tx": retention[0],
                "institutions": institutions,
                "people": people,
                "achievements": achievements,
                "authors": authors,
            }
        except sqlite3.Error as exc:
            fail("source", "export cannot be read", reason=str(exc))
        finally:
            con.close()

    def _read_log(self, path, checkpoint):
        con = self._connect(path)
        try:
            con.execute("BEGIN")
            self._check_export_schema(con)
            retention = con.execute(
                "SELECT first_tx FROM retention WHERE singleton=1"
            ).fetchone()
            if retention is None:
                fail("source", "export retention marker missing")
            txids = [
                row[0]
                for row in con.execute(
                    "SELECT txid FROM transactions WHERE txid>? ORDER BY txid",
                    (checkpoint,),
                )
            ]
            grouped = {}
            for row in con.execute(
                "SELECT * FROM changes WHERE txid>? ORDER BY txid,ordinal", (checkpoint,)
            ):
                grouped.setdefault(row["txid"], []).append(dict(row))
            con.execute("COMMIT")
            return retention[0], txids, grouped
        except sqlite3.Error as exc:
            fail("source", "export log cannot be read", reason=str(exc))
        finally:
            con.close()

    @staticmethod
    def _derive(institutions, people, achievements, authors):
        by_achievement = {}
        dangling = 0
        for row in authors:
            if row["achievement_id"] in achievements:
                by_achievement.setdefault(row["achievement_id"], []).append(
                    (row["ordinal"], row["person_id"])
                )
            else:
                dangling += 1
        records = []
        for aid in sorted(achievements):
            achievement = achievements[aid]
            institution = institutions.get(achievement["institution_id"])
            if institution is None:
                fail(
                    "source",
                    "achievement references missing institution",
                    achievement=aid,
                    institution=achievement["institution_id"],
                )
            names = []
            for ordinal, person_id in sorted(by_achievement.get(aid, [])):
                person = people.get(person_id)
                if person is None:
                    fail(
                        "source",
                        "author relation references missing person",
                        achievement=aid,
                        person=person_id,
                    )
                names.append(person)
            records.append(
                {
                    "id": aid,
                    "data": {
                        "title": achievement["title"],
                        "institution": institution,
                        "kind": achievement["kind"],
                        "year": achievement["year"],
                        "people": names,
                    },
                }
            )
        return records, dangling

    def _mirror_maps(self, source):
        institutions = {
            row["id"]: row["name"]
            for row in self.store.rows(
                "SELECT id,name FROM connector_institutions WHERE source=?", (source,)
            )
        }
        people = {
            row["id"]: row["name"]
            for row in self.store.rows(
                "SELECT id,name FROM connector_people WHERE source=?", (source,)
            )
        }
        achievements = {
            row["id"]: row
            for row in self.store.rows(
                "SELECT * FROM connector_achievements WHERE source=?", (source,)
            )
        }
        authors = self.store.rows(
            "SELECT achievement_id,person_id,ordinal FROM connector_authors WHERE source=? ORDER BY achievement_id,ordinal,person_id",
            (source,),
        )
        return institutions, people, achievements, authors

    def _state(self, source):
        row = self.store.one("SELECT * FROM connector_state WHERE source=?", (source,))
        if row is None:
            fail("missing", "connector not initialized for source", source=source)
        return {**row, "failure": decode(row["failure"]) if row["failure"] else None}

    def _set_state(self, source, status, boundary, checkpoint, generation, failure):
        if self.store.depth == 0:
            fail("state", "connector state update requires transaction")
        self.store.connection.execute(
            "INSERT INTO connector_state VALUES(?,?,?,?,?,?,?) ON CONFLICT(source) DO UPDATE SET status=excluded.status,boundary_txid=excluded.boundary_txid,checkpoint_txid=excluded.checkpoint_txid,generation=excluded.generation,mapping_version=excluded.mapping_version,failure=excluded.failure",
            (
                source,
                status,
                boundary,
                checkpoint,
                generation,
                MAPPING_VERSION,
                canonical(failure) if failure is not None else None,
            ),
        )

    def _ledger_row(
        self, source, txid, kind, generation, batch, records_digest, affected, status, reason
    ):
        if self.store.depth == 0:
            fail("state", "connector ledger update requires transaction")
        self.store.connection.execute(
            "INSERT INTO connector_ledger VALUES(?,?,?,?,?,?,?,?,?,?) ON CONFLICT(source,txid) DO UPDATE SET kind=excluded.kind,generation=excluded.generation,batch=excluded.batch,records_digest=excluded.records_digest,mapping_version=excluded.mapping_version,affected=excluded.affected,status=excluded.status,reason=excluded.reason",
            (
                source,
                txid,
                kind,
                generation,
                batch,
                records_digest,
                MAPPING_VERSION,
                canonical(affected),
                status,
                canonical(reason) if reason is not None else None,
            ),
        )

    def _ledger_rows(self, source):
        rows = []
        for row in self.store.rows(
            "SELECT * FROM connector_ledger WHERE source=? ORDER BY txid", (source,)
        ):
            rows.append(
                {
                    **row,
                    "affected": decode(row["affected"]),
                    "reason": decode(row["reason"]) if row["reason"] else None,
                }
            )
        return rows

    def _decode_change(self, row):
        table = row["table_name"]
        if table not in TABLES:
            fail("source", "change references unknown table", table=table, txid=row["txid"])
        operation = row["operation"]
        if operation not in {"upsert", "delete"}:
            fail("source", "change uses unknown operation", operation=operation)
        keys, columns = TABLES[table]
        key = decode(row["key_json"])
        if not isinstance(key, dict) or set(key) != set(keys):
            fail("source", "change key differs from export contract", table=table)
        before = decode(row["before_json"]) if row["before_json"] is not None else None
        after = decode(row["after_json"]) if row["after_json"] is not None else None
        if before is not None and (not isinstance(before, dict) or set(before) != set(columns)):
            fail("source", "change before-image differs from export contract", table=table)
        if operation == "upsert":
            if not isinstance(after, dict) or set(after) != set(columns):
                fail("source", "change after-image differs from export contract", table=table)
        elif after is not None or before is None:
            fail("source", "delete change requires before-image and no after-image")
        return table, operation, key, before, after

    def _check_before(self, source, table, key, before):
        keys, columns = TABLES[table]
        where = " AND ".join(name + "=?" for name in keys)
        row = self.store.one(
            "SELECT * FROM connector_" + table + " WHERE source=? AND " + where,
            [source] + [key[name] for name in keys],
        )
        mirror = {name: row[name] for name in columns} if row else None
        if mirror != before:
            fail(
                "integrity",
                "change-log before-image differs from connector mirror",
                table=table,
                key=key,
                mirror=mirror,
                logged=before,
            )

    def _apply_change(self, con, source, table, operation, key, after, affected, txid):
        keys, columns = TABLES[table]
        where = " AND ".join(name + "=?" for name in keys)
        params = [source] + [key[name] for name in keys]
        if operation == "delete":
            con.execute(
                "DELETE FROM connector_" + table + " WHERE source=? AND " + where, params
            )
        elif table == "achievements":
            con.execute(
                "INSERT OR REPLACE INTO connector_achievements VALUES(?,?,?,?,?,?,?)",
                (
                    source,
                    after["id"],
                    after["title"],
                    after["institution_id"],
                    after["kind"],
                    after["year"],
                    txid,
                ),
            )
        else:
            con.execute(
                "INSERT OR REPLACE INTO connector_"
                + table
                + " (source,"
                + ",".join(columns)
                + ") VALUES(?,"
                + ",".join("?" for _ in columns)
                + ")",
                [source] + [after[name] for name in columns],
            )
        if table == "achievements":
            affected.add(key["id"])
        elif table == "authors":
            affected.add(key["achievement_id"])
        elif table == "institutions":
            for row in con.execute(
                "SELECT id FROM connector_achievements WHERE source=? AND institution_id=?",
                (source, key["id"]),
            ):
                affected.add(row["id"])
        elif table == "people":
            for row in con.execute(
                "SELECT achievement_id FROM connector_authors WHERE source=? AND person_id=?",
                (source, key["id"]),
            ):
                affected.add(row["achievement_id"])

    def _apply_transaction(self, source, txid, rows):
        batch = source + "-tx-" + str(txid)
        with self.store.atomic() as con:
            state = self._state(source)
            if state["status"] == "needs_rebuild":
                fail("state", "export log gap requires rebuild before polling")
            if state["checkpoint_txid"] >= txid:
                return {"txid": txid, "status": "skipped"}
            if state["checkpoint_txid"] != txid - 1:
                fail("sequence", "source transaction chain broken")
            current = self.sources.get(source)
            if current["generation"] != state["generation"]:
                fail(
                    "conflict",
                    "source generation changed outside connector",
                    expected=state["generation"],
                    actual=current["generation"],
                )
            affected = set()
            for row in rows:
                table, operation, key, before, after = self._decode_change(row)
                self._check_before(source, table, key, before)
                self._apply_change(con, source, table, operation, key, after, affected, txid)
            for aid in affected:
                con.execute(
                    "UPDATE connector_achievements SET txid=? WHERE source=? AND id=?",
                    (txid, source, aid),
                )
            records, dangling = self._derive(*self._mirror_maps(source))
            generation = state["generation"] + 1
            result = self.sources.snapshot(
                batch, source, generation, records, state["generation"], True
            )
            self._set_state(source, "ok", state["boundary_txid"], txid, generation, None)
            self._ledger_row(
                source,
                txid,
                "transaction",
                generation,
                batch,
                digest(records),
                sorted(affected),
                "applied",
                None,
            )
            self.store.audit(
                "connector_transaction",
                source,
                {
                    "txid": txid,
                    "generation": generation,
                    "affected": sorted(affected),
                    "changed": result["changed"],
                    "withdrawn": result["withdrawn"],
                    "dangling_relations": dangling,
                    "records_digest": digest(records),
                },
            )
            return {
                "txid": txid,
                "status": "applied",
                "generation": generation,
                "affected": sorted(affected),
                "changed": result["changed"],
                "unchanged": len(result["unchanged"]),
                "withdrawn": result["withdrawn"],
            }

    def _record_failure(self, source, txid, exc):
        with self.store.atomic():
            state = self._state(source)
            if state["checkpoint_txid"] == txid - 1 and state["status"] != "needs_rebuild":
                failure = {"txid": txid, "code": exc.code, "message": str(exc)}
                self._set_state(
                    source,
                    "failed",
                    state["boundary_txid"],
                    state["checkpoint_txid"],
                    state["generation"],
                    failure,
                )
                self._ledger_row(
                    source,
                    txid,
                    "transaction",
                    state["generation"],
                    source + "-tx-" + str(txid),
                    "",
                    [],
                    "failed",
                    exc.record(),
                )
                self.store.audit("connector_failed", source, failure)
        return {"txid": txid, "status": "failed", "error": exc.record()}

    def _load(self, source, path, during_read, kind):
        identifier(source)
        snap = self._read_export(path, during_read)
        records, dangling = self._derive(
            snap["institutions"], snap["people"], snap["achievements"], snap["authors"]
        )
        with self.store.atomic() as con:
            existing = self.store.one(
                "SELECT * FROM connector_state WHERE source=?", (source,)
            )
            current = self.sources.get(source)
            if kind == "snapshot":
                if existing is not None:
                    fail("state", "connector already initialized; use rebuild")
                if current["generation"] != 0:
                    fail(
                        "conflict",
                        "source already holds records; use rebuild to switch explicitly",
                    )
            elif existing is not None and snap["boundary"] < existing["checkpoint_txid"]:
                fail(
                    "state",
                    "rebuild boundary behind checkpoint; export history regressed",
                    boundary=snap["boundary"],
                    checkpoint=existing["checkpoint_txid"],
                )
            if (
                existing is not None
                and snap["boundary"] == existing["checkpoint_txid"]
                and current["generation"] == existing["generation"]
            ):
                row = self.store.one(
                    "SELECT * FROM connector_ledger WHERE source=? AND txid=?",
                    (source, snap["boundary"]),
                )
                if (
                    row is not None
                    and row["status"] == "applied"
                    and row["records_digest"] == digest(records)
                ):
                    return {
                        "source": source,
                        "kind": kind,
                        "boundary": snap["boundary"],
                        "checkpoint": snap["boundary"],
                        "generation": existing["generation"],
                        "records": len(records),
                        "mapping_version": MAPPING_VERSION,
                        "reused": True,
                    }
            generation = current["generation"] + 1
            batch = (
                source + "-snapshot-" + str(snap["boundary"]) + "-g" + str(generation)
            )
            result = self.sources.snapshot(
                batch, source, generation, records, current["generation"], True
            )
            for table in TABLES:
                con.execute("DELETE FROM connector_" + table + " WHERE source=?", (source,))
            con.executemany(
                "INSERT INTO connector_institutions VALUES(?,?,?)",
                [
                    (source, ident, name)
                    for ident, name in sorted(snap["institutions"].items())
                ],
            )
            con.executemany(
                "INSERT INTO connector_people VALUES(?,?,?)",
                [(source, ident, name) for ident, name in sorted(snap["people"].items())],
            )
            con.executemany(
                "INSERT INTO connector_achievements VALUES(?,?,?,?,?,?,?)",
                [
                    (
                        source,
                        row["id"],
                        row["title"],
                        row["institution_id"],
                        row["kind"],
                        row["year"],
                        snap["boundary"],
                    )
                    for row in sorted(snap["achievements"].values(), key=lambda r: r["id"])
                ],
            )
            con.executemany(
                "INSERT INTO connector_authors VALUES(?,?,?,?)",
                [
                    (source, row["achievement_id"], row["person_id"], row["ordinal"])
                    for row in snap["authors"]
                ],
            )
            self._set_state(
                source, "ok", snap["boundary"], snap["boundary"], generation, None
            )
            self._ledger_row(
                source,
                snap["boundary"],
                kind,
                generation,
                batch,
                digest(records),
                sorted(snap["achievements"]),
                "applied",
                None,
            )
            self.store.audit(
                "connector_" + kind,
                source,
                {
                    "boundary": snap["boundary"],
                    "generation": generation,
                    "records": len(records),
                    "dangling_relations": dangling,
                    "records_digest": digest(records),
                },
            )
        return {
            "source": source,
            "kind": kind,
            "boundary": snap["boundary"],
            "checkpoint": snap["boundary"],
            "generation": generation,
            "records": len(records),
            "dangling_relations": dangling,
            "changed": result["changed"],
            "withdrawn": result["withdrawn"],
            "mapping_version": MAPPING_VERSION,
            "reused": False,
        }

    def snapshot(self, source, path, during_read=None):
        return self._load(source, path, during_read, "snapshot")

    def rebuild(self, source, path, during_read=None):
        return self._load(source, path, during_read, "rebuild")

    def poll(self, source, path):
        identifier(source)
        state = self._state(source)
        if state["status"] == "needs_rebuild":
            return {
                "source": source,
                "status": "needs_rebuild",
                "checkpoint": state["checkpoint_txid"],
                "failure": state["failure"],
                "applied": [],
            }
        current = self.sources.get(source)
        if current["generation"] != state["generation"]:
            failure = {
                "code": "conflict",
                "message": "source generation changed outside connector",
                "expected": state["generation"],
                "actual": current["generation"],
            }
            with self.store.atomic():
                fresh = self._state(source)
                if fresh["generation"] == state["generation"] and fresh["status"] != "needs_rebuild":
                    self._set_state(
                        source,
                        "failed",
                        fresh["boundary_txid"],
                        fresh["checkpoint_txid"],
                        fresh["generation"],
                        failure,
                    )
                    self.store.audit("connector_failed", source, failure)
            return {
                "source": source,
                "status": "failed",
                "checkpoint": state["checkpoint_txid"],
                "failure": failure,
                "applied": [],
            }
        first_tx, txids, grouped = self._read_log(path, state["checkpoint_txid"])
        gap = None
        expected = list(
            range(state["checkpoint_txid"] + 1, state["checkpoint_txid"] + 1 + len(txids))
        )
        if txids and first_tx > state["checkpoint_txid"] + 1:
            gap = {
                "reason": "export log pruned past checkpoint",
                "first_tx": first_tx,
                "needed_tx": state["checkpoint_txid"] + 1,
            }
        elif txids != expected:
            gap = {"reason": "export transaction log incomplete", "txids": txids[:20]}
        elif set(grouped) - set(txids):
            gap = {
                "reason": "export change log references unknown transactions",
                "txids": sorted(set(grouped) - set(txids)),
            }
        if gap is not None:
            with self.store.atomic():
                fresh = self._state(source)
                if (
                    fresh["status"] != "needs_rebuild"
                    and fresh["checkpoint_txid"] == state["checkpoint_txid"]
                ):
                    self._set_state(
                        source,
                        "needs_rebuild",
                        fresh["boundary_txid"],
                        fresh["checkpoint_txid"],
                        fresh["generation"],
                        gap,
                    )
                    self.store.audit("connector_gap", source, gap)
            return {
                "source": source,
                "status": "needs_rebuild",
                "checkpoint": state["checkpoint_txid"],
                "failure": gap,
                "applied": [],
            }
        applied = []
        for txid in txids:
            try:
                outcome = self._apply_transaction(source, txid, grouped.get(txid, []))
            except MeshError as exc:
                outcome = self._record_failure(source, txid, exc)
            applied.append(outcome)
            if outcome["status"] == "failed":
                break
        state = self._state(source)
        return {
            "source": source,
            "status": state["status"],
            "checkpoint": state["checkpoint_txid"],
            "generation": state["generation"],
            "applied": applied,
        }

    def status(self, source, path=None):
        identifier(source)
        state = self._state(source)
        current = self.sources.get(source)
        mirror = {
            table: self.store.one(
                "SELECT COUNT(*) AS n FROM connector_" + table + " WHERE source=?",
                (source,),
            )["n"]
            for table in TABLES
        }
        records = self.store.one(
            "SELECT COUNT(*) AS a,SUM(CASE WHEN active=0 THEN 1 ELSE 0 END) AS i FROM records WHERE source=?",
            (source,),
        )
        result = {
            "source": source,
            "status": state["status"],
            "boundary_txid": state["boundary_txid"],
            "checkpoint_txid": state["checkpoint_txid"],
            "generation": state["generation"],
            "source_generation": current["generation"],
            "mapping_version": state["mapping_version"],
            "failure": state["failure"],
            "mirror": mirror,
            "records": {"active": records["a"] - (records["i"] or 0), "inactive": records["i"] or 0},
            "index_stale": self.store.one(
                "SELECT COUNT(*) AS n FROM records WHERE source=? AND active=1 AND dict_revision!=?",
                (source, self.store.revision("dictionary")),
            )["n"],
        }
        if path is not None:
            con = self._connect(path)
            try:
                self._check_export_schema(con)
                max_tx = con.execute(
                    "SELECT COALESCE(MAX(txid),0) FROM transactions"
                ).fetchone()[0]
                first_tx = con.execute(
                    "SELECT first_tx FROM retention WHERE singleton=1"
                ).fetchone()[0]
            finally:
                con.close()
            result["export"] = {
                "max_txid": max_tx,
                "first_tx": first_tx,
                "pending": max(0, max_tx - state["checkpoint_txid"]),
            }
        return result

    def ledger(self, source):
        identifier(source)
        self._state(source)
        return {"source": source, "rows": self._ledger_rows(source)}

    def lineage(self, source, achievement=None):
        identifier(source)
        state = self._state(source)
        institutions, people, achievements, authors = self._mirror_maps(source)
        by_achievement = {}
        for row in authors:
            by_achievement.setdefault(row["achievement_id"], []).append(
                (row["ordinal"], row["person_id"])
            )

        def entry(aid):
            row = achievements[aid]
            key = record_key(source, aid)
            record = self.store.one(
                "SELECT generation,revision,active FROM records WHERE key=?", (key,)
            )
            return {
                "achievement": aid,
                "record": key,
                "present": True,
                "active": bool(record["active"]) if record else False,
                "revision": record["revision"] if record else None,
                "generation": record["generation"] if record else None,
                "title": row["title"],
                "institution_id": row["institution_id"],
                "institution": institutions.get(row["institution_id"]),
                "kind": row["kind"],
                "year": row["year"],
                "authors": [
                    {"person": pid, "name": people.get(pid), "ordinal": ordinal}
                    for ordinal, pid in sorted(by_achievement.get(aid, []))
                ],
                "txid": row["txid"],
                "mapping_version": state["mapping_version"],
            }

        if achievement is not None:
            achievement = bounded_text(achievement, 200)
            if achievement in achievements:
                return entry(achievement)
            key = record_key(source, achievement)
            record = self.store.one(
                "SELECT generation,revision,active FROM records WHERE key=?", (key,)
            )
            touched = [
                row for row in self._ledger_rows(source) if achievement in row["affected"]
            ]
            if record is None and not touched:
                fail("missing", "achievement unknown to connector", achievement=achievement)
            return {
                "achievement": achievement,
                "record": key,
                "present": False,
                "active": bool(record["active"]) if record else False,
                "revision": record["revision"] if record else None,
                "generation": record["generation"] if record else None,
                "last_transaction": touched[-1] if touched else None,
                "mapping_version": state["mapping_version"],
            }
        return {
            "source": source,
            "mapping_version": state["mapping_version"],
            "records": [entry(aid) for aid in sorted(achievements)],
        }

    def verify(self, source):
        identifier(source)
        with self.store.atomic():
            state = self._state(source)
            current = self.sources.get(source)
            if current["generation"] != state["generation"]:
                fail(
                    "integrity",
                    "source generation differs from connector checkpoint",
                    expected=state["generation"],
                    actual=current["generation"],
                )
            rows = self._ledger_rows(source)
            window = [
                row
                for row in rows
                if state["boundary_txid"] <= row["txid"] <= state["checkpoint_txid"]
            ]
            if (
                not window
                or window[0]["txid"] != state["boundary_txid"]
                or window[0]["kind"] not in {"snapshot", "rebuild"}
                or window[0]["status"] != "applied"
            ):
                fail("integrity", "connector ledger lost its snapshot anchor")
            rest = window[1:]
            if [row["txid"] for row in rest] != list(
                range(state["boundary_txid"] + 1, state["checkpoint_txid"] + 1)
            ):
                fail("integrity", "connector ledger chain incomplete")
            if any(
                row["status"] != "applied" or row["kind"] != "transaction" for row in rest
            ):
                fail("integrity", "connector ledger contains unapplied transaction")
            generations = [row["generation"] for row in window]
            if generations != list(
                range(generations[0], generations[0] + len(generations))
            ) or generations[-1] != state["generation"]:
                fail("integrity", "connector ledger generations inconsistent")
            records, dangling = self._derive(*self._mirror_maps(source))
            expected_keys = {record_key(source, item["id"]) for item in records}
            active = {}
            for row in self.store.rows(
                "SELECT key FROM records WHERE source=? AND active=1", (source,)
            ):
                active[row["key"]] = self.sources.record(row["key"])
            if set(active) != expected_keys:
                fail(
                    "integrity",
                    "active records differ from connector projection",
                    missing=sorted(expected_keys - set(active)),
                    extra=sorted(set(active) - expected_keys),
                )
            for item in records:
                key = record_key(source, item["id"])
                if active[key]["raw"] != source_contract(item["data"]):
                    fail(
                        "integrity",
                        "record payload differs from connector projection",
                        record=key,
                    )
            revision = self.store.revision("dictionary")
            stale = sorted(key for key, row in active.items() if row["dict_revision"] != revision)
            if stale:
                fail(
                    "index",
                    "dictionary changed; reindex records before matching or publishing",
                    records=stale,
                )
            for key, record in active.items():
                blocks = [
                    row["block"]
                    for row in self.store.rows(
                        "SELECT block FROM blocks WHERE record=? ORDER BY block", (key,)
                    )
                ]
                if blocks != self.sources.normalizer.block_keys(record["normalized"]):
                    fail(
                        "integrity",
                        "blocking index differs from normalized record",
                        record=key,
                    )
            audit = self.store.verify_audit()
        return {
            "verified": True,
            "source": source,
            "status": state["status"],
            "checkpoint": state["checkpoint_txid"],
            "generation": state["generation"],
            "records": len(active),
            "dangling_relations": dangling,
            "audit_head": audit["head"],
        }
