import tempfile
import unittest
from pathlib import Path
from researchmesh.service import ResearchMesh
from researchmesh.contracts import MeshError
from lab.source_db import connect, create, mutate, prune, transaction


class ConnectorCase(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.dir.cleanup)
        self.export = create(Path(self.dir.name) / "export.sqlite3")
        self.mesh = ResearchMesh(Path(self.dir.name) / "mesh.sqlite3")
        self.addCleanup(self.mesh.close)
        self.mesh.sources.register("inst", "研究所登记出口", 10)
        self.connector = self.mesh.connectors

    def write(self, changes):
        con = connect(self.export)
        try:
            return transaction(con, changes)
        finally:
            con.close()

    def seed_second(self):
        return self.write(
            [
                {"table": "institutions", "key": {"id": "org2"}, "value": {"id": "org2", "name": "第二研究所"}},
                {"table": "achievements", "key": {"id": "a2"}, "value": {"id": "a2", "title": "巡检数据集", "institution_id": "org2", "kind": "dataset", "year": 2026}},
                {"table": "people", "key": {"id": "p3"}, "value": {"id": "p3", "name": "人员丙"}},
                {"table": "authors", "key": {"achievement_id": "a2", "person_id": "p3"}, "value": {"achievement_id": "a2", "person_id": "p3", "ordinal": 1}},
            ]
        )


class SnapshotAndCatchup(ConnectorCase):
    def test_snapshot_isolation_then_atomic_multi_table_transaction(self):
        snapshot = self.connector.snapshot(
            "inst", self.export, during_read=lambda: mutate(self.export)
        )
        self.assertEqual(snapshot["boundary"], 1)
        record = self.mesh.sources.record("inst:a1")
        self.assertEqual(record["raw"]["institution"], "合成研究所")
        self.assertEqual(record["raw"]["people"], ["人员甲"])
        out = self.connector.poll("inst", self.export)
        self.assertEqual([row["txid"] for row in out["applied"]], [2])
        self.assertEqual(out["checkpoint"], 2)
        record = self.mesh.sources.record("inst:a1")
        self.assertEqual(record["raw"]["institution"], "合成研究所新名")
        self.assertEqual(record["generation"], 2)
        lineage = self.connector.lineage("inst", "a1")
        self.assertEqual(
            [(a["name"], a["ordinal"]) for a in lineage["authors"]],
            [("人员甲", 1), ("人员乙", 2)],
        )
        self.assertEqual(lineage["txid"], 2)
        self.assertEqual(lineage["mapping_version"], 1)
        diff = self.mesh.changes.compare_generations("inst", 1, 2)
        self.assertEqual(len(diff["changes"]), 1)
        self.assertEqual(
            {f["field"] for f in diff["changes"][0]["fields"]},
            {"institution", "people"},
        )
        self.assertTrue(self.connector.verify("inst")["verified"])
        self.assertTrue(self.mesh.quality.verify()["verified"])

    def test_relation_delete_keeps_achievement(self):
        self.connector.snapshot("inst", self.export)
        mutate(self.export)
        self.connector.poll("inst", self.export)
        mutate(self.export, "unlink")
        out = self.connector.poll("inst", self.export)
        self.assertEqual(out["applied"][0]["withdrawn"], [])
        record = self.mesh.sources.record("inst:a1")
        self.assertTrue(record["active"])
        self.assertEqual(record["raw"]["people"], ["人员乙"])
        lineage = self.connector.lineage("inst", "a1")
        self.assertEqual([a["name"] for a in lineage["authors"]], ["人员乙"])
        self.assertTrue(self.connector.verify("inst")["verified"])

    def test_achievement_delete_withdraws_record(self):
        self.connector.snapshot("inst", self.export)
        mutate(self.export, "unlink")
        self.connector.poll("inst", self.export)
        mutate(self.export, "delete")
        out = self.connector.poll("inst", self.export)
        self.assertEqual(out["applied"][0]["withdrawn"], ["inst:a1"])
        record = self.mesh.sources.record("inst:a1", True)
        self.assertFalse(record["active"])
        self.assertFalse(self.mesh.sources.history("inst:a1")[-1]["active"])
        self.assertEqual(
            self.mesh.store.rows("SELECT * FROM blocks WHERE record='inst:a1'"), []
        )
        gone = self.connector.lineage("inst", "a1")
        self.assertFalse(gone["present"])
        self.assertEqual(gone["last_transaction"]["txid"], 3)
        self.assertEqual(gone["last_transaction"]["kind"], "transaction")
        self.assertTrue(self.connector.verify("inst")["verified"])

    def test_snapshot_requires_fresh_source(self):
        self.mesh.sources.snapshot(
            "manual",
            "inst",
            1,
            [
                {
                    "id": "hand1",
                    "data": {
                        "title": "人工成果",
                        "institution": "某机构",
                        "kind": "dataset",
                    },
                }
            ],
            0,
        )
        with self.assertRaises(MeshError) as caught:
            self.connector.snapshot("inst", self.export)
        self.assertEqual(caught.exception.code, "conflict")
        result = self.connector.rebuild("inst", self.export)
        self.assertEqual(result["checkpoint"], 1)
        self.assertFalse(self.mesh.sources.record("inst:hand1", True)["active"])
        self.assertTrue(self.connector.verify("inst")["verified"])

    def test_rebuild_replay_is_reused(self):
        self.connector.snapshot("inst", self.export)
        again = self.connector.rebuild("inst", self.export)
        self.assertTrue(again["reused"])
        self.assertEqual(self.mesh.sources.get("inst")["generation"], 1)
        mutate(self.export)
        moved = self.connector.rebuild("inst", self.export)
        self.assertFalse(moved["reused"])
        self.assertEqual(moved["boundary"], 2)
        self.assertEqual(
            self.mesh.sources.record("inst:a1")["raw"]["institution"], "合成研究所新名"
        )


class FailureAndRecovery(ConnectorCase):
    def test_failed_transaction_rolls_back_and_retains_reason(self):
        self.connector.snapshot("inst", self.export)
        self.write(
            [
                {"table": "institutions", "key": {"id": "org1"}, "value": {"id": "org1", "name": "改名研究所"}},
                {"table": "achievements", "key": {"id": "a9"}, "value": {"id": "a9", "title": "问题成果", "institution_id": "org1", "kind": "banana", "year": 2026}},
            ]
        )
        out = self.connector.poll("inst", self.export)
        self.assertEqual(out["status"], "failed")
        self.assertEqual(out["applied"][0]["status"], "failed")
        self.assertEqual(out["checkpoint"], 1)
        mirror = self.mesh.store.one(
            "SELECT name FROM connector_institutions WHERE source='inst' AND id='org1'"
        )
        self.assertEqual(mirror["name"], "合成研究所")
        self.assertIsNone(
            self.mesh.store.one(
                "SELECT id FROM connector_achievements WHERE source='inst' AND id='a9'"
            )
        )
        self.assertEqual(self.mesh.sources.get("inst")["generation"], 1)
        status = self.connector.status("inst")
        self.assertEqual(status["status"], "failed")
        self.assertEqual(status["failure"]["txid"], 2)
        self.assertEqual(status["failure"]["code"], "input")
        ledger = self.connector.ledger("inst")["rows"]
        failed = [row for row in ledger if row["status"] == "failed"]
        self.assertEqual(len(failed), 1)
        self.assertEqual(failed[0]["txid"], 2)
        self.assertEqual(failed[0]["reason"]["code"], "input")
        self.assertTrue(self.connector.verify("inst")["verified"])
        self.assertTrue(self.mesh.quality.verify()["verified"])
        again = self.connector.poll("inst", self.export)
        self.assertEqual(again["status"], "failed")
        self.assertEqual(again["checkpoint"], 1)

    def test_log_gap_enters_needs_rebuild_and_rebuild_switches(self):
        self.connector.snapshot("inst", self.export)
        mutate(self.export)
        self.connector.poll("inst", self.export)
        self.seed_second()
        self.connector.poll("inst", self.export)
        self.assertEqual(self.connector.status("inst")["checkpoint_txid"], 3)
        self.write(
            [{"table": "institutions", "key": {"id": "org2"}, "value": {"id": "org2", "name": "第二研究院"}}]
        )
        prune(self.export, 5)
        out = self.connector.poll("inst", self.export)
        self.assertEqual(out["status"], "needs_rebuild")
        self.assertEqual(out["checkpoint"], 3)
        self.assertEqual(out["failure"]["needed_tx"], 4)
        record = self.mesh.sources.record("inst:a2")
        self.assertEqual(record["raw"]["institution"], "第二研究所")
        self.mesh.releases.preview()
        again = self.connector.poll("inst", self.export)
        self.assertEqual(again["status"], "needs_rebuild")
        self.assertEqual(again["applied"], [])
        rebuilt = self.connector.rebuild("inst", self.export)
        self.assertEqual(rebuilt["boundary"], 4)
        self.assertEqual(rebuilt["checkpoint"], 4)
        self.assertEqual(self.connector.status("inst")["status"], "ok")
        record = self.mesh.sources.record("inst:a2")
        self.assertEqual(record["raw"]["institution"], "第二研究院")
        self.assertTrue(self.connector.verify("inst")["verified"])
        self.assertTrue(self.mesh.quality.verify()["verified"])

    def test_prune_beyond_all_transactions_is_not_a_gap(self):
        self.connector.snapshot("inst", self.export)
        prune(self.export, 10)
        out = self.connector.poll("inst", self.export)
        self.assertEqual(out["status"], "ok")
        self.assertEqual(out["applied"], [])
        mutate(self.export)
        out = self.connector.poll("inst", self.export)
        self.assertEqual(out["status"], "needs_rebuild")
        rebuilt = self.connector.rebuild("inst", self.export)
        self.assertEqual(rebuilt["checkpoint"], 2)

    def test_before_image_mismatch_is_integrity_failure(self):
        self.connector.snapshot("inst", self.export)
        with self.mesh.store.atomic() as con:
            con.execute(
                "UPDATE connector_institutions SET name='被篡改' WHERE source='inst' AND id='org1'"
            )
        mutate(self.export)
        out = self.connector.poll("inst", self.export)
        self.assertEqual(out["status"], "failed")
        self.assertEqual(out["applied"][0]["error"]["code"], "integrity")
        self.assertEqual(out["checkpoint"], 1)
        self.assertEqual(
            self.mesh.sources.record("inst:a1")["raw"]["institution"], "合成研究所"
        )

    def test_unknown_export_object_fails_clearly(self):
        with self.assertRaises(MeshError) as caught:
            self.connector.snapshot("inst", Path(self.dir.name) / "missing.sqlite3")
        self.assertEqual(caught.exception.code, "path")
        with self.assertRaises(MeshError) as caught:
            self.connector.poll("inst", self.export)
        self.assertEqual(caught.exception.code, "missing")


class ConcurrencyAndGovernance(ConnectorCase):
    def test_restart_replay_creates_no_extra_versions(self):
        self.connector.snapshot("inst", self.export)
        mutate(self.export)
        self.connector.poll("inst", self.export)
        path = Path(self.dir.name) / "mesh.sqlite3"
        self.mesh.close()
        self.mesh = ResearchMesh(path)
        self.connector = self.mesh.connectors
        head = self.mesh.store.verify_audit()["head"]
        revision = self.mesh.sources.record("inst:a1")["revision"]
        out = self.connector.poll("inst", self.export)
        self.assertEqual(out["applied"], [])
        self.assertEqual(self.mesh.store.verify_audit()["head"], head)
        self.assertEqual(self.mesh.sources.record("inst:a1")["revision"], revision)
        self.seed_second()
        out = self.connector.poll("inst", self.export)
        self.assertEqual([row["txid"] for row in out["applied"]], [3])
        self.assertEqual(self.mesh.sources.record("inst:a2")["revision"], 1)
        self.assertTrue(self.connector.verify("inst")["verified"])

    def test_manual_import_generation_conflict_preserves_manual(self):
        self.connector.snapshot("inst", self.export)
        self.mesh.sources.snapshot(
            "manual1",
            "inst",
            2,
            [
                {
                    "id": "hand1",
                    "data": {
                        "title": "人工补录成果",
                        "institution": "合成研究所",
                        "kind": "software",
                    },
                }
            ],
            1,
            False,
        )
        out = self.connector.poll("inst", self.export)
        self.assertEqual(out["status"], "failed")
        self.assertEqual(out["failure"]["code"], "conflict")
        self.assertTrue(self.mesh.sources.record("inst:hand1")["active"])
        self.assertEqual(
            self.mesh.sources.record("inst:a1")["raw"]["institution"], "合成研究所"
        )
        mutate(self.export)
        out = self.connector.poll("inst", self.export)
        self.assertEqual(out["status"], "failed")
        self.assertEqual(
            self.mesh.sources.record("inst:a1")["raw"]["institution"], "合成研究所"
        )
        rebuilt = self.connector.rebuild("inst", self.export)
        self.assertEqual(rebuilt["boundary"], 2)
        self.assertFalse(self.mesh.sources.record("inst:hand1", True)["active"])
        self.assertEqual(
            self.mesh.sources.record("inst:a1")["raw"]["institution"], "合成研究所新名"
        )
        self.assertTrue(self.connector.verify("inst")["verified"])

    def test_dictionary_change_blocks_publish_until_reindex(self):
        self.connector.snapshot("inst", self.export)
        self.mesh.dictionary.put("institution", "合成研究所", "规范研究所", 0, "演练")
        with self.assertRaises(MeshError) as caught:
            self.mesh.releases.preview()
        self.assertEqual(caught.exception.code, "index")
        self.assertEqual(self.connector.status("inst")["index_stale"], 1)
        with self.assertRaises(MeshError) as caught:
            self.connector.verify("inst")
        self.assertEqual(caught.exception.code, "index")
        self.mesh.sources.reindex(1)
        self.mesh.releases.preview()
        self.assertTrue(self.connector.verify("inst")["verified"])
        self.assertTrue(self.mesh.quality.verify()["verified"])

    def test_status_reports_export_position(self):
        self.connector.snapshot("inst", self.export)
        mutate(self.export)
        status = self.connector.status("inst", self.export)
        self.assertEqual(status["export"]["max_txid"], 2)
        self.assertEqual(status["export"]["pending"], 1)
        self.assertEqual(status["mirror"]["achievements"], 1)
        self.connector.poll("inst", self.export)
        status = self.connector.status("inst", self.export)
        self.assertEqual(status["export"]["pending"], 0)
        self.assertEqual(status["records"], {"active": 1, "inactive": 0})


if __name__ == "__main__":
    unittest.main()
