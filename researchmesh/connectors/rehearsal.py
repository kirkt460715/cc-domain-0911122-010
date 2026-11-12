import tempfile
from pathlib import Path
from ..contracts import MeshError
from ..service import ResearchMesh
from lab.source_db import connect, create, mutate, prune, transaction


def run():
    stages = {}
    with tempfile.TemporaryDirectory() as directory:
        export = create(Path(directory) / "export.sqlite3")
        mesh_path = Path(directory) / "mesh.sqlite3"
        mesh = ResearchMesh(mesh_path)
        mesh.sources.register("inst", "研究所登记出口", 10)
        connector = mesh.connectors

        # 初次快照：读取事务进行中另一连接并发提交 tx2，快照仍停在边界 1
        snapshot = connector.snapshot("inst", export, during_read=lambda: mutate(export))
        assert snapshot["boundary"] == 1 and snapshot["checkpoint"] == 1
        record = mesh.sources.record("inst:a1")
        assert record["raw"]["institution"] == "合成研究所"
        assert record["raw"]["people"] == ["人员甲"]
        stages["snapshot_under_concurrent_commit"] = {
            "boundary": snapshot["boundary"],
            "institution_seen": record["raw"]["institution"],
        }

        # 增量衔接：tx2 一笔事务改机构并加第二作者，研合侧同一 generation 整笔生效
        out = connector.poll("inst", export)
        assert [row["txid"] for row in out["applied"]] == [2]
        record = mesh.sources.record("inst:a1")
        assert record["raw"]["institution"] == "合成研究所新名"
        lineage = connector.lineage("inst", "a1")
        assert [a["name"] for a in lineage["authors"]] == ["人员甲", "人员乙"]
        assert [a["ordinal"] for a in lineage["authors"]] == [1, 2]
        assert lineage["txid"] == 2 and lineage["mapping_version"] == 1
        diff = mesh.changes.compare_generations("inst", 1, 2)
        assert {f["field"] for f in diff["changes"][0]["fields"]} == {
            "institution",
            "people",
        }
        stages["multi_table_transaction"] = {
            "txid": 2,
            "generation": out["generation"],
            "authors_by_ordinal": [a["name"] for a in lineage["authors"]],
        }

        # 删除作者关系不等于删除成果：记录仍活动，人员按剩余关系更新
        mutate(export, "unlink")
        out = connector.poll("inst", export)
        record = mesh.sources.record("inst:a1")
        assert record["active"] and record["raw"]["people"] == ["人员乙"]
        assert out["applied"][0]["withdrawn"] == []
        stages["relation_delete_keeps_achievement"] = {
            "txid": 3,
            "record_active": True,
            "people": record["raw"]["people"],
        }

        # 成果删除才撤回对应来源项
        mutate(export, "delete")
        out = connector.poll("inst", export)
        assert out["applied"][0]["withdrawn"] == ["inst:a1"]
        record = mesh.sources.record("inst:a1", True)
        assert not record["active"]
        assert mesh.sources.history("inst:a1")[-1]["active"] is False
        assert mesh.store.rows("SELECT * FROM blocks WHERE record='inst:a1'") == []
        gone = connector.lineage("inst", "a1")
        assert not gone["present"] and gone["last_transaction"]["txid"] == 4
        stages["achievement_delete_withdraws"] = {"txid": 4, "active": False}

        # 日志清理越过检查点：明确进入需要重建，旧来源记录仍可使用
        writer = connect(export)
        transaction(
            writer,
            [
                {"table": "institutions", "key": {"id": "org2"}, "value": {"id": "org2", "name": "第二研究所"}},
                {"table": "achievements", "key": {"id": "a2"}, "value": {"id": "a2", "title": "巡检数据集", "institution_id": "org2", "kind": "dataset", "year": 2026}},
                {"table": "people", "key": {"id": "p3"}, "value": {"id": "p3", "name": "人员丙"}},
                {"table": "authors", "key": {"achievement_id": "a2", "person_id": "p3"}, "value": {"achievement_id": "a2", "person_id": "p3", "ordinal": 1}},
            ],
        )
        writer.close()
        out = connector.poll("inst", export)
        assert out["checkpoint"] == 5
        writer = connect(export)
        transaction(
            writer,
            [{"table": "institutions", "key": {"id": "org2"}, "value": {"id": "org2", "name": "第二研究院"}}],
        )
        writer.close()
        prune(export, 7)
        out = connector.poll("inst", export)
        assert out["status"] == "needs_rebuild" and out["checkpoint"] == 5
        record = mesh.sources.record("inst:a2")
        assert record["raw"]["institution"] == "第二研究所"
        mesh.releases.preview()
        stages["log_gap_needs_rebuild"] = {
            "status": out["status"],
            "checkpoint": out["checkpoint"],
            "old_source_usable": record["raw"]["institution"],
        }

        # 重新快照完成并通过 generation 检查后再切换
        rebuilt = connector.rebuild("inst", export)
        assert rebuilt["boundary"] == 6 and rebuilt["checkpoint"] == 6
        record = mesh.sources.record("inst:a2")
        assert record["raw"]["institution"] == "第二研究院"
        assert connector.status("inst")["status"] == "ok"
        stages["rebuild_then_switch"] = {
            "boundary": 6,
            "generation": rebuilt["generation"],
            "institution": record["raw"]["institution"],
        }
        mesh.close()

        # 重启后重复读取不产生额外版本
        mesh = ResearchMesh(mesh_path)
        connector = mesh.connectors
        head = mesh.store.verify_audit()["head"]
        revision = mesh.sources.record("inst:a2")["revision"]
        out = connector.poll("inst", export)
        assert out["applied"] == [] and out["checkpoint"] == 6
        assert mesh.store.verify_audit()["head"] == head
        assert mesh.sources.record("inst:a2")["revision"] == revision
        stages["restart_replay_idempotent"] = {"checkpoint": 6, "audit_head_stable": True}

        # 词典变化发生在采集期间：过期索引不得参与发布，显式重建索引后恢复
        mesh.dictionary.put("institution", "第二研究院", "规范化研究院", 0, "演练词典")
        try:
            mesh.releases.preview()
            raise AssertionError("stale index must block release preview")
        except MeshError as exc:
            assert exc.code == "index"
        assert connector.status("inst")["index_stale"] == 1
        mesh.sources.reindex(1)
        mesh.releases.preview()
        stages["dictionary_change_requires_reindex"] = {"blocked_then_reindexed": True}

        # 人工导入发生在采集期间：连接器不覆盖人工版本，显式重建才切换
        generation = mesh.sources.get("inst")["generation"]
        mesh.sources.snapshot(
            "manual1",
            "inst",
            generation + 1,
            [
                {
                    "id": "hand1",
                    "data": {
                        "title": "人工补录成果",
                        "institution": "第二研究院",
                        "kind": "software",
                        "year": 2026,
                        "people": ["人员丁"],
                    },
                }
            ],
            generation,
            False,
        )
        out = connector.poll("inst", export)
        assert out["status"] == "failed" and out["failure"]["code"] == "conflict"
        assert mesh.sources.record("inst:hand1")["active"]
        assert mesh.sources.record("inst:a2")["raw"]["institution"] == "第二研究院"
        stages["manual_import_not_overwritten"] = {
            "status": out["status"],
            "manual_record_active": True,
        }
        rebuilt = connector.rebuild("inst", export)
        assert not mesh.sources.record("inst:hand1", True)["active"]

        # 失败事务：一笔事务内任何一环失败都整笔回滚并保留原因
        export2 = create(Path(directory) / "export2.sqlite3")
        mesh.sources.register("bad", "故障演练出口", 20)
        connector.snapshot("bad", export2)
        writer = connect(export2)
        transaction(
            writer,
            [
                {"table": "institutions", "key": {"id": "org1"}, "value": {"id": "org1", "name": "改名研究所"}},
                {"table": "achievements", "key": {"id": "a9"}, "value": {"id": "a9", "title": "问题成果", "institution_id": "org1", "kind": "banana", "year": 2026}},
            ],
        )
        writer.close()
        out = connector.poll("bad", export2)
        assert out["status"] == "failed" and out["applied"][0]["status"] == "failed"
        assert out["checkpoint"] == 1
        mirror = mesh.store.one(
            "SELECT name FROM connector_institutions WHERE source='bad' AND id='org1'"
        )
        assert mirror["name"] == "合成研究所"
        assert (
            mesh.store.one(
                "SELECT id FROM connector_achievements WHERE source='bad' AND id='a9'"
            )
            is None
        )
        failure = connector.status("bad")["failure"]
        assert failure["txid"] == 2 and failure["code"] == "input"
        writer = connect(export2)
        transaction(
            writer,
            [{"table": "achievements", "key": {"id": "a9"}, "value": {"id": "a9", "title": "问题成果", "institution_id": "org1", "kind": "dataset", "year": 2026}}],
        )
        writer.close()
        out = connector.poll("bad", export2)
        assert out["status"] == "failed" and out["checkpoint"] == 1
        rebuilt = connector.rebuild("bad", export2)
        assert rebuilt["checkpoint"] == 3
        assert mesh.sources.record("bad:a9")["raw"]["kind"] == "dataset"
        stages["failed_transaction_atomic"] = {
            "reason_retained": failure["code"],
            "log_poisoned_until": "rebuild",
            "recovered_checkpoint": 3,
        }

        # 最终核对：源结果、来源版本与索引一致
        assert connector.verify("inst")["verified"]
        assert connector.verify("bad")["verified"]
        assert mesh.quality.verify()["verified"]
        preview = mesh.releases.preview()
        mesh.releases.freeze("rehearsal", preview["digest"], "rehearsal")
        assert mesh.releases.get("rehearsal")["digest"] == preview["digest"]
        stages["final_consistency"] = {"verified": True, "release": "rehearsal"}
        mesh.close()
    return {"status": "ok", "stages": stages, "offline": True}
