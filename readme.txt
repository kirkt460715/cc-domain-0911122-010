# researchmesh 本地业务处理

本项目保存业务原始记录、版本及可核对的处理历史，命令入口通过 JSON 参数调用领域操作。现有业务模块位于 researchmesh/，测试位于 tests/。数据文件与运行输出放在工作目录之外或 var/ 中。

## 运行环境

Python 3.11，标准库与 SQLite。项目不包含生产账号或真实用户数据。先执行 docker build -t researchmesh-local .，之后 docker run --rm --network none researchmesh-local；依赖在构建阶段准备，正式测试与演示不需要公网。

## 测试与语法检查

python3 -m unittest discover -s tests -v
python3 -m compileall -q researchmesh lab tests

## 现有操作示例

python3 -m researchmesh demo
python3 -m researchmesh rehearse
python3 -m researchmesh --db mesh.sqlite3 describe
python3 -m researchmesh --db mesh.sqlite3 run request.json
python3 -m researchmesh --db mesh.sqlite3 ingest lab batch1 1 records.jsonl --expected-generation 0
python3 -m researchmesh verify release.json
python3 -m unittest discover -s tests -v
python3 -m compileall -q researchmesh

## 数据库来源连接器

researchmesh/connectors/ 提供只读数据库出口连接器：先在一致读快照中取得完整成果，再按事务号衔接增量日志，逐笔以单事务更新来源记录与匹配索引。快照边界、检查点、事务账本与映射版本保存在研合库内；日志清理越过检查点时进入需要重建状态，重新快照通过 generation 检查后切换。语义与操作见 docs/数据库连接器.md，python3 -m researchmesh rehearse 演练并发快照、多表事务、关系与成果删除、日志缺口及恢复。

## 本地来源

lab/ 提供独立的合作方协议或合成文档发生器，不连接生产服务。协议、样本含义和操作见 docs/来源协议.md。运行 python3 -m lab.demo 可查看来源行为，python3 -m lab.check 依次执行测试、语法检查、业务演示和来源演示。HTTP 服务默认绑定回环地址，测试使用自动分配端口并在结束时清理。
