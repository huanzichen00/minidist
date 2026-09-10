# MiniDist

MiniDist 是一个用 Go 实现的、基于 HTTP 的内存 KV 集群。节点使用一致性哈希选择副本，并通过 quorum 读写提供基础的数据一致性能力。

## 已实现功能

- 一致性哈希环与虚拟节点（默认每个节点 100 个虚拟节点）
- `N=3 / W=2 / R=2` 的 quorum 读、写、删除
- 版本化数据：以 `(Counter, NodeID)` 排序，删除使用 tombstone，避免旧数据复活
- 并发副本访问、读修复（read repair）
- Hinted handoff：副本暂时不可用时保存最新 hint，并在后台重试
- 直接 ping、间接 ping 与 failure detector（alive / suspect / dead）
- Gossip 同步故障探测状态，并支持 incarnation 自我反驳
- 集群扩容：成员同步、rebalance 与旧副本安全清理
- 节点移除流程：drain 到 future ring、成员同步和 rebalance
- 调试接口：查看本地 KV、hints、ring 成员和节点健康状态

完整设计见 [ARCHITECTURE.md](ARCHITECTURE.md)。

## 快速开始

需要 Go 1.26.2 或更高版本。打开三个终端，在项目根目录分别启动三个节点：

```bash
go run ./cmd/node -addr 127.0.0.1:8081 \
  -cluster 127.0.0.1:8081,127.0.0.1:8082,127.0.0.1:8083
```

```bash
go run ./cmd/node -addr 127.0.0.1:8082 \
  -cluster 127.0.0.1:8081,127.0.0.1:8082,127.0.0.1:8083
```

```bash
go run ./cmd/node -addr 127.0.0.1:8083 \
  -cluster 127.0.0.1:8081,127.0.0.1:8082,127.0.0.1:8083
```

写入、读取和删除：

```bash
curl -X PUT --data 'hello' http://127.0.0.1:8081/kv/foo
curl http://127.0.0.1:8082/kv/foo
curl -X DELETE http://127.0.0.1:8083/kv/foo
```

查看节点成员与健康状态：

```bash
curl http://127.0.0.1:8081/internal/debug/members
```

添加节点时，先用完整集群列表启动新节点：

```bash
go run ./cmd/node -addr 127.0.0.1:8084 \
  -cluster 127.0.0.1:8081,127.0.0.1:8082,127.0.0.1:8083,127.0.0.1:8084
```

再向现有节点发起成员同步和 rebalance：

```bash
curl -X POST http://127.0.0.1:8081/admin/members \
  -H 'Content-Type: application/json' \
  -d '{"member":"127.0.0.1:8084"}'
```

运行测试：

```bash
go test ./...
go test -race ./...
go vet ./...
```
