# MiniDist

MiniDist 是一个用 Go 实现的分布式存储练习项目。当前包含两条主要数据路径：

- Dynamo 风格的分布式 KV
- 建立在 KV metadata 之上的分布式对象存储

项目直接实现一致性哈希、quorum、版本、read repair、hinted handoff、故障探测、成员变更、WAL / snapshot、chunk replication、rebalance、drain 和 GC 等机制，尽量保持数据流和协议可追踪。

详细设计见 [ARCHITECTURE.md](ARCHITECTURE.md)。

## 当前架构

~~~mermaid
flowchart LR
    Client[Client]
    Node[Node / HTTP]

    subgraph KV[Distributed KV]
        Ring[Consistent Hash Ring]
        Quorum[N=3 / W=2 / R=2]
        Store[Versioned Memory Store]
        Persist[WAL + Snapshot]
    end

    subgraph Object[Object Storage]
        Obj[object.Store]
        Meta[Object Metadata]
        Chunk[Distributed Chunk Plane]
        Disk[Local Chunk Store]
        GC[Mark-and-Sweep GC]
    end

    Client --> Node
    Node --> Ring
    Ring --> Quorum
    Quorum --> Store
    Store --> Persist

    Node --> Obj
    Obj -->|metadata| KV
    Obj -->|chunk bytes| Chunk
    Chunk --> Disk
    Meta --> GC
    Disk --> GC
~~~

对象数据分成两部分：

~~~text
metadata   -> distributed KV
chunk data -> distributed chunk plane -> local filesystem
~~~

metadata 使用现有 KV 的版本、quorum、WAL 和 snapshot。文件内容按固定大小切块，chunk ID 为 SHA-256(content)，再根据 hash ring 复制到多个节点。

## 已实现功能

- 一致性哈希环与虚拟节点，默认每个真实节点 100 个 virtual nodes
- KV 默认 N=3 / W=2 / R=2
- Node.Put / Node.Get / Node.Delete
- (Counter, NodeID) 版本排序
- tombstone 删除
- KV read repair
- Hinted handoff
- failure detector：alive / suspect / dead
- 直接 ping、间接 ping、gossip、incarnation、self-refutation
- ClusterConfig + configVersion
- AddMember 后 KV / chunk rebalance
- RemoveMember 前 future-ring drain
- WAL、CRC32、snapshot、启动恢复、torn-tail repair
- 默认 4 MiB 固定大小 chunking
- SHA-256 content-addressed chunk store
- 临时文件 + fsync + rename 的 chunk 落盘
- 同一 chunk 并发写入
- chunk checksum 校验
- chunk write quorum
- distributed chunk read fallback
- chunk read repair
- copy-only chunk rebalance
- future-ring chunk drain
- object metadata / manifest
- 跨节点对象上传与下载
- object overwrite
- object delete：metadata tombstone
- cluster-wide mark-and-sweep chunk GC
- GC grace period、configVersion barrier、local sweep 锁

## Object Storage

### 上传

~~~text
PUT /objects/file.bin
        |
        v
object.Store.Put
        |
        +-> Reader 按 4 MiB 分块
        |      |
        |      +-> SHA-256(chunk)
        |      +-> Node.PutChunk
        |              |
        |              +-> Ring.GetN
        |              +-> 并发复制到 owners
        |              +-> W quorum
        |
        +-> Metadata{Name, Size, ChunkSize, Chunks}
               |
               +-> JSON
               +-> Node.Put
               +-> distributed KV quorum write
~~~

metadata 最后提交。读取端只会看到已经完成 metadata commit 的对象。

上传中途失败时，已经落盘的 chunk 可能暂时没有 metadata 引用。GC 会在 grace period 之后处理这些 orphan chunks。

### 下载

~~~text
GET /objects/file.bin
        |
        v
Node.Get(metadata key)
        |
        v
Metadata
        |
        v
按顺序读取 chunk
        |
        +-> Ring.GetN(chunk ID)
        +-> owner fallback
        +-> SHA-256 校验
        +-> 缺失 / 损坏副本触发后台 repair
        |
        v
HTTP ResponseWriter
~~~

chunk 由内容哈希确定正确结果。GetChunk 找到 checksum-valid 副本后即可返回，同时后台尝试修复预期 owner 上缺失或损坏的副本。

### 覆盖与删除

覆盖对象时先写新 chunks，再提交新 metadata。旧 chunks 暂时保留，等待 GC。

删除对象时写 metadata tombstone：

~~~text
DELETE /objects/file.bin
        |
        v
metadata tombstone
        |
        v
对象对外不可见
        |
        v
旧 chunks 等待 GC
~~~

### Chunk GC

手动触发：

~~~bash
curl -X POST http://127.0.0.1:8081/admin/gc
~~~

GC 流程：

~~~text
/admin/gc
   |
   v
收集所有成员的 local MARK
   |
   +-> 扫描 object metadata
   +-> 合并 referenced chunk IDs
   +-> 校验 configVersion
   |
   v
Global Live Set
   |
   v
向所有成员发送 SWEEP
   |
   +-> 再次校验 configVersion
   +-> live chunk 保留
   +-> grace period 内的 chunk 保留
   +-> 其余旧 orphan chunk 删除
~~~

默认 grace period 为 10 分钟。

chunk.Store 在 Put 复用已有 chunk 时会刷新 mtime。Sweep 与 Put / Get 通过 RWMutex 隔离，避免物理删除和正常访问同时操作同一文件。

## 快速开始

需要 Go 1.26.2 或更高版本。

启动三个节点：

~~~bash
go run ./cmd/node -addr 127.0.0.1:8081 \
  -cluster 127.0.0.1:8081,127.0.0.1:8082,127.0.0.1:8083
~~~

~~~bash
go run ./cmd/node -addr 127.0.0.1:8082 \
  -cluster 127.0.0.1:8081,127.0.0.1:8082,127.0.0.1:8083
~~~

~~~bash
go run ./cmd/node -addr 127.0.0.1:8083 \
  -cluster 127.0.0.1:8081,127.0.0.1:8082,127.0.0.1:8083
~~~

## KV 使用

写入：

~~~bash
curl -X PUT --data 'hello' http://127.0.0.1:8081/kv/foo
~~~

读取：

~~~bash
curl http://127.0.0.1:8082/kv/foo
~~~

删除：

~~~bash
curl -X DELETE http://127.0.0.1:8083/kv/foo
~~~

查看成员状态：

~~~bash
curl http://127.0.0.1:8081/internal/debug/members
~~~

查看 hints：

~~~bash
curl http://127.0.0.1:8081/internal/debug/hints
~~~

## Object 使用

上传：

~~~bash
curl -X PUT --data-binary @example.bin \
  http://127.0.0.1:8081/objects/example.bin
~~~

下载：

~~~bash
curl http://127.0.0.1:8082/objects/example.bin \
  -o downloaded.bin
~~~

删除：

~~~bash
curl -X DELETE http://127.0.0.1:8083/objects/example.bin
~~~

校验下载内容：

~~~bash
shasum -a 256 example.bin downloaded.bin
~~~

## 成员变更

添加节点：

~~~bash
go run ./cmd/node -addr 127.0.0.1:8084 \
  -cluster 127.0.0.1:8081,127.0.0.1:8082,127.0.0.1:8083,127.0.0.1:8084
~~~

~~~bash
curl -X POST http://127.0.0.1:8081/admin/members \
  -H 'Content-Type: application/json' \
  -d '{"member":"127.0.0.1:8084"}'
~~~

成员同步完成后，各节点根据新 ring 执行 KV / chunk rebalance。chunk rebalance 保留旧副本，后续由 GC 清理无引用文件。

移除节点：

~~~bash
curl -X DELETE http://127.0.0.1:8081/admin/members \
  -H 'Content-Type: application/json' \
  -d '{"member":"127.0.0.1:8084"}'
~~~

RemoveMember 先让目标节点按 future ring 迁移本地 KV 和 chunks。drain 成功后再发布新的 membership，并让剩余节点执行 rebalance。

## 本地数据文件

未显式指定 -wal 时，节点会生成：

~~~text
minidist-127.0.0.1_8081.wal
minidist-127.0.0.1_8081.wal.snapshot
minidist-127.0.0.1_8081.wal.chunks/
~~~

用途：

- .wal：KV WAL
- .wal.snapshot：KV snapshot
- .wal.chunks/：本节点 chunk files

启动恢复顺序：

~~~text
load snapshot
    |
    v
replay WAL
    |
    v
恢复 Memory + max version counter
~~~

大文件内容直接进入 chunk store，不写入 KV WAL。

## 测试

~~~bash
go test ./...
go test -race ./...
go vet ./...
~~~

跨节点对象 round trip：

~~~bash
dd if=/dev/urandom of=/tmp/minidist-test.bin bs=1m count=10

curl -X PUT --data-binary @/tmp/minidist-test.bin \
  http://127.0.0.1:8081/objects/test.bin

curl http://127.0.0.1:8082/objects/test.bin \
  -o /tmp/minidist-downloaded.bin

shasum -a 256 /tmp/minidist-test.bin /tmp/minidist-downloaded.bin
~~~

RemoveMember 可以用四节点集群测试：上传对象，确认目标节点持有 chunk，执行成员移除，停止该节点，再从剩余节点下载并比较 SHA-256。

GC 可以通过 overwrite / delete 制造 orphan chunks，等待 grace period 后调用 POST /admin/gc，检查返回的 scanned / kept / deleted 统计。

## 当前限制

- ClusterConfig 还没有 Raft / leader，多个 coordinator 之间缺少强一致的配置顺序
- membership change、rebalance、drain 和 GC 依赖 configVersion 做校验，没有统一的 control-plane log
- chunk 没有独立 hinted handoff；读取时的 repair 为 best-effort
- chunk rebalance 采用 copy-only，旧 placement 由 GC 逐步回收
- object metadata 继承 Dynamo KV 的一致性语义，不提供线性一致读写
- RemoveMember 仍假设迁移期间 membership 相对稳定
- GC 是保守型 mark-and-sweep；节点不可达或版本不一致时整轮失败
- 默认对象读取以 4 MiB chunk 为单位进入内存，尚未改成文件句柄级 streaming

## 下一步

下一阶段准备把 membership 放进 Raft control plane：

~~~text
Raft leader
    |
    v
propose ClusterConfig command
    |
    v
majority commit
    |
    v
state machine apply
    |
    v
统一 membership / configVersion 顺序
~~~

先实现最小 Raft 日志和状态机，再接入现有 ClusterConfig。数据面仍保留当前 Dynamo KV 和 chunk replication。
