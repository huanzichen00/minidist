# MiniDist 项目架构

本文记录当前 main 分支的实现。MiniDist 由 Dynamo 风格 KV 和对象存储两部分组成。KV 提供版本化 quorum 读写、本地持久化和故障收敛；对象层使用 KV 保存 metadata，文件内容进入独立的 distributed chunk data plane。

## 总览

~~~mermaid
flowchart TB
    Client[Client]
    Main[cmd/node]
    Handler[Node.Handler]
    Node[Node]

    subgraph KV[Distributed KV]
        Ring[hashring.Ring]
        Memory[store.Memory]
        WAL[WAL + Snapshot]
        Hint[Hinted Handoff]
        FD[Failure Detector]
    end

    subgraph Object[Object Storage]
        ObjectStore[object.Store]
        Metadata[Object Metadata]
        ChunkPlane[Distributed Chunk Plane]
        ChunkStore[chunk.Store]
        Repair[Chunk Read Repair]
        GC[Cluster Chunk GC]
    end

    Client --> Main --> Handler --> Node

    Node --> Ring
    Node --> Memory --> WAL
    Node --> Hint
    Node --> FD

    Node --> ObjectStore
    ObjectStore -->|metadata| Node
    ObjectStore -->|chunk bytes| ChunkPlane
    ChunkPlane --> ChunkStore
    ChunkPlane --> Repair
    Metadata --> GC
    ChunkStore --> GC
~~~

Node 是协调层。它负责 ring placement、quorum、副本 HTTP、membership、rebalance、drain、chunk repair 和 GC。object.Store 负责对象分块、metadata 编解码和 chunk 顺序。

~~~text
object metadata
    -> distributed KV
    -> Version / quorum / WAL / snapshot

chunk bytes
    -> SHA-256
    -> consistent hash
    -> N owners
    -> write quorum / read fallback / read repair
    -> local chunk.Store
~~~

## 目录职责

~~~text
minidist/
├── cmd/node/main.go
├── internal/hashring/
│   └── ring.go
├── internal/store/
│   ├── memory.go
│   ├── wal.go
│   └── snapshot.go
├── internal/chunk/
│   ├── store.go
│   └── chunker.go
├── internal/object/
│   ├── metadata.go
│   └── store.go
└── internal/node/
    ├── node.go
    ├── handler.go
    ├── replication.go
    ├── version.go
    ├── chunk.go
    ├── gc.go
    ├── hint.go
    ├── membership.go
    ├── probe.go
    ├── gossip.go
    ├── background.go
    ├── cluster_membership.go
    ├── drain.go
    ├── rebalance.go
    └── debug.go
~~~

主要文件：

- node.go：Node 依赖、N/W/R、store 初始化
- handler.go：外部、内部、管理 HTTP handler
- replication.go：KV quorum 读写、删除、read repair
- version.go：版本分配
- chunk.go：distributed chunk Put/Get、内部 chunk 协议、chunk read repair
- gc.go：local mark、cluster mark collection、distributed sweep
- cluster_membership.go：ClusterConfig、AddMember、RemoveMember
- drain.go：future-ring drain
- rebalance.go：KV / chunk rebalance
- store/memory.go：版本化 KV、snapshot 入口
- store/wal.go：WAL record、CRC32、replay
- store/snapshot.go：snapshot 格式和原子保存
- chunk/store.go：本地 content-addressed chunk store、Sweep
- object/store.go：对象上传、下载、metadata commit、删除

## Node 核心状态

~~~mermaid
classDiagram
    class Node {
      -string addr
      -Ring ring
      -Memory store
      -ChunkStore chunks
      -http.Client client
      -int replicas
      -int writeQuorum
      -int readQuorum
      -atomic.Uint64 version
      -atomic.Uint64 configVersion
      -Mutex configMu
      -hintStore hints
      -failureDetector fd
      -object.Store objects

      +Put(ctx, key, data)
      +Get(ctx, key)
      +Delete(ctx, key)
      +PutChunk(ctx, data)
      +GetChunk(ctx, id)
      +RunGC(ctx, gracePeriod)
    }

    class Ring {
      +Add(node)
      +Remove(node)
      +Get(key)
      +GetN(key, n)
      +Members()
    }

    class Memory {
      +Set(key, value)
      +Get(key)
      +Snapshot()
      +DeleteIfMatch(key, version)
    }

    class LocalChunkStore {
      +Put(data)
      +Get(id)
      +Delete(id)
      +Exists(id)
      +IDs()
      +Sweep(live, cutoff)
    }

    class ObjectStore {
      +Put(ctx, name, reader, chunkSize)
      +Get(ctx, name, writer)
      +Delete(ctx, name)
      +Metadata(ctx, name)
      +WriteTo(ctx, metadata, writer)
    }

    Node --> Ring
    Node --> Memory
    Node --> LocalChunkStore
    Node --> ObjectStore
~~~

ring 同时决定 KV 和 chunk 的副本位置。正式 membership 来自 ring / ClusterConfig。failure detector 维护健康状态，不直接修改正式成员集合。

## KV 数据路径

### 写入

~~~mermaid
sequenceDiagram
    participant C as Client
    participant N as Coordinator
    participant R as Ring
    participant A as Replica A
    participant B as Replica B
    participant D as Replica C
    participant H as Hint Store

    C->>N: PUT /kv/k
    N->>R: GetN(k, 3)
    R-->>N: A, B, C

    par 读取已有版本
        N->>A: GET /internal/kv/k
        N->>B: GET /internal/kv/k
        N->>D: GET /internal/kv/k
    end

    N->>N: nextVersion

    par 写全部副本
        N->>A: PUT /internal/kv/k
        N->>B: PUT /internal/kv/k
        N->>D: PUT /internal/kv/k
    end

    alt success >= W
        N-->>C: success
    else success < W
        N-->>C: 503
    end

    opt 某副本失败
        N->>H: save hint
    end
~~~

版本结构：

~~~text
Version
├── Counter
└── NodeID
~~~

比较顺序为 Counter、NodeID。NodeID 在 Counter 相同的时候提供稳定顺序。

DELETE 走同一套复制路径，Value.Deleted=true。新 tombstone 会压过旧版本的数据。

### 读取

~~~mermaid
sequenceDiagram
    participant C as Client
    participant N as Coordinator
    participant A as Replica A
    participant B as Replica B
    participant D as Replica C

    C->>N: GET /kv/k

    par 读取副本
        N->>A: GET /internal/kv/k
        N->>B: GET /internal/kv/k
        N->>D: GET /internal/kv/k
    end

    N->>N: 选择最大 Version

    alt successful responses < R
        N-->>C: 503
    else latest is tombstone / missing
        N-->>C: 404
    else
        N-->>C: latest Data
    end

    opt 发现旧副本或缺失副本
        N->>A: repair
        N->>B: repair
        N->>D: repair
    end
~~~

Get 返回 ([]byte, bool, error)。bool=false 且 error=nil 表示正常的 key 不存在。

## WAL 与 Snapshot

store.Memory 的持久化顺序：

~~~text
Set
 |
 +-> WAL append
 |      |
 |      +-> record length
 |      +-> CRC32
 |      +-> payload
 |
 +-> memory map update
 |
 +-> MaybeSnapshot
~~~

WAL：

- header magic：MDWL
- 每条 record 带长度和 CRC32
- replay 遇到 torn tail 时截断尾部
- checksum / header 损坏直接返回错误

Snapshot：

- header magic：MDSP
- 保存 payload length 和 CRC32
- 保存 Data 和 MaxVersionCounter
- 临时文件写入、fsync、rename
- snapshot 成功后 WAL truncate 到 header

节点启动：

~~~text
load snapshot
    |
    v
open WAL
    |
    v
replay WAL
    |
    v
恢复 data + max version counter
~~~

chunk bytes 不进入 KV WAL。

## Object Storage

### Metadata

Metadata 结构：

~~~text
Metadata
├── Name
├── Size
├── ChunkSize
└── Chunks[]
    ├── ID
    └── Size
~~~

metadata key：

~~~text
SHA-256(object name)
    |
    v
object:meta:<hash>
~~~

metadata 最终通过 Node.Put / Node.Get / Node.Delete 操作，继承 KV 的版本、quorum 和持久化语义。

### Chunk

chunk ID：

~~~text
SHA-256(chunk bytes)
~~~

本地路径按 ID 前缀分层：

~~~text
root/<id[0:2]>/<id[2:4]>/<id>
~~~

Put 流程：

~~~text
计算 ID
   |
   +-> 已有合法文件
   |      |
   |      +-> refresh mtime
   |      +-> return
   |
   +-> CreateTemp
          |
          +-> Write
          +-> fsync
          +-> Close
          +-> Rename
~~~

已有文件 checksum 不正确时，Put 会重新写入正确内容。

chunk.Store 使用 gcMu RWMutex：

- Put / Get / Exists / IDs 获取读锁
- Delete / Sweep 获取写锁

Sweep 执行期间不会和普通 chunk 访问同时修改文件。

### 上传

~~~mermaid
sequenceDiagram
    participant C as Client
    participant N as Node
    participant O as object.Store
    participant R as Ring
    participant A as Owner A
    participant B as Owner B
    participant D as Owner C
    participant KV as Distributed KV

    C->>N: PUT /objects/file.bin
    N->>O: Put

    loop 每个 chunk
        O->>N: PutChunk(bytes)
        N->>R: GetN(chunkID, N)

        par replication
            N->>A: PUT /internal/chunks/id
            N->>B: PUT /internal/chunks/id
            N->>D: PUT /internal/chunks/id
        end

        N-->>O: ChunkInfo after W quorum
    end

    O->>KV: Put metadata
    KV-->>O: metadata committed
    O-->>C: Metadata JSON
~~~

metadata 是对象可见性的 commit point。所有 chunks 先写，metadata 最后提交。

### 下载与 Chunk Read Repair

GetChunk 根据 ring 取得预期 owners，依次读取副本。第一个通过 SHA-256 校验的副本作为结果返回。

成功读取后会启动后台 repair：

~~~text
valid chunk data
    |
    +-> expected owner A: healthy -> skip
    +-> expected owner B: missing -> PUT repair
    +-> expected owner C: checksum error -> PUT repair
~~~

repair 使用独立后台 context，当前为 best-effort。读取请求无需等待 repair 完成。

### 覆盖与删除

覆盖：

~~~text
write new chunks
    |
    v
commit new metadata
    |
    v
old chunks lose metadata references
    |
    v
later GC
~~~

删除：

~~~text
Node.Delete(metadata key)
    |
    v
metadata tombstone
    |
    v
object hidden
    |
    v
old chunks later GC
~~~

chunk 支持内容去重。删除对象时不会立即删除其 chunks，避免误删其他对象仍然引用的相同内容。

## Membership 与数据迁移

### ClusterConfig

~~~text
ClusterConfig
├── Version
└── Members[]
~~~

configVersion 单独维护，不和 KV Version 混用。

AddMember / RemoveMember 在 coordinator 上由 configMu 串行化。当前仍允许不同 coordinator 同时发起配置操作，这部分计划交给 Raft control plane。

### AddMember

~~~text
POST /admin/members
    |
    v
build next ClusterConfig
    |
    v
sync config to members
    |
    v
rebalance
    |
    +-> KV
    +-> chunks
~~~

KV rebalance 可以在新 owners 全部写成功且版本仍匹配时删除旧本地副本。

chunk rebalance 采用 copy-only：

~~~text
local chunk IDs
    |
    v
current Ring.GetN
    |
    v
copy to all expected owners
    |
    v
keep local old copy
~~~

### RemoveMember

~~~text
futureMembers = current - target
    |
    v
sendDrain(target, futureMembers)
    |
    v
target builds future ring
    |
    +-> drain KV
    +-> drain chunks
    |
    v
all migration succeeds
    |
    v
sync new ClusterConfig
    |
    v
remaining members rebalance
~~~

drain 失败时成员配置保持原状。退出节点上的 chunk 文件不会在 drain 阶段主动删除。

## Cluster Chunk GC

GC 处理上传失败、overwrite、delete 和 copy-only rebalance 留下的 orphan chunks。

### Local Mark

每个节点扫描本地 Memory Snapshot：

~~~text
all KV entries
    |
    +-> key prefix != object:meta: -> skip
    +-> tombstone -> skip
    +-> live metadata -> decode JSON
                           |
                           v
                     collect chunk IDs
~~~

metadata JSON 解析失败会终止 mark。本轮 GC 不会继续。

### Cluster Mark

Coordinator 读取当前 configVersion 和 ring members，然后请求：

~~~text
GET /internal/gc/mark
~~~

每个成员返回：

~~~text
ConfigVersion
LiveChunks[]
~~~

Coordinator 对所有 LiveChunks 求并集。任一成员不可达、任一 configVersion 不一致、mark 期间本地 configVersion 变化，当前 GC 都会停止。

### Sweep

Coordinator 记录 gcStart，cutoff 计算为：

~~~text
gcStart - gracePeriod
~~~

随后发送：

~~~text
POST /internal/gc/sweep
~~~

请求内容：

~~~text
ConfigVersion
LiveChunks[]
Cutoff
~~~

每个节点在物理删除前再次检查 configVersion，然后调用 chunk.Store.Sweep。

删除条件：

~~~text
chunk ID not in Global Live Set
AND
mtime < cutoff
~~~

默认 grace period 为 10 分钟。

Put 命中已存在 chunk 时会刷新 mtime，因此近期被新上传复用的 chunk 会继续保留。

当前 sweep 对已经成功处理的节点不做回滚。GC 可以重复运行，后续轮次继续清理剩余 orphan chunks。

## HTTP 接口

| 路径 | 方法 | 用途 |
|---|---:|---|
| /kv/{key} | GET | quorum read + KV read repair |
| /kv/{key} | PUT | quorum write |
| /kv/{key} | DELETE | quorum tombstone write |
| /objects/{name} | PUT | 上传对象 |
| /objects/{name} | GET | 下载对象 |
| /objects/{name} | DELETE | metadata tombstone |
| /internal/kv/{key} | GET | 本地 KV 读取 |
| /internal/kv/{key} | PUT | 本地 KV 写入 |
| /internal/chunks/{id} | GET | 本地 chunk 读取 |
| /internal/chunks/{id} | PUT | 本地 chunk 写入 |
| /internal/ping | GET | 直接探测 |
| /internal/ping-request | POST | 间接探测 |
| /internal/gossip | POST | failure detector gossip |
| /internal/members/sync | PUT | 同步 ClusterConfig |
| /internal/drain | POST | future-ring drain |
| /internal/rebalance | POST | KV / chunk rebalance |
| /internal/gc/mark | GET | 返回 local live chunk set |
| /internal/gc/sweep | POST | 执行 local chunk sweep |
| /admin/members | POST / DELETE | 添加或移除成员 |
| /admin/gc | POST | 手动触发 cluster chunk GC |
| /internal/debug/kv/{key} | GET / PUT | 调试本地 KV |
| /internal/debug/hints | GET | 查看 hints |
| /internal/debug/members | GET | 查看成员和 FD 状态 |

内部 chunk PUT 会重新计算 SHA-256 并检查 URL 中的 ID。内部 chunk GET 返回 application/octet-stream。

## 后台任务

RunBackground 当前运行：

~~~text
flushHints
probeOnce
gossipOnce
~~~

KV hints 会持续重放到原目标副本。

failure detector：

~~~text
direct ping
    |
    +-> success -> alive
    |
    +-> fail
          |
          v
     indirect ping
          |
          +-> success -> alive
          +-> fail -> suspect / dead
~~~

Gossip 传播 incarnation / version 状态。节点收到针对自己的非 alive 状态后会提高 incarnation 并重新声明 alive。

chunk repair 由成功的 GetChunk 触发，不属于 RunBackground 固定周期任务。

## 一致性与并发边界

| 主题 | 当前实现 |
|---|---|
| KV placement | Ring.GetN |
| KV write | N/W quorum |
| KV read | N/R quorum + max Version |
| KV delete | versioned tombstone |
| KV convergence | read repair + hinted handoff |
| metadata | distributed KV |
| chunk identity | SHA-256(content) |
| chunk write | expected owners + W quorum |
| chunk read | owner fallback + checksum |
| chunk repair | successful read 后 best-effort repair |
| chunk rebalance | copy-only |
| chunk drain | future-ring full copy |
| object overwrite | chunks first, metadata last |
| object delete | metadata tombstone |
| chunk GC | global mark union + grace period + configVersion checks |
| membership | ClusterConfig + configVersion |
| control plane | coordinator-driven HTTP sync |

当前需要注意：

- metadata 使用 Dynamo KV 语义，没有线性一致保证
- chunk write 达到 W 后仍等待本轮 owner 请求全部返回
- chunk 没有独立 hinted handoff
- chunk repair 依赖读取触发
- RemoveMember 没有冻结整个集群的并发客户端写入
- GC 遇到成员不可达或版本不一致时停止
- configVersion 检查提供保守 barrier，membership 和 GC 尚未共享统一日志
- 多 coordinator 的 membership change 缺少全局顺序

## 下一阶段：Raft Control Plane

下一步把 ClusterConfig 的变更放进 Raft 日志。

目标路径：

~~~text
admin membership request
    |
    v
Raft leader
    |
    v
append ClusterConfig command
    |
    v
majority replicate
    |
    v
commitIndex advances
    |
    v
apply to membership state machine
    |
    v
ring / configVersion update
~~~

第一阶段只让 Raft 管 control-plane metadata。KV 和 chunk 数据面继续使用当前的 Dynamo / consistent-hash 设计。

计划先实现：

~~~text
Term
LogEntry
CommitIndex
LastApplied
RequestVote
AppendEntries
Apply loop
~~~

完成最小 Raft 后，再把现有 AddMember / RemoveMember 的配置提交接到状态机。
