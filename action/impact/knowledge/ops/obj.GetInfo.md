# obj.GetInfo

Read an object's metadata (last message on its meta subject).

- **Tier: low** — score 3 (invocation 3 + 2 × steady 0)
- Group: Object Store
- Symbol: `jetstream.ObjectStore.GetInfo` — `func(ctx context.Context, name string, opts ...GetObjectInfoOpt) (*ObjectInfo, error)`
- Pattern: request-reply; round trips: 1
- Disk I/O: read; Raft: none; server state: none; scan: none

## Flow

- **1.** [object.go/GetInfo](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/object.go) reads the object's metadata by requesting the last message on its meta subject $O.\<bucket>.M.\<name-encoded>, issued as the last-per-subject get in [stream.go/GetLastMsgForSubject](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/stream.go).
- **2.** OBJ buckets set AllowDirect, so the request rides DIRECT.GET.\<stream>.\<meta-subject> and [stream.go/processDirectGetLastBySubjectRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/stream.go) serves it from any up-to-date replica off a queue group, bypassing the $JS.API pool and Raft.
- **3.** Through [filestore.go/LoadLastMsg](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/filestore.go), the store resolves the last sequence for the meta subject via the per-subject index and loads that single message; a cold block faults its whole 1-8 MB extent into cache.
- **4.** [stream.go/getDirectRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/stream.go) returns the single meta message and [object.go/GetInfo](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/object.go) unmarshals it into ObjectInfo.

## In practice

- **Reads metadata, not the object** — GetInfo fetches only the rollup meta entry — size, chunk count, digest — never the object bytes, so its cost is flat regardless of how large the object is. Use it to check existence or size before deciding whether to pull the full object.
- **Scales reads across replicas** — Direct-get lets any up-to-date replica answer the request off a queue group, so metadata reads never contend for the leader or the shared API workers. In a multi-replica bucket that read load spreads across the followers, keeping GetInfo cheap under concurrent access.

## Evidence

- Client: [jetstream/object.go — (\*obs).GetInfo](https://github.com/nats-io/nats.go/blob/654ca4ea87cbdc68a0f7cdee2aa0b87f5509b30d/jetstream/object.go)
- Server: [server/jetstream\_api.go — (\*Server).jsMsgGetRequest](https://github.com/nats-io/nats-server/blob/1be499156d9bc757ea08bd148608b622e38b7514/server/jetstream_api.go)
