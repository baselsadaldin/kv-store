# kv-store

A simple thread-safe in-memory key-value store written in Go, with a small
interactive CLI for trying it out.

## Packages

- [`store`](./store) — the core `Store` type: `Set`, `Get`, `Delete`, `Has`, `Len`, `Keys`.
- [`cmd/kvstore`](./cmd/kvstore) — an interactive REPL over the store.

## Usage

```go
s := store.New()
s.Set("foo", "bar")

v, err := s.Get("foo")
if err != nil {
    // errors.Is(err, store.ErrKeyNotFound)
}
```

## Running the CLI

```sh
go run ./cmd/kvstore
```

```
kvstore> commands: SET key value | GET key | DELETE key | KEYS | EXIT
> SET foo bar
OK
> GET foo
bar
> DELETE foo
OK
> GET foo
(nil)
```

## Testing

```sh
go test ./...
```
