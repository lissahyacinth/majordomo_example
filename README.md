# Majordomo Example

Example of the Majordomo pattern expressed in https://zguide.zeromq.org/docs/chapter4/#Service-Oriented-Reliable-Queuing-Majordomo-Pattern

Main differences extend to how the broker and services are referenced by the workers. In the C variant each worker holds
a pointer to the broker, which would extend to the Broker and the Worker holding references to each other. To avoid
this, although it could have been replicated using Reference Counting, and maintain behaviour, there are structural differences
in the C Broker and the Rust Broker.

```c
# Reference C Broker
typedef struct {
    zctx_t *ctx;                //  Our context
    void *socket;               //  Socket for clients & workers
    int verbose;                //  Print activity to stdout
    char *endpoint;             //  Broker binds to this endpoint
    zhash_t *services;          //  Hash of known services
    zhash_t *workers;           //  Hash of known workers
    zlist_t *waiting;           //  List of waiting workers
    uint64_t heartbeat_at;      //  When to send HEARTBEAT
} broker_t;
```

```rust
// Rust Broker
struct MajordomoBroker {
    context: Context,
    socket: Socket,   
    endpoint: String, 
    services: HashMap<String, Service>, // Difference - we're mapping from name to service, not just hashset of names 
    workers_to_service: HashMap<String, String>, // Difference - We're mapping from Worker Name -> Service Name.
    // Difference - the broker doesn't hold a list of waiting workers, we rely on our services for that
    heartbeat_at: Instant,
}
```

## Running
In separate windows run
* `cargo run -- broker`
* `cargo run -- worker`
* `cargo run -- client`

The client should communicate with the main workers, and eventually return the following rough output;

```
100000 requests/replies processed
Total time: 18.75s
Requests per second: 5332.83
```