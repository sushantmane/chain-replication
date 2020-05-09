Chain Replication
=================


---
### Implementation

#### Server (Replica Nodes)

```bash
java -cp target/chain-replication-1.0-SNAPSHOT-jar-with-dependencies.jar edu.sjsu.cs249.chain.server.ServerMain -z 192.168.56.111:9999 -r /tail-chain -p 5454
```
#### Client

```bash
java -cp target/chain-replication-1.0-SNAPSHOT-jar-with-dependencies.jar edu.sjsu.cs249.chain.client.ClientMain -z 192.168.56.111:9999 -r /tail-chain -p 5454 --nif en0 --get ben
```

---
### References
