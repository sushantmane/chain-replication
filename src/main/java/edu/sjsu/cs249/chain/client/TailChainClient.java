package edu.sjsu.cs249.chain.client;

import edu.sjsu.cs249.chain.GetRequest;
import edu.sjsu.cs249.chain.GetResponse;
import edu.sjsu.cs249.chain.HeadResponse;
import edu.sjsu.cs249.chain.TailChainReplicaGrpc;
import edu.sjsu.cs249.chain.TailChainReplicaGrpc.TailChainReplicaBlockingStub;
import edu.sjsu.cs249.chain.TailDeleteRequest;
import edu.sjsu.cs249.chain.TailIncrementRequest;
import edu.sjsu.cs249.chain.util.Utils;
import edu.sjsu.cs249.chain.zookeeper.ZookeeperClient;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class TailChainClient {

    private static final Logger LOG = LoggerFactory.getLogger(TailChainClient.class);

    private int cXid = 1; // client transaction id
    private int RETRIES = 10;
    private int port;    // port on which client will listen for messages from tail
    private String host; // ip address of this client
    private String root; // zookeeper chain replica root znode
    private ZookeeperClient zk;
    private TailClientServer tcServer; // to handle messages from tail
    private ConcurrentMap<Integer, Boolean> xidResponseQue = new ConcurrentHashMap<>();
    private TailChainReplicaBlockingStub headStub;
    private TailChainReplicaBlockingStub tailStub;
    private long headSid;
    private long tailSid;
    private AtomicBoolean updateCtxInProgress = new AtomicBoolean(false);

    public TailChainClient(String zkAddress, String root, String host, int port) {
        this.port = port;
        this.root = root;
        this.host = host;
        this.zk = new ZookeeperClient(zkAddress, root);
        this.tcServer = new TailClientServer(xidResponseQue, port);
    }

    public void init() throws IOException, InterruptedException, KeeperException {
        startTcServer();
        connectToZk();
        Executors.newSingleThreadScheduledExecutor()
                .scheduleAtFixedRate(()-> {
                    if (!updateCtxInProgress.get()) {
                        updateStubs();;
                    }
                }, 0, 10, TimeUnit.MILLISECONDS);
    }

    public void connectToZk() throws IOException, InterruptedException, KeeperException {
        zk.connect();
    }

    public void startTcServer() throws IOException {
        tcServer.start();
    }

    private String sidToZNodeAbsPath(long sessionId) {
        return getAbsPath(Utils.getHexSid(sessionId));
    }

    private String getAbsPath(String hexSid) {
        return root + "/" + hexSid;
    }

    private int getCXid() {
        return cXid++;
    }

    // returns stub for given node
    private TailChainReplicaBlockingStub getStub(String znode)
            throws KeeperException, InterruptedException {
        byte[] data = zk.getData(znode, false);
        String target = new String(data).split("\n")[0];
        ManagedChannel ch = ManagedChannelBuilder.forTarget(target).usePlaintext().build();
        return TailChainReplicaGrpc.newBlockingStub(ch);
    }

    // destroy stub and shutdown associated channel
    private void destroyStubAndChannel(TailChainReplicaBlockingStub stub) {
        if (stub != null) {
            ManagedChannel ch = (ManagedChannel) stub.getChannel();
            ch.shutdown();
            if (!ch.isTerminated()) {
                ch.shutdownNow(); // todo: figure out better way for handling this
            }
            stub = null;
        }
    }

    private void setChainHead(long sid) throws KeeperException, InterruptedException {
        if (headSid == sid) {
            return;
        }
        if (sid == -1) {
            headSid = -1;
            headStub = null; // no nodes in chain
            return;
        }
        destroyStubAndChannel(headStub);
        headStub = getStub(sidToZNodeAbsPath(sid));
        headSid = sid;
        LOG.info("NewHead:{}", Utils.getHexSid(sid));
    }

    private void setChainTail(long sid) throws KeeperException, InterruptedException {
        if (tailSid == sid) {
            return;
        }
        if (sid == -1) {
            tailSid = -1;
            tailStub = null; // no nodes in chain
            return;
        }
        destroyStubAndChannel(tailStub);
        tailStub = getStub(sidToZNodeAbsPath(sid));
        tailSid = sid;
        LOG.info("NewTail:{}", Utils.getHexSid(sid));
    }

    void updateStubs() {
        updateCtxInProgress.set(true);
        try {
            setChainHead(zk.getHeadSid());
            setChainTail(zk.getTailSid());
        } catch (Exception e) {
            // todo: can we handle this in a better way?
            LOG.error("Failed to create stub. Exp: {}", e.getMessage());
            if (tailSid == -1 || headSid == -1) {
                LOG.info("Chain is empty."); // which is kind of okay cause operations will fail
            } else {
                LOG.error("Some znodes might have corrupt data.");
            }
            headStub = null;
            tailStub = null;
        } finally {
            updateCtxInProgress.set(false);
        }
    }

    private boolean isChainEmpty() {
        return headStub == null || tailStub == null;
    }

    // ***** CLIENT APIs *****

    /**
     * Get the value to which the specified key is mapped.
     * @param key key for which the value is to be retrieved
     */
    // case 1: rc = 0 - Success -- action: return SUCCESS
    // case 2: rc = 1 - I'm not tail -- action: retry
    // case 3: rc = 2 - Key does not exist -- action: return ENOKEY
    // case 4: (after request was sent) tail to which request was sent
    //         is down but chain is not empty -- action: return ECHNMTY
    // case 5: (after request was sent) all nodes in chain went down
    //          -- action: return ECHNMTY
    // case 6: (before sending request) chain is empty
    //          -- no nodes in replication chain -- action: return ECHNMTY
    public Response get(String key) {
        GetRequest request = GetRequest.newBuilder().setKey(key).build();
        int attempt = 0;
        do {
            if (isChainEmpty()) {
                return new Response(Response.Code.ECHNMTY, key);
            }
            try {
                GetResponse rsp = tailStub.get(request);
                int rc = rsp.getRc();
                if (rc == 0) {
                    return new Response(Response.Code.SUCCESS, key, rsp.getValue());
                }
                if (rc == 2) {
                    return new Response(Response.Code.ENOKEY, key);
                }
                if (rc == 1) {
                    System.out.println("The tail has been changed. Retrying...");
                    continue;
                }
            } catch (StatusRuntimeException e) {
                // case 5: (after request was sent) all nodes in chain went down
                if (e.getStatus().getCode() == Status.Code.UNAVAILABLE && isChainEmpty()) {
                    return new Response(Response.Code.ECHNMTY, key);
                }
                // something went wrong -- when?
                if (e.getStatus().getCode() != Status.Code.UNAVAILABLE) {
                    break;
                }
            }
        } while (attempt++ < RETRIES);
        return new Response(Response.Code.EFAULT, key);
    }

    /**
     * Increment the value of a specified key.
     * @param key   Key for which the value is to be incremented
     * @param value Value of the key will be incremented by value.
     *              If key does not exist, it will be created with this value.
     *              Negative value will decrement the value by value.
     */
    public Response increment(String key, int value) {
        return execIdOp(OpCode.INC, key, value);
    }

    /**
     * Removes the entry for the specified key.
     * @param key
     */
    public Response delete(String key) {
        return execIdOp(OpCode.DEL, key);
    }

    enum OpCode { DEL, INC }

    // increment delete operation executor
    private Response execIdOp(OpCode op, String key) {
        return execIdOp(op, key, 0);
    }

    private Response execIdOp(OpCode op, String key, int value) {
        TailDeleteRequest delReq = null;
        TailIncrementRequest incReq = null;
        int cXid = getCXid();

        if (op == OpCode.DEL) {
            delReq = TailDeleteRequest.newBuilder().setKey(key)
                    .setHost(host).setPort(port).setCxid(cXid).build();
        } else {
            incReq = TailIncrementRequest.newBuilder().setKey(key)
                    .setIncrValue(value).setHost(host).setPort(port).setCxid(cXid).build();
        }

        int attempt = 0;
        do {
            if (tcServer.isTerminated()) {
                LOG.error("TailChainClient server is down. Aborting request.");
                return new Response(Response.Code.EABORT, key);
            }
            if (isChainEmpty()) {
                return new Response(Response.Code.ECHNMTY, key);
            }
            try {
                HeadResponse rsp;
                if (op == OpCode.DEL) {
                    rsp = headStub.delete(delReq);
                } else {
                    rsp = headStub.increment(incReq);
                }
                if (rsp.getRc() == 1) {
                    System.out.println("The tail has been changed. Retrying...");
                    // wrong head
                    updateStubs();
                    continue;
                }
                // refactor: what if tail sends response before we add cXid?
                // xidResponseQue.put(cXid, false);
                // state: wait for response from tail
                while (!xidResponseQue.containsKey(cXid)) {
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException ignored) {
                    }
                    // should we check whether chain is empty?
                    // because if all node goes down, client will hung
                    // as it will keep waiting for response from the tail
                }
                // state: got response from tail
                xidResponseQue.remove(cXid);
                return new Response(Response.Code.SUCCESS, key);
            } catch (StatusRuntimeException e) {
                // case 5: (after request was sent) all nodes in chain went down
                if (e.getStatus().getCode() == Status.Code.UNAVAILABLE && isChainEmpty()) {
                    return new Response(Response.Code.ECHNMTY, key);
                }
                // something went wrong -- when?
                if (e.getStatus().getCode() == Status.Code.UNKNOWN) {
                    LOG.error("Please check whether all server znodes has correct data.");
                    break;
                }
                // something went wrong -- when?
                if (e.getStatus().getCode() != Status.Code.UNAVAILABLE) {
                    break;
                }
                // if response is unavailable but chain is not empty,
                // try sending request to updated head node
            }
        } while (attempt++ < RETRIES);
        return new Response(Response.Code.EFAULT, key);
    }
}
