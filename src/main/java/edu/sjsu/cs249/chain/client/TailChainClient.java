package edu.sjsu.cs249.chain.client;

import edu.sjsu.cs249.chain.GetRequest;
import edu.sjsu.cs249.chain.GetResponse;
import edu.sjsu.cs249.chain.TailChainReplicaGrpc;
import edu.sjsu.cs249.chain.TailChainReplicaGrpc.TailChainReplicaBlockingStub;
import edu.sjsu.cs249.chain.TailDeleteRequest;
import edu.sjsu.cs249.chain.TailIncrementRequest;
import edu.sjsu.cs249.chain.util.Utils;
import edu.sjsu.cs249.chain.zookeeper.ZookeeperClient;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.net.InetSocketAddress;

public class TailChainClient {

    private ManagedChannel channel;
    private ZookeeperClient zk;
    private TailChainReplicaBlockingStub headStub;
    private TailChainReplicaBlockingStub tailStub;

    private String host; // ip address of this client
    private int port;    // port on which client will listen for messages from tail
    private long sid;    // zookeeper session id
    private String root; // zookeeper chain replica root znode
    private int cXid = 1; // client transaction id
    private int RETRIES = 10;

    public TailChainClient(String zkAddress, String root, String host, int port) {
        this.port = port;
        this.root = root;
        this.host = host;
        this.zk = new ZookeeperClient(zkAddress, root);
    }

    public void connectToZk() throws IOException, InterruptedException {
        zk.connect();
        sid = zk.getSessionId();
        System.out.println("Client connected to zookeeper service. sid: " + sid);
    }

    private String sidToZNode(long sessionId) {
        return root + "/" + Utils.getHexSid(sessionId);
    }

    private String getAbsPath(String hexSid) {
        return root + "/" + hexSid;
    }

    private TailChainReplicaBlockingStub getStub(String znode) throws KeeperException, InterruptedException {
        //get stub from cache
        byte data[] = zk.getData(getAbsPath(znode), false);
        String target = new String(data).split("\n")[0];
        InetSocketAddress addr = Utils.str2addr(target);
        ManagedChannel ch = ManagedChannelBuilder.forTarget(target).usePlaintext().build();
        return TailChainReplicaGrpc.newBlockingStub(ch);
    }

    /**
     * Get the value to which the specified key is mapped.
     * @param key key for which the value is to be retrieved
     */
    public Response get(String key) throws KeeperException, InterruptedException {
        //todo: update following code
        zk.updateContext();
        System.out.println("tail: " + zk.getTailReplica());
        tailStub = getStub(zk.getTailReplica());


        GetRequest request = GetRequest.newBuilder().setKey(key).build();
        GetResponse rsp;
        int rc = 1;
        do {
            rsp = tailStub.get(request);
            rc = rsp.getRc();
            if (rc == 1) {
                System.out.println("The tail has been changed. Retrying...");
                // todo: check need to invoke updateContext() before proceeding
            }
        } while (rc == 1);
        if (rc == 0) {
            System.out.println("key: " + key + " value: " + rsp.getValue());
        }
        if (rc == 2) {
            System.err.println("Key " + key + " does not exist.");
        }
        return new Response(key, Response.Code.SUCCESS);
    }

    /**
     * Increment the value of a specified key.
     * @param key   Key for which the value is to be incremented
     * @param value Value of the key will be incremented by value.
     *              If key does not exist, it will be created with this value.
     *              Negative value will decrement the value by value.
     */
    public Response increment(String key, int value) {
        TailIncrementRequest req = TailIncrementRequest.newBuilder()
                .setKey(key)
                .setIncrValue(value)
                .setHost(host)
                .setPort(port)
                .setCxid(getCXid())
                .build();
        int rc;
        do {
            rc = headStub.increment(req).getRc();
            // todo: handle grpc exception
            if (rc != 0) {
                System.out.println("The head has been changed. Retrying...");
                // todo: check need to invoke updateContext() before proceeding
            }
        } while (rc != 0);

        // todo: wait for response from the tail
        return new Response(key, Response.Code.SUCCESS);
    }

    /**
     * Removes the entry for the specified key.
     * @param key
     */
    public Response delete(String key) throws KeeperException, InterruptedException {
        //todo: update following code
        zk.updateContext();
        System.out.println("head: " + zk.getHeadReplica());
        headStub = getStub(zk.getHeadReplica());

        TailDeleteRequest req = TailDeleteRequest.newBuilder()
                .setKey(key)
                .setHost(host)
                .setPort(port)
                .setCxid(getCXid())
                .build();
        int rc = -1;
        int retry = 0;
        do {
            try {
                rc = headStub.delete(req).getRc();
            } catch (StatusRuntimeException e) {
                // todo: decide action, probably retry with latest head information
                System.err.println("GRPC StatusCode: " + e.getStatus().getCode());
                continue;
            }
            if (rc != 0) {
                System.out.println("The head has been changed. Retrying...");
                // todo: check need to invoke updateContext() before proceeding
            }
        } while (rc != 0 && retry++ < RETRIES);
        // todo: wait for response from the tail

        return new Response(key, Response.Code.SUCCESS);
    }

    private int getCXid() {
        return cXid++;
    }
}
