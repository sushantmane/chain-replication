package edu.sjsu.cs249.chain.client;

import edu.sjsu.cs249.chain.GetRequest;
import edu.sjsu.cs249.chain.GetResponse;
import edu.sjsu.cs249.chain.TailChainReplicaGrpc;
import edu.sjsu.cs249.chain.TailChainReplicaGrpc.TailChainReplicaBlockingStub;
import edu.sjsu.cs249.chain.util.Utils;
import edu.sjsu.cs249.chain.zookeeper.ZookeeperClient;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.net.InetSocketAddress;

public class TailChainClient {

    private ManagedChannel channel;
    private TailChainReplicaBlockingStub headStub;
    private TailChainReplicaBlockingStub tailStub;
    // ip address and port number of host on which it
    // will listen for messages from tail replica node
    private String host;
    private int port;
    private ZookeeperClient zk;
    private long sid;
    private String root;

    public TailChainClient(String zkAddress, String root, int port) {
        this.port = port;
        this.root = root;
        this.zk = new ZookeeperClient(zkAddress, root);
    }

    public void connectToZk() throws IOException, InterruptedException {
        zk.connect();
        sid = zk.getSessionId();
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
        System.out.println("target: " + target);
        InetSocketAddress addr = Utils.str2addr(target);
        ManagedChannel ch = ManagedChannelBuilder.forTarget(target).usePlaintext().build();
        return TailChainReplicaGrpc.newBlockingStub(ch);
    }

    /**
     * Get the value to which the specified key is mapped.
     * @param key key for which the value is to be retrieved
     */
    public void get(String key) throws KeeperException, InterruptedException {
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
            }
        } while (rc == 1);
        if (rc == 0) {
            System.out.println("key: " + key + " value: " + rsp.getValue());
        }
        if (rc == 2) {
            System.err.println("Key " + key + " does not exist.");
        }
    }

    /**
     * Increment the value of a specified key.
     * @param key   Key for which the value is to be incremented
     * @param value Value of the key will be incremented by value.
     *              If key does not exist, it will be created with this value.
     *              Negative value will decrement the value by value.
     */
    public void increment(String key, int value) {
    }

    /**
     * Removes the entry for the specified key.
     * @param key
     */
    public void delete(String key) {
    }

    public static void main(String[] args) throws KeeperException, InterruptedException, IOException {
        String zkAddress = "192.168.56.111:9999";
        String chainRoot = "/tail-chain";
        int port = 4415;
        TailChainClient client = new TailChainClient(zkAddress, chainRoot, port);
        client.connectToZk();
        client.get("cs249");
    }
}
