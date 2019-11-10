package edu.sjsu.cs249.chain.client;

import edu.sjsu.cs249.chain.GetRequest;
import edu.sjsu.cs249.chain.GetResponse;
import edu.sjsu.cs249.chain.TailChainReplicaGrpc;
import edu.sjsu.cs249.chain.TailChainReplicaGrpc.TailChainReplicaBlockingStub;
import edu.sjsu.cs249.chain.TailDeleteRequest;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.net.SocketException;
import java.net.UnknownHostException;

public class TailChainClient {

    private ManagedChannel channel;
    private TailChainReplicaBlockingStub headStub;
    private TailChainReplicaBlockingStub tailStub;

    public TailChainClient(String target) {
        channel = ManagedChannelBuilder.forTarget(target).usePlaintext().build();
        headStub = TailChainReplicaGrpc.newBlockingStub(channel);
    }

    /**
     * Get the value to which the specified key is mapped.
     * @param key key for which the value is to be retrieved
     */
    public void get(String key) {
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
        TailDeleteRequest request = TailDeleteRequest.newBuilder().build();
    }

    public static void main(String[] args) throws UnknownHostException, SocketException {
        String target = "127.0.0.1:5144";
        // TailChainClient client = new TailChainClient(target);
        // client.get("zookeeper");
    }
}
