package edu.sjsu.cs249.chain.client;

import edu.sjsu.cs249.chain.GetRequest;
import edu.sjsu.cs249.chain.GetResponse;
import edu.sjsu.cs249.chain.TailChainReplicaGrpc;
import edu.sjsu.cs249.chain.TailChainReplicaGrpc.TailChainReplicaBlockingStub;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

public class TailChainClient {

    private ManagedChannel channel;
    private TailChainReplicaBlockingStub headStub;
    private TailChainReplicaBlockingStub tailStub;

    public TailChainClient(String target) {
        channel = ManagedChannelBuilder.forTarget(target).usePlaintext().build();
        headStub = TailChainReplicaGrpc.newBlockingStub(channel);
    }

    public void get(String key) {
        GetRequest request = GetRequest.newBuilder().setKey(key).build();
        GetResponse rsp = headStub.get(request);
        System.out.println("rc:" + rsp.getRc());
        System.out.println("val:" + rsp.getValue());
    }

    public static void main(String[] args) {
        String target = "127.0.0.1:5144";
        TailChainClient client = new TailChainClient(target);
        client.get("zookeeper");
    }
}
