package edu.sjsu.cs249.chain.client;

import edu.sjsu.cs249.chain.ChainResponse;
import edu.sjsu.cs249.chain.CxidProcessedRequest;
import edu.sjsu.cs249.chain.TailClientGrpc;
import edu.sjsu.cs249.chain.zookeeper.ZookeeperClient;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class ClientTest {


    private static void testTailClientService() {
        ManagedChannel channel =
                ManagedChannelBuilder.forTarget("127.0.0.1:4545").usePlaintext().build();
        TailClientGrpc.TailClientBlockingStub stub
                = TailClientGrpc.newBlockingStub(channel);
        CxidProcessedRequest request =
                CxidProcessedRequest.newBuilder().setCxid(1).build();
        ChainResponse response = stub.cxidProcessed(request);
        System.out.println("ACK - RC: " + response.getRc());
    }

    private static void testZk() throws IOException, InterruptedException, KeeperException {
        String root = "/mychain";
        ZookeeperClient zk = new ZookeeperClient("192.168.56.111:9999", root);
        zk.connect();

        // children change watch
        System.out.println("setW: " + zk.getData(root, true));

        while (true) {
            Thread.sleep(100000);
//            System.out.println("setW: " + zk.exists(root, true));
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
//        testTailClientService();
//        testZk();

        ConcurrentMap<String, Integer> map = new ConcurrentHashMap<>();
        for (int i = 0; i < 20; i++) {
            map.put("key" + i, i);
        }

        for (String key: map.keySet()) {
            if (key.equals("key1")) {
                System.out.println(key);
                map.remove(key);
            }
        }

        System.out.println(map);
    }
}
