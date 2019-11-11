package edu.sjsu.cs249.chain.client;

import edu.sjsu.cs249.chain.ChainResponse;
import edu.sjsu.cs249.chain.CxidProcessedRequest;
import edu.sjsu.cs249.chain.TailClientGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

public class ClientTest {


    private static void testTailClientService() {
        ManagedChannel channel =
                ManagedChannelBuilder.forTarget("127.0.0.1:4545").usePlaintext().build();
        TailClientGrpc.TailClientBlockingStub stub
                = TailClientGrpc.newBlockingStub(channel);
        CxidProcessedRequest request =
                CxidProcessedRequest.newBuilder().setCxid(98473625).build();
        ChainResponse response = stub.cxidProcessed(request);
        System.out.println("ACK - RC: " + response.getRc());
    }

    public static void main(String[] args) {
        testTailClientService();
    }
}
