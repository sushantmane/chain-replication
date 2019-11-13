package edu.sjsu.cs249.chain.client;

import edu.sjsu.cs249.chain.ChainResponse;
import edu.sjsu.cs249.chain.CxidProcessedRequest;
import edu.sjsu.cs249.chain.TailClientGrpc;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentMap;

public class TailClientService extends TailClientGrpc.TailClientImplBase {

    private static final Logger LOG = LoggerFactory.getLogger(TailClientService.class);
    private ConcurrentMap<Integer, Boolean> xidResponseQue;

    public TailClientService(ConcurrentMap<Integer, Boolean> xidResponseQue) {
        this.xidResponseQue = xidResponseQue;
    }

    @Override
    public void cxidProcessed(CxidProcessedRequest req, StreamObserver<ChainResponse> rspObs) {
        LOG.info("Ack for xid {} received", req.getCxid());
        // boolean res = xidResponseQue.replace(req.getCxid(), false, true);
        xidResponseQue.put(req.getCxid(), true);
        System.out.println("ACK: " + " cxid:" + req.getCxid());
        ChainResponse rsp = ChainResponse.newBuilder().setRc(0).build();
        rspObs.onNext(rsp);
        rspObs.onCompleted();
    }
}
