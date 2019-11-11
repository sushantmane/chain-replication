package edu.sjsu.cs249.chain.client;

import edu.sjsu.cs249.chain.ChainResponse;
import edu.sjsu.cs249.chain.CxidProcessedRequest;
import edu.sjsu.cs249.chain.TailClientGrpc;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TailClientService extends TailClientGrpc.TailClientImplBase {

    private static final Logger LOG = LoggerFactory.getLogger(TailClientService.class);

    /**
     * @param req       CxidProcessedRequest request parameter
     * @param rspObs    Response observer
     */
    @Override
    public void cxidProcessed(CxidProcessedRequest req, StreamObserver<ChainResponse> rspObs) {
        int cXid = req.getCxid();
        LOG.info("ACK - CXID: {}", cXid);
        // todo: notify watcher/observer thread that transaction with cXid has been finished.
        ChainResponse rsp = ChainResponse.newBuilder().setRc(0).build();
        rspObs.onNext(rsp);
        rspObs.onCompleted();
    }
}
