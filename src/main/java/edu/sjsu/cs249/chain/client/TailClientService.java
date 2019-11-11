package edu.sjsu.cs249.chain.client;

import com.google.common.eventbus.EventBus;
import edu.sjsu.cs249.chain.ChainResponse;
import edu.sjsu.cs249.chain.CxidProcessedRequest;
import edu.sjsu.cs249.chain.TailClientGrpc;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentMap;

public class TailClientService extends TailClientGrpc.TailClientImplBase {

    private static final Logger LOG = LoggerFactory.getLogger(TailClientService.class);
    private EventBus xidEventBus;
    private ConcurrentMap<Integer, Boolean> xidResQue;

    public TailClientService(EventBus eventBus, ConcurrentMap<Integer, Boolean> xidResQue) {
        this.xidEventBus = eventBus;
        this.xidResQue = xidResQue;
    }

    /**
     * @param req       CxidProcessedRequest request parameter
     * @param rspObs    Response observer
     */
    @Override
    public void cxidProcessed(CxidProcessedRequest req, StreamObserver<ChainResponse> rspObs) {
        int cXid = req.getCxid();

        xidResQue.replace(cXid, false, true);

        LOG.info("ACK - CXID: {}", cXid);
        // todo: notify watcher/observer thread that transaction with cXid has been finished.
        XidProcessed xidProcessed = new XidProcessed(cXid);
        LOG.info("Posting event on event bus");
        xidEventBus.post(xidProcessed);
        ChainResponse rsp = ChainResponse.newBuilder().setRc(0).build();
        rspObs.onNext(rsp);
        rspObs.onCompleted();
    }
}
