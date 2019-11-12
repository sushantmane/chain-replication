package edu.sjsu.cs249.chain.server;

import edu.sjsu.cs249.chain.ChainResponse;
import edu.sjsu.cs249.chain.GetRequest;
import edu.sjsu.cs249.chain.GetResponse;
import edu.sjsu.cs249.chain.HeadResponse;
import edu.sjsu.cs249.chain.LatestXidRequest;
import edu.sjsu.cs249.chain.LatestXidResponse;
import edu.sjsu.cs249.chain.TailChainReplicaGrpc.TailChainReplicaImplBase;
import edu.sjsu.cs249.chain.TailDeleteRequest;
import edu.sjsu.cs249.chain.TailIncrementRequest;
import edu.sjsu.cs249.chain.TailStateTransferRequest;
import edu.sjsu.cs249.chain.TailStateUpdateRequest;
import edu.sjsu.cs249.chain.zookeeper.ZookeeperClient;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class TailChainReplicaService extends TailChainReplicaImplBase {

    private static final Logger LOG = LoggerFactory.getLogger(TailChainReplicaService.class);

    private ZookeeperClient zk;
    private long sid;
    private String chainRoot;
    private ConcurrentMap<String, Integer> kvStore = new ConcurrentHashMap<>();

    public TailChainReplicaService(ZookeeperClient zk) {
        this.zk = zk;
        this.sid = zk.getSessionId();
    }

    @Override
    public void proposeStateUpdate(TailStateUpdateRequest req, StreamObserver<ChainResponse> rspObs) {
        ChainResponse.Builder rspBldr = ChainResponse.newBuilder();
        rspObs.onNext(rspBldr.build());
        rspObs.onCompleted();
    }

    @Override
    public void getLatestXid(LatestXidRequest req, StreamObserver<LatestXidResponse> rspObs) {
        LatestXidResponse.Builder rspBldr = LatestXidResponse.newBuilder();
        rspObs.onNext(rspBldr.build());
        rspObs.onCompleted();
    }

    @Override
    public void stateTransfer(TailStateTransferRequest req, StreamObserver<ChainResponse> rspObs) {
        ChainResponse.Builder rspBldr = ChainResponse.newBuilder();
        rspObs.onNext(rspBldr.build());
        rspObs.onCompleted();
    }

    @Override
    public void increment(TailIncrementRequest req, StreamObserver<HeadResponse> rspObs) {
        LOG.debug("INC Request - cXid: {} client(ip:port):({}:{}) key: {} val: {}",
                req.getCxid(), req.getHost(), req.getPort(), req.getKey(), req.getIncrValue());
        HeadResponse.Builder rspBldr = HeadResponse.newBuilder();
        if (amIHead()) {
            int dVal = req.getIncrValue();
            kvStore.compute(req.getKey(), (k, v) -> v == null ? dVal : v + dVal);
            rspBldr.setRc(0);
            //todo: trigger propagate state event
        } else {
            rspBldr.setRc(1);
        }
        LOG.debug("INC Response - rc: {}", rspBldr.getRc());
        rspObs.onNext(rspBldr.build());
        rspObs.onCompleted();
    }

    @Override
    public void delete(TailDeleteRequest req, StreamObserver<HeadResponse> rspObs) {
        LOG.debug("DEL Request - cXid: {} client(ip:port):({}:{}) key: {}",
                req.getCxid(), req.getHost(), req.getPort(), req.getKey());
        HeadResponse.Builder rspBldr = HeadResponse.newBuilder();
        if (amIHead()) {
            kvStore.remove(req.getKey());
            rspBldr.setRc(0);
            //todo: trigger propagate state event aync
        } else {
            rspBldr.setRc(1);
        }
        LOG.debug("DEL Response - rc: {}", rspBldr.getRc());
        rspObs.onNext(rspBldr.build());
        rspObs.onCompleted();
    }

    @Override
    public void get(GetRequest req, StreamObserver<GetResponse> rspObs) {
        LOG.debug("GET Request - key: {}", req.getKey());
        GetResponse.Builder rspBldr = GetResponse.newBuilder();
        if (amITail()) {
            Integer val = kvStore.get(req.getKey());
            if (val == null) {
                rspBldr.setRc(2);
            } else {
                rspBldr.setRc(0).setValue(val);
            }
        } else {
            rspBldr.setRc(1);
        }
        LOG.debug("GET Response - rc: {} value: {}", rspBldr.getRc(), rspBldr.getValue());
        rspObs.onNext(rspBldr.build());
        rspObs.onCompleted();
    }

    private boolean amIHead() {
//        return zk.isHead(sid);
        return true;
    }

    private boolean amITail() {
        return true;
//        return zk.isTail(sid);
    }
}
