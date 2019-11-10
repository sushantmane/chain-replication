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

public class TailChainReplicaService extends TailChainReplicaImplBase {

    private static final Logger LOG = LoggerFactory.getLogger(TailChainReplicaService.class);

    private ZookeeperClient zk;
    private long sid;
    private String chainRoot;


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
        HeadResponse.Builder rspBldr = HeadResponse.newBuilder();
        rspBldr.setRc(0);
        rspObs.onNext(rspBldr.build());
        rspObs.onCompleted();
    }

    @Override
    public void delete(TailDeleteRequest req, StreamObserver<HeadResponse> rspObs) {
        HeadResponse.Builder rspBldr = HeadResponse.newBuilder();
        rspObs.onNext(rspBldr.build());
        rspObs.onCompleted();
    }

    @Override
    public void get(GetRequest req, StreamObserver<GetResponse> rspObs) {
        LOG.info("GET Request - key: {}", req.getKey());
        GetResponse.Builder rspBldr = GetResponse.newBuilder();
        rspBldr.setRc(0).setValue(123456);
        LOG.info("GET Response - rc: {} value: {}", rspBldr.getRc(), rspBldr.getValue());
        rspObs.onNext(rspBldr.build());
        rspObs.onCompleted();
    }
}
