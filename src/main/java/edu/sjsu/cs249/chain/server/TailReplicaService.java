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
import io.grpc.stub.StreamObserver;

public class TailReplicaService extends TailChainReplicaImplBase {

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
        GetResponse.Builder rspBldr = GetResponse.newBuilder();
        rspBldr.setRc(0).setValue(123456);
        rspObs.onNext(rspBldr.build());
        rspObs.onCompleted();
    }
}
