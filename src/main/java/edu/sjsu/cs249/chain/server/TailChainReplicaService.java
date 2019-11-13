package edu.sjsu.cs249.chain.server;

import edu.sjsu.cs249.chain.ChainResponse;
import edu.sjsu.cs249.chain.CxidProcessedRequest;
import edu.sjsu.cs249.chain.GetRequest;
import edu.sjsu.cs249.chain.GetResponse;
import edu.sjsu.cs249.chain.HeadResponse;
import edu.sjsu.cs249.chain.LatestXidRequest;
import edu.sjsu.cs249.chain.LatestXidResponse;
import edu.sjsu.cs249.chain.TailChainReplicaGrpc;
import edu.sjsu.cs249.chain.TailChainReplicaGrpc.TailChainReplicaBlockingStub;
import edu.sjsu.cs249.chain.TailChainReplicaGrpc.TailChainReplicaImplBase;
import edu.sjsu.cs249.chain.TailClientGrpc;
import edu.sjsu.cs249.chain.TailClientGrpc.TailClientBlockingStub;
import edu.sjsu.cs249.chain.TailDeleteRequest;
import edu.sjsu.cs249.chain.TailIncrementRequest;
import edu.sjsu.cs249.chain.TailStateTransferRequest;
import edu.sjsu.cs249.chain.TailStateUpdateRequest;
import edu.sjsu.cs249.chain.util.Utils;
import edu.sjsu.cs249.chain.zookeeper.ZookeeperClient;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class TailChainReplicaService extends TailChainReplicaImplBase {

    private static final Logger LOG = LoggerFactory.getLogger(TailChainReplicaService.class);

    private ZookeeperClient zk;
    private ConcurrentMap<String, Integer> kvStore = new ConcurrentHashMap<>();
    private ConcurrentMap<TailStateUpdateRequest, Boolean> statePropagateReqQue = new ConcurrentHashMap<>();
    private TailChainReplicaBlockingStub successorStub; // to propagate state
    private TailChainReplicaBlockingStub tailStub; // log truncation
    private TailClientBlockingStub clientStub; // used to notify client
    private AtomicLong latestXid = new AtomicLong();
    private AtomicBoolean updateCtxInProgress = new AtomicBoolean();
    private long successorId = -1;
    private long predecessorID = -1;
    private long headSid = -1;
    private long tailSid = -1;

    public TailChainReplicaService(ZookeeperClient zk) {
        this.zk = zk;
        startUpdateCtxThread();
    }

    @Override
    public void proposeStateUpdate(TailStateUpdateRequest req, StreamObserver<ChainResponse> rspObs) {
        LOG.debug("ProposeStateUpdate request - cXid: {} client(ip:port):({}:{}) key: {} val: {} src:{} xid:{}",
                req.getCxid(), req.getHost(), req.getPort(), req.getKey(), req.getValue(), req.getSrc(), req.getXid());
        ChainResponse.Builder rspBldr = ChainResponse.newBuilder();
        // is my predecessor sending me this?
        if (req.getSrc() != predecessorID) {
            rspBldr.setRc(1);
            LOG.debug("ProposeStateUpdate response - Not a valid predecessor");
            rspObs.onNext(rspBldr.build());
            rspObs.onCompleted();
            return;
        }
        // perform state machine updates
        latestXid.set(req.getXid());
        kvStore.put(req.getKey(), req.getValue());
        // if I'm tail I'll notify client
        if (zk.amITail()) {
            TailClientBlockingStub tcbStub = getClientStub(req.getHost(), req.getPort());
            CxidProcessedRequest request = CxidProcessedRequest.newBuilder()
                    .setCxid(req.getCxid()).build();
            try {
                LOG.debug("State updates for xid:{} are complete. Notifying client - cXid:{}",
                        req.getXid(), req.getCxid());
                tcbStub.cxidProcessed(request);
            } catch (StatusRuntimeException e) {
                LOG.error("Failed to notify client - xid:{} cXid:{} e:{}", req.getXid(), req.getCxid(), e);
            }
        } else {
            // propagate change to successor node
            // update request with src
            TailStateUpdateRequest updateRequest = TailStateUpdateRequest.newBuilder(req)
                    .setSrc(zk.getSessionId()).build();
            statePropagateReqQue.put(updateRequest, false);
            try {
                LOG.debug("Initiate state update for xid:{} from {} to {}", req.getXid(),
                        Utils.getHexSid(zk.getSessionId()), Utils.getHexSid(successorId));
                ChainResponse rsp = successorStub.proposeStateUpdate(updateRequest);
                if (rsp.getRc() == 1) {
                    LOG.info("StateUpdate request was ignored - Not a valid predecessor");
                }
            } catch (StatusRuntimeException e) {
                LOG.error("Failed to propagate state - xid:{} cXid:{} e:{}", req.getXid(), req.getCxid(), e);
            }
        }
        rspBldr.setRc(0);
        LOG.debug("ProposeStateUpdate response - rc: {}", rspBldr.getRc());
        rspObs.onNext(rspBldr.build());
        rspObs.onCompleted();
    }

    @Override
    public void getLatestXid(LatestXidRequest req, StreamObserver<LatestXidResponse> rspObs) {
        LOG.debug("LatexXidRequest received");
        LatestXidResponse.Builder rspBldr = LatestXidResponse.newBuilder();
        if (zk.amITail()) {
            rspBldr.setXid(latestXid.get());
            rspBldr.setRc(0);
            LOG.debug("LatexXidRequest response - rc:{} xid:{}", rspBldr.getRc(), rspBldr.getXid());
        } else {
            LOG.debug("LatexXidRequest response - I ain't a tail replica");
            rspBldr.setRc(1);
        }
        rspObs.onNext(rspBldr.build());
        rspObs.onCompleted();
    }

    @Override
    public void stateTransfer(TailStateTransferRequest req, StreamObserver<ChainResponse> rspObs) {
        // take write lock
        LOG.debug("StateTransfer request received from node:{}", req.getSrc());
        ChainResponse.Builder rspBldr = ChainResponse.newBuilder();
        if (req.getSrc() != predecessorID) {
            rspBldr.setRc(1);
            LOG.debug("StateTransfer response - Invalid predecessor");
            rspObs.onNext(rspBldr.build());
            rspObs.onCompleted();
            return;
        }
        // update kvStore
        for (String key : req.getStateMap().keySet()) {
            kvStore.put(key, req.getStateMap().get(key));
        }
        // update xid
        latestXid.set(req.getStateXid());

        // apply missed update
        for (TailStateUpdateRequest stateRequest : req.getSentList()) {
            if (stateRequest.getXid() < latestXid.get()) {
                continue; // update already applied
            }
            latestXid.set(stateRequest.getXid());
            if (zk.amITail()) {
                notifyClient(stateRequest.getHost(), stateRequest.getPort(),
                        stateRequest.getXid(), stateRequest.getCxid());
                continue;
            }
            TailStateUpdateRequest updateRequest = TailStateUpdateRequest.newBuilder(stateRequest)
                    .setSrc(zk.getSessionId()).build();
            try {
                int rc = successorStub.proposeStateUpdate(updateRequest).getRc();
                if (rc == 1) {
                    LOG.info("StateUpdate request was ignored - Not a valid predecessor");
                }
                statePropagateReqQue.put(updateRequest, true);
            } catch (StatusRuntimeException e) {
                LOG.error("Failed to propagate state - xid:{} cXid:{} e:{}",
                        updateRequest.getXid(), updateRequest.getCxid(), e);
            }
        }
        LOG.info("StateTransfer has been completed. PredecessorNode:{}", Utils.getHexSid(predecessorID));
        rspBldr.setRc(0);
        LOG.debug("StateTransfer response - rc:{}", rspBldr.getRc());
        rspObs.onNext(rspBldr.build());
        rspObs.onCompleted();
    }

    @Override
    public void increment(TailIncrementRequest req, StreamObserver<HeadResponse> rspObs) {
        LOG.debug("INC request - k:{} v:{} cXid:{} cli:({}:{})", req.getKey(), req.getIncrValue(),
                req.getCxid(), req.getHost(), req.getPort());
        HeadResponse.Builder rspBldr = HeadResponse.newBuilder();
        // not head
        if (!zk.amIHead()) {
            rspBldr.setRc(1);
            LOG.debug("INC Response - rc: {}", rspBldr.getRc());
            rspObs.onNext(rspBldr.build());
            rspObs.onCompleted();
            return;
        }
        long tXid = req.getCxid(); // get new xid for this update
        int dVal = req.getIncrValue();
        kvStore.compute(req.getKey(), (k, v) -> v == null ? dVal : v + dVal);
        // if tail respond to client
        if (zk.amITail()) {
            TailClientBlockingStub tcbStub = getClientStub(req.getHost(), req.getPort());
            CxidProcessedRequest request = CxidProcessedRequest.newBuilder()
                    .setCxid(req.getCxid()).build();
            try {
                tcbStub.cxidProcessed(request);
            } catch (StatusRuntimeException e) {
                System.out.println("Failed to notify client. cXid: " + req.getCxid());
                e.printStackTrace();
            }
        } else {
            TailStateUpdateRequest stateUpdateRequest = TailStateUpdateRequest.newBuilder()
                    .setXid(tXid)
                    .setSrc(zk.getSessionId())
                    .setKey(req.getKey())
                    .setValue(kvStore.get(req.getKey()))
                    .setCxid(req.getCxid())
                    .setHost(req.getHost())
                    .setPort(req.getPort())
                    .setXid(getXid())
                    .build();
            statePropagateReqQue.put(stateUpdateRequest, false);
            try {
                ChainResponse rsp = successorStub.proposeStateUpdate(stateUpdateRequest);
                if (rsp.getRc() == 1) {
                    System.out.println("Sent to wrong successor..");
                    // todo: add retry logic
                }
            } catch (StatusRuntimeException e) {
                System.out.println("Failed to propagate state. cXid: "
                        + req.getCxid() + " xid: " + tXid);
                e.printStackTrace();
            }
        }
        rspBldr.setRc(0);
        LOG.debug("INC Response - rc: {}", rspBldr.getRc());
        rspObs.onNext(rspBldr.build());
        rspObs.onCompleted();
    }

    @Override
    public void delete(TailDeleteRequest req, StreamObserver<HeadResponse> rspObs) {
        LOG.debug("DEL request - k:{} cXid:{} cli:({}:{})", req.getKey(),
                req.getCxid(), req.getHost(), req.getPort());
        HeadResponse.Builder rspBldr = HeadResponse.newBuilder();
        if (!zk.amIHead()) {
            rspBldr.setRc(1);
            LOG.debug("DEL Response - rc: {}", rspBldr.getRc());
            rspObs.onNext(rspBldr.build());
            rspObs.onCompleted();
            return;
        }
        long tXid = getXid(); // get new xid for this update
        kvStore.put(req.getKey(), 0);
        // if tail respond to client
        if (zk.amITail()) {
            TailClientBlockingStub tcbStub = getClientStub(req.getHost(), req.getPort());
            CxidProcessedRequest request = CxidProcessedRequest.newBuilder()
                    .setCxid(req.getCxid()).build();
            try {
                tcbStub.cxidProcessed(request);
            } catch (StatusRuntimeException e) {
                System.out.println("Failed to notify client. cXid: " + req.getCxid());
                e.printStackTrace();
            }
        } else {
            TailStateUpdateRequest stateUpdateRequest = TailStateUpdateRequest.newBuilder()
                    .setXid(tXid)
                    .setSrc(zk.getSessionId())
                    .setKey(req.getKey())
                    .setValue(0)
                    .setCxid(req.getCxid())
                    .setHost(req.getHost())
                    .setPort(req.getPort())
                    .setXid(getXid())
                    .build();
            statePropagateReqQue.put(stateUpdateRequest, false);
            try {
                ChainResponse rsp = successorStub.proposeStateUpdate(stateUpdateRequest);
                if (rsp.getRc() == 1) {
                    // tbd: retry?
                    LOG.info("StateUpdateRequest was ignored by - Not a valid predecessor");
                }
            } catch (StatusRuntimeException e) {
                LOG.error("StateUpdateRequest failed. xid:{} - {}", tXid, e.getMessage());
                // tbd: should we retry or check whether current node is now tail?
            }
        }
        rspBldr.setRc(0);
        LOG.debug("DEL Response - rc: {}", rspBldr.getRc());
        rspObs.onNext(rspBldr.build());
        rspObs.onCompleted();
    }

    // DONE
    @Override
    public void get(GetRequest req, StreamObserver<GetResponse> rspObs) {
        LOG.debug("GET Request - key: {}", req.getKey());
        GetResponse.Builder rspBldr = GetResponse.newBuilder();
        if (zk.amITail()) {
            Integer val = kvStore.get(req.getKey());
            if (val == null || val == 0) {
                rspBldr.setRc(2);
            } else {
                rspBldr.setRc(0).setValue(val);
            }
            LOG.debug("GET response - rc:{} k:{} v:{}", rspBldr.getRc(), req.getKey(), rspBldr.getValue());
        } else {
            rspBldr.setRc(1);
            LOG.debug("GET response - I ain't a tail replica");
        }
        rspObs.onNext(rspBldr.build());
        rspObs.onCompleted();
    }

    void startUpdateCtxThread() {
        Executors.newSingleThreadScheduledExecutor()
                .scheduleAtFixedRate(() -> {
                    if (!updateCtxInProgress.get()) {
                        updateCtx();
                    }
                }, 5, 5, TimeUnit.MILLISECONDS);
    }

    // update my env
    void updateCtx() {
        updateCtxInProgress.set(true);
        //LOG.debug("Context refresh is in progress...");
        try {
            setHeadId(zk.getHeadSid());
            setPredecessorId(zk.getPredecessor());
            getSuccessorStub(zk.getSuccessor());
            getTailStub(zk.getTailSid());  // test on tail or just call local method
        } catch (Exception e) {
            LOG.error("Some znodes might have corrupt data.", e);
            e.printStackTrace(); // todo: remove
            System.exit(1); // fail early
        } finally {
            //LOG.debug("My:{} Head:{} Tail:{} Pred:{} Succ:{}", Utils.getHexSid(zk.getSessionId()),
            //        zk.amIHead(), zk.amITail(), Utils.getHexSid(zk.getPredecessor()),
            //        Utils.getHexSid(zk.getSuccessor()));
            //LOG.debug("Context refresh is finished");
            updateCtxInProgress.set(false);
        }
    }

    private void pruneCompletedRequests() {
        try {
            LatestXidRequest req = LatestXidRequest.newBuilder().build();
            LatestXidResponse rsp = tailStub.getLatestXid(req);
            if (rsp.getRc() == 1) {
                LOG.info("GetLatestXid was ignored - Not a valid tail");
                return;
            }
            for (TailStateUpdateRequest request : statePropagateReqQue.keySet()) {
                if (request.getXid() <= rsp.getXid()) {
                    statePropagateReqQue.remove(request);
                }
            }
        } catch (StatusRuntimeException e) {
            LOG.error("GetLatestXid request failed. ", e);
        }
    }

    private long getXid() {
        return latestXid.getAndAdd(1);
    }

    private void setPredecessorId(long sId) {
        if (predecessorID == sId) {
            return;
        }
        predecessorID = sId;
    }

    private void setSuccessorId(long sId) {
        if (successorId == sId) {
            return;
        }
        successorId = sId;
    }

    private void setHeadId(long sId) {
        if (headSid == sId) {
            return;
        }
        headSid = sId;
    }

    private void setTailId(long sId) {
        if (tailSid == sId) {
            return;
        }
        tailSid = sId;
    }

    public void notifyClient(String host, int port, long xid, int cXid) {
        // todo: add caching for client stubs
        TailClientBlockingStub tcbStub = getClientStub(host, port);
        CxidProcessedRequest request = CxidProcessedRequest.newBuilder().setCxid(cXid).build();
        try {
            LOG.debug("Notifying client - xid:{} cXid:{}", xid, cXid);
            tcbStub.cxidProcessed(request);
        } catch (StatusRuntimeException e) {
            LOG.error("Failed to notify client - xid:{} cXid:{} e:{}", xid, cXid, e);
        } finally {
            ManagedChannel ch = (ManagedChannel) tcbStub.getChannel();
            ch.shutdownNow();
        }
    }

    private void getSuccessorStub(long sid) throws KeeperException, InterruptedException {
        if (successorId == sid) {
            return;
        }
        if (sid == -1) {
            successorId = sid; // I'm tail. No don't have successor.
            return;
        }
        // new successor
        destroyStubAndChannel(successorStub);
        successorStub = getStub(sidToZNodeAbsPath(sid));
        successorId = sid;
        // start state transfer request
        TailStateTransferRequest req = TailStateTransferRequest.newBuilder()
                .setSrc(zk.getSessionId())
                .setStateXid(latestXid.get())
                .addAllSent(statePropagateReqQue.keySet())
                .putAllState(kvStore)
                .build();
        LOG.info("Initiate state transfer from {} to {}",
                Utils.getHexSid(zk.getSessionId()), Utils.getHexSid(successorId));
        try {
            if (successorStub.stateTransfer(req).getRc() == 0) {
                LOG.info("State transfer from {} to {} complete",
                        Utils.getHexSid(zk.getSessionId()), Utils.getHexSid(successorId));
            } else {
                LOG.info("State transfer request ignored. NotAValidPredecessor");
            }
        } catch (StatusRuntimeException e) {
            LOG.error("Failed to transfer state from {} to {}",
                    Utils.getHexSid(zk.getSessionId()), Utils.getHexSid(successorId));
        }
    }

    private void getTailStub(long sid) throws KeeperException, InterruptedException {
        if (tailSid == sid) {
            return;
        }
        // use reentrant lock?
        destroyStubAndChannel(tailStub);
        tailStub = getStub(sidToZNodeAbsPath(sid));
        tailSid = sid;
        System.out.println("NewTail: " + Utils.getHexSid(sid));
    }

    private String sidToZNodeAbsPath(long sessionId) {
        return getAbsPath(Utils.getHexSid(sessionId));
    }

    private String getAbsPath(String hexSid) {
        return zk.getRoot() + "/" + hexSid;
    }

    // returns stub for given node
    private TailChainReplicaBlockingStub getStub(String znode)
            throws KeeperException, InterruptedException {
        byte[] data = zk.getData(znode, false);
        String target = new String(data).split("\n")[0];
        ManagedChannel ch = ManagedChannelBuilder.forTarget(target).usePlaintext().build();
        return TailChainReplicaGrpc.newBlockingStub(ch);
    }

    // todo: replace with cache for stubs
    // returns stub for <host:port> client
    private TailClientBlockingStub getClientStub(String host, int port) {
        ManagedChannel ch = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
        return TailClientGrpc.newBlockingStub(ch);
    }

    // destroy stub and shutdown associated channel
    private void destroyStubAndChannel(TailChainReplicaBlockingStub stub) {
        if (stub == null) {
            return;
        }
        closeChannel((ManagedChannel) stub.getChannel());
    }

    // destroy stub and shutdown associated channel
    private void destroyStubAndChannel(TailClientBlockingStub stub) {
        if (stub == null) {
            return;
        }
        closeChannel((ManagedChannel) stub.getChannel());
    }

    private void closeChannel(ManagedChannel ch) {
        if (ch == null || ch.isTerminated()) {
            return;
        }
        ch.shutdown();
        if (!ch.isTerminated()) {
            ch.shutdownNow(); // todo: figure out better way for handling this
        }
    }
}
