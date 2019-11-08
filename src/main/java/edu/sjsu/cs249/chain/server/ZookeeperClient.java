package edu.sjsu.cs249.chain.server;

import edu.sjsu.cs249.chain.util.Utils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class ZookeeperClient implements Watcher {

    private static final Logger LOG = LoggerFactory.getLogger(ZookeeperClient.class);

    private ZooKeeper zk;
    private String connectString;
    private int timeout;
    private long sessionId;
    private CountDownLatch connLatch = new CountDownLatch(1);
    private AtomicLong predecessorSid = new AtomicLong();
    private AtomicLong successorSid = new AtomicLong();
    private AtomicBoolean tail = new AtomicBoolean();
    private AtomicBoolean head = new AtomicBoolean();

    protected ZookeeperClient(String connectString, int timeout) {
        this.connectString = connectString;
        this.timeout = timeout;
    }

    // connect with zookeeper server and establish a session
    // return -1 if fails to establish a session otherwise return session id
    public long connect() {
        try {
            zk = new ZooKeeper(connectString, timeout,this);
            // wait for session establishment
            connLatch.await();
            sessionId = zk.getSessionId();
        } catch (IOException | InterruptedException e) {
            return -1;
        }
        return sessionId;
    }

    // end zookeeper session
    public void closeConnection() {
        try {
            zk.close();
        } catch (InterruptedException e) {
            // ok
        }
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        LOG.info("New notification - type: {} state: {}", watchedEvent.getType(),
                watchedEvent.getState().toString());
        if (watchedEvent.getState() == Event.KeeperState.SyncConnected) {
            // session established, open connect latch
            connLatch.countDown();
        }
    }

    // create a node and set data for it
    public String create(String node, String data, CreateMode mode)
            throws KeeperException, InterruptedException {
        return zk.create(node, data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, mode);
    }

    // get data of the node
    public Stat getData(String path, boolean setWatch)
            throws KeeperException, InterruptedException {
        Stat data = new Stat();
        zk.getData(path, setWatch, data);
        return data;
    }

    // get children of the node represented by path
    public List<String> getChildren(String path, boolean watch)
            throws KeeperException, InterruptedException {
        return zk.getChildren(path, watch);
    }

    // delete the node
    public int delete(String path) {
        int rc = 0; // OK
        try {
            zk.delete(path, -1);
        } catch (KeeperException e) {
           if (e.code() == KeeperException.Code.NONODE) {
               rc = 2; // ENOENT
           }
        } catch (InterruptedException e) {
            rc = 11; // EAGAIN
        }
        return rc;
    }

    // return true of sid is successor of this session id
    protected boolean isSuccessor(long sid) {
        return sid == successorSid.get();
    }

    // return true of sid is predecessor of this session id
    protected boolean isPredecessor(long sid) {
        return sid == predecessorSid.get();
    }

    // return true if this node is first in chain
    protected boolean amIHead() {
        return head.get();
    }

    // return true if this node is last in chain
    protected boolean amITail() {
        return tail.get();
    }

    // returns sorted list of children of given node
    protected List<Long> getOrderListOfChainNodes(String path, boolean watch)
            throws KeeperException, InterruptedException {
        List<String> children = getChildren(path, watch);
        List<Long> sessionIds = new ArrayList<>();
        for (String child : children) {
            try {
                sessionIds.add(Utils.getLongSessionId(child));
            } catch (NumberFormatException e) {
                // skip non-numbers
            }
        }
        sessionIds.sort(Long::compare);
        return sessionIds;
    }

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        ZookeeperClient client = new ZookeeperClient("192.168.56.111:9999", 3000);
        long sid = client.connect();
        String chainRoot = "/tail-chain";
        String path = chainRoot + "/" + Utils.getHexSessionId(sid);
        String data = "127.0.0.1:5144";
        System.out.println("Path: " + path);
//        client.create(path, data, CreateMode.EPHEMERAL);
        client.create(path+"0", data, CreateMode.EPHEMERAL);
        client.create(path+"1", data, CreateMode.EPHEMERAL);
        client.create(path+"2", data, CreateMode.EPHEMERAL);
        System.out.println(client.getOrderListOfChainNodes(chainRoot, true));

        Thread.sleep(100000);
    }
}
