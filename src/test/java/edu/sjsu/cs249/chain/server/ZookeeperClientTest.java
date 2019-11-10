package edu.sjsu.cs249.chain.server;

import edu.sjsu.cs249.chain.util.Utils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ZookeeperClientTest {

    private String connectString = "192.168.56.111:9999";
    private int timeout = 300000;
    private ZookeeperClient zk;
    private String chainRoot = "/tail-chain";

    @BeforeEach
    void setup() {
        zk = new ZookeeperClient(connectString, timeout);
        zk.connect();
    }

    @AfterEach
    void tearDown() {
        zk.closeConnection();
    }

    @Test
    void connect() throws InterruptedException, KeeperException {
        ZookeeperClient client = new ZookeeperClient("192.168.56.111:9999", 3000);
        long sid = client.connect();
        String path = chainRoot + "/" + Utils.getHexSid(sid);
        String data = "127.0.0.1:5144";
        System.out.println("Path: " + path);
        client.create(path, data, CreateMode.EPHEMERAL);
        System.out.println(client.getOrderListOfChainNodes(chainRoot, true));
        Thread.sleep(10000000000L);
    }

    @Test
    void oneNodeChain() throws KeeperException, InterruptedException {
        long sid = zk.getSessionId();
        // set watch on chain root node
        if (!zk.exists(chainRoot, true)) {
            // chain root node is not present
            return;
        }
        String replica = chainRoot + "/" + Utils.getHexSid(sid);
        String headData = "192.168.56.112:5001";
        zk.create(replica, headData, CreateMode.EPHEMERAL);
        zk.updateContext();
        List<Long> children = zk.getOrderListOfChainNodes(chainRoot, true);
        assertTrue(zk.amIHead());
        assertTrue(zk.amITail());
    }

    @Test
    void twoNodeChain() throws KeeperException, InterruptedException {
        long sid = zk.getSessionId();
        // set watch on chain root node
        if (!zk.exists(chainRoot, true)) {
            // chain root node is not present
            return;
        }
        // replica-1::head
        String replica0 = chainRoot + "/" + Utils.getHexSid(sid);
        String headData = "192.168.56.112:5001";
        zk.create(replica0, headData, CreateMode.EPHEMERAL);
        // replica-2::tail
        String replica1 = chainRoot + "/" + Utils.getHexSid(sid + 1);
        String tailData = "192.168.56.113:5002";
        zk.create(replica1, tailData, CreateMode.EPHEMERAL);
        zk.updateContext();
        assertTrue(zk.amIHead());
        assertFalse(zk.amITail());
        assertFalse(zk.isHead(sid + 1));
        assertTrue(zk.isTail(sid + 1));
    }

    @Test
    void threeNodeChain_Head() throws KeeperException, InterruptedException {
        long sid = zk.getSessionId();
        long sid_r1 = sid + 1;
        long sid_r2 = sid + 2;
        // set watch on chain root node
        if (!zk.exists(chainRoot, true)) {
            // chain root node is not present
            return;
        }
        // replica-1::head
        String replica0 = chainRoot + "/" + Utils.getHexSid(sid);
        String headData = "192.168.56.112:5001";
        zk.create(replica0, headData, CreateMode.EPHEMERAL);

        // replica-2
        String replica1 = chainRoot + "/" + Utils.getHexSid(sid_r1);
        String midData = "192.168.56.113:5002";
        zk.create(replica1, midData, CreateMode.EPHEMERAL);

        // replica-3::tail
        String replica2 = chainRoot + "/" + Utils.getHexSid(sid_r2);
        String tailData = "192.168.56.114:5003";
        zk.create(replica2, tailData, CreateMode.EPHEMERAL);

        zk.updateContext();
        assertTrue(zk.amIHead());
        assertFalse(zk.amITail());
        assertTrue(zk.isSuccessor(sid_r1));
        assertFalse(zk.isHead(sid_r1));
        assertFalse(zk.isTail(sid_r1));
        assertFalse(zk.isHead(sid_r2));
        assertTrue(zk.isTail(sid_r2));
    }
}