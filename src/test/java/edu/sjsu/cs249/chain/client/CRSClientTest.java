package edu.sjsu.cs249.chain.client;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class CRSClientTest {

    @Test
    void main_printHelpTest() {
        String[] args = {"--help"};
        CRSClient.main(args);
    }

    @Test
    void main_MissingZookeeperOptionTest() {
        String[] args = {};
        CRSClient.main(args);
    }

    @Test
    void main_Test() {
        String[] args = {"-z", "192.168.56.111:5144", "-p 8787", "-r", "/", "--inc", "red", "10"};
        CRSClient.main(args);
    }

    @Test
    void main_KVTest() {
        String[] args = {"-z", "192.168.56.111:5144", "-r", "/tail-chain", "--inc", "sushant", "10", "123"};
        CRSClient.main(args);
    }
}