package edu.sjsu.cs249.chain.client;

import org.junit.jupiter.api.Test;

import java.io.IOException;

class ClientMainTest {

    @Test
    void main_printHelpTest() throws IOException, InterruptedException {
        String[] args = {"--help"};
        ClientMain.main(args);
    }

    @Test
    void main_MissingZookeeperOptionTest() throws IOException, InterruptedException {
        String[] args = {};
        ClientMain.main(args);
    }

    @Test
    void main_Test() throws IOException, InterruptedException {
        String[] args = {"-z", "192.168.56.111:5144", "-p", "8787", "-r", "/", "--inc", "red", "10"};
        ClientMain.main(args);
    }

    @Test
    void main_KVTest() throws IOException, InterruptedException {
        String[] args = {"-z", "192.168.56.111:5144", "-p", "8787", "-r", "/tail-chain", "--inc", "sushant", "10", "123"};
        ClientMain.main(args);
    }
}