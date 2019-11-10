package edu.sjsu.cs249.chain.client;

import org.junit.jupiter.api.Test;

class CliAppDemoTest {

    @Test
    void main_printHelpTest() {
        String[] args = {"--help"};
        CliAppDemo.main(args);
    }

    @Test
    void main_MissingZookeeperOptionTest() {
        String[] args = {};
        CliAppDemo.main(args);
    }

    @Test
    void main_Test() {
        String[] args = {"-z", "192.168.56.111:5144", "this xd", "-r", "/", "--inc", "red", "10"};
        CliAppDemo.main(args);
    }

    @Test
    void main_KVTest() {
        String[] args = {"-z", "192.168.56.111:5144", "-r", "/tail-chain", "--inc", "sushant", "10", "123"};
        CliAppDemo.main(args);
    }
}