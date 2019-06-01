package rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;

public class RPCServer {
    public static void main(String[] args) throws IOException {
        RPC.Builder builder = new RPC.Builder(new Configuration());
        builder.setBindAddress("127.0.0.1").setPort(10000)
                .setInstance(new LoginServiceImpl())
                .setProtocol(LoginServiceInterface.class);
        RPC.Server build = builder.build();
        build.start();
    }
}
