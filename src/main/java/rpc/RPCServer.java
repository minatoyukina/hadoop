package rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;

//上传到服务器用hadoop jar xxx.jar rpc.RPCServer启动Server
public class RPCServer {
    public static void main(String[] args) throws IOException {
        RPC.Builder builder = new RPC.Builder(new Configuration());
        builder.setBindAddress("192.168.178.130").setPort(10000)
                .setInstance(new LoginServiceImpl())
                .setProtocol(LoginServiceInterface.class);
        RPC.Server build = builder.build();
        build.start();
    }
}
