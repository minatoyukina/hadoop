package rpc;

public interface LoginServiceInterface {
    long versionID = 1L;

    String login(String name, String password);
}
