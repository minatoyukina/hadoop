package rpc;

public class LoginServiceImpl implements LoginServiceInterface {
    @Override
    public String login(String name, String password) {
        return "" + name + " login successfully";
    }
}
