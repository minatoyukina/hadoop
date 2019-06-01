package action.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;

public class DBLoader {
    public static void loadDB(HashMap<String, String> ruleMap) {
        Connection conn = null;
        Statement statement = null;
        ResultSet resultSet = null;

        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            conn = DriverManager.getConnection("jdbc:mysql://10.14.37.114:3306/hadoop?serverTimezone=GMT%2B8&useSSL=false", "root", "123456");
            statement = conn.createStatement();
            resultSet = statement.executeQuery("select url,info from urlrule");
            while (resultSet.next()) {
                ruleMap.put(resultSet.getString(1), resultSet.getString(2));
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (resultSet != null) {
                    resultSet.close();
                }
                if (statement != null) {
                    statement.close();
                }
                if (conn != null) {
                    conn.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
