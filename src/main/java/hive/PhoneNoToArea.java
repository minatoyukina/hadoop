package hive;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.HashMap;

public class PhoneNoToArea extends UDF {
    private static HashMap<String, String> area = new HashMap<>();

    static {
        area.put("138", "beijing");
        area.put("139", "shanghai");
        area.put("140", "tianjin");
        area.put("141", "guangzhou");
        area.put("142", "shenzhen");
    }

    public String evaluate(String phoneNo) {
        String prefix = phoneNo.substring(0, 3);
        String areaStr = area.get(prefix) != null ? area.get(prefix) : "other";
        return phoneNo + "  " + areaStr;
    }
}
