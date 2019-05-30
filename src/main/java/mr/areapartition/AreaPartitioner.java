package mr.areapartition;

import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;

public class AreaPartitioner<K, V> extends Partitioner<K, V> {

    private static HashMap<String, Integer> map = new HashMap<>();

    static {
        map.put("135", 0);
        map.put("136", 1);
        map.put("137", 2);
        map.put("138", 3);
        map.put("139", 4);
    }

    @Override
    public int getPartition(K k, V v, int i) {
        return map.get(k.toString().substring(0, 3)) == null ? 5 : map.get(k.toString().substring(0, 3));
    }
}
