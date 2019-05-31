package hdfs;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;

public class HDFSUtil {

    private FileSystem fs;

    @Before
    public void init() throws Exception {
        Configuration conf = new Configuration();
        fs = FileSystem.get(new URI("hdfs://192.168.178.130:9000/"), conf);
    }

    @Test
    public void uploadFile() throws IOException {
        Path f = new Path("/test");
        FSDataOutputStream os = fs.create(f);
        FileInputStream is = new FileInputStream(new File("./a.txt"));
        IOUtils.copy(is, os);
    }

    @Test
    public void uploadFileSimple() throws IOException {
        Path src = new Path("./a.txt");
        Path dest = new Path("/test/a.txt");
        fs.copyFromLocalFile(src, dest);
    }

    @Test
    public void downloadFile() throws Exception {
        FSDataInputStream is = fs.open(new Path("/wc/output/part-r-00000"));
        FileOutputStream os = new FileOutputStream(new File("./a.txt"));
        IOUtils.copy(is, os);
    }

    @Test
    public void mkdir() throws Exception {
        boolean mkdirs = fs.mkdirs(new Path("/test"));
        System.out.println(mkdirs ? "it is OK" : "is is failed");
    }

    @Test
    public void rmFileOrDir() throws Exception {
        fs.delete(new Path("/test"), true);
    }

    @Test
    public void listFiles() throws Exception {
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
        while (listFiles.hasNext()) {
            LocatedFileStatus file = listFiles.next();
            System.out.println(file.getPath());
            BlockLocation[] blockLocations = file.getBlockLocations();
            for (BlockLocation s : blockLocations) {
                System.out.println(s.getHosts()[0]);
            }
        }
    }

}
