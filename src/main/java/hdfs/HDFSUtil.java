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
        fs = FileSystem.get(new URI("hdfs://192.168.178.130：9000/"), conf);
    }

    @Test
    public void uploadFile() throws IOException {
        Path f = new Path("/data.log");
        FSDataOutputStream os = fs.create(f);
        FileInputStream is = new FileInputStream(new File(""));
        IOUtils.copy(is, os);
    }

    @Test
    public void uploadFileSimple() throws IOException {
        Path src = new Path("home/hadoop/baby.jpg");
        Path dest = new Path("home/hadoop/baby.jpg");
        fs.copyFromLocalFile(src, dest);
    }

    @Test
    public void downloadFile() throws Exception {
        FSDataInputStream is = fs.open(new Path("/baby.jpg"));
        FileOutputStream os = new FileOutputStream(new File(""));
        IOUtils.copy(is, os);
    }

    @Test
    public void mkdir() throws Exception {
        boolean mkdirs = fs.mkdirs(new Path(""));
        System.out.println(mkdirs ? "it is OK" : "is is failed");
    }

    @Test
    public void rmFileOrDir() throws Exception {
        fs.delete(new Path(""), true);
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
