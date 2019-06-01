package action.enhance;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class EnhanceOutputFormat<K, V> extends FileOutputFormat<K, V> {
    @Override
    public RecordWriter<K, V> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException {
        FileSystem fs = FileSystem.get(taskAttemptContext.getConfiguration());
        Path comp = new Path("/action/enhance/enhancedlog");
        Path toCrawl = new Path("/action/enhance/tocrawl");
        FSDataOutputStream compOS = fs.create(comp);
        FSDataOutputStream toCrawlOS = fs.create(toCrawl);
        return new EnhanceRecordWriter<>(compOS, toCrawlOS);
    }

    private class EnhanceRecordWriter<KEY, VALUE> extends RecordWriter<KEY, VALUE> {
        private FSDataOutputStream compOS;
        private FSDataOutputStream toCrawlOS;

        @Override
        public void write(KEY k, VALUE v) throws IOException {
            if (k.toString().contains("itiscomplete")) {
                compOS.writeUTF(k.toString());
            } else {
                toCrawlOS.writeUTF(k.toString());
            }
        }

        @Override
        public void close(TaskAttemptContext taskAttemptContext) throws IOException {
            if (compOS != null) {
                compOS.close();
            }
            if (toCrawlOS != null) {
                toCrawlOS.close();
            }
        }

        EnhanceRecordWriter(FSDataOutputStream compOS, FSDataOutputStream toCrawlOS) {
            this.compOS = compOS;
            this.toCrawlOS = toCrawlOS;
        }
    }
}
