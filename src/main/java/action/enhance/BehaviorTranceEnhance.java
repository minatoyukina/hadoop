package action.enhance;

import action.topkurl.TopKURL;
import action.util.DBLoader;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.util.HashMap;

public class BehaviorTranceEnhance {
    public static class EnhanceMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
        private HashMap<String, String> urlMap = new HashMap<>();

        @Override
        protected void setup(Context context) {
            DBLoader.loadDB(urlMap);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) {
            String line = value.toString();
            String[] fields = StringUtils.split(line, "\t");
            try {
                if (fields.length > 27 && StringUtils.isNotEmpty(fields[26])
                        && fields[26].startsWith("http")) {
                    String url = fields[26];
                    String info = urlMap.get(url);
                    String result;
                    if (info != null) {
                        result = line + "\t" + info + "\n\r";
                        context.write(new Text(result), NullWritable.get());
                    } else {
                        result = url + "\t" + "tocrawl" + "\n\r";
                        context.write(new Text(result), NullWritable.get());
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(TopKURL.class);

        job.setMapperClass(EnhanceMapper.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setOutputFormatClass(EnhanceOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
