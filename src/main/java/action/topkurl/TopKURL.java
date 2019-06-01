package action.topkurl;

import mr.flowsum.FlowBean;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class TopKURL {
    private static class TopKURLMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
        private Text k = new Text();
        private FlowBean bean = new FlowBean();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = StringUtils.split(line, "\t");

            if (fields.length > 32 && StringUtils.isNotEmpty(fields[26]) && fields[26].startsWith("http")) {
                String url = fields[26];
                long up_flow = Long.parseLong(fields[30]);
                long d_flow = Long.parseLong(fields[31]);
                k.set(url);
                bean.set("", up_flow, d_flow);
                context.write(k, bean);
            }
        }
    }

    private static class TopKURLReducer extends Reducer<Text, FlowBean, Text, LongWritable> {
        private TreeMap<FlowBean, Text> treeMap = new TreeMap<>();
        private double flow_amount = 0;

        @Override
        protected void reduce(Text key, Iterable<FlowBean> values, Context context) {
            Text url = new Text(key.toString());
            long up_sum = 0;
            long d_sum = 0;
            for (FlowBean value : values) {
                up_sum += value.getUp_flow();
                d_sum += value.getD_flow();
            }
            FlowBean bean = new FlowBean("", up_sum, d_sum);
            flow_amount += bean.getS_flow();
            treeMap.put(bean, url);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            double tempCount = 0;
            Set<Map.Entry<FlowBean, Text>> entrySet = treeMap.entrySet();
            for (Map.Entry<FlowBean, Text> entry : entrySet) {
                if (tempCount / flow_amount < 0.8) {
                    context.write(entry.getValue(), new LongWritable(entry.getKey().getS_flow()));
                    tempCount += entry.getKey().getS_flow();

                } else {
                    return;
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(TopKURL.class);

        job.setMapperClass(TopKURLMapper.class);
        job.setReducerClass(TopKURLReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
