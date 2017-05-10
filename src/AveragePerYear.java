/**
 * Created by admin on 5/10/17.
 */
/**
 * Created by admin on 3/19/17.
 */
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AveragePerYear {

    public static class TokenizerMapper
            extends Mapper<LongWritable, Text, Text,Text>{

        // private final static IntWritable one = new IntWritable(1);
        //public static final Log log = LogFactory.getLog(TokenizerMapper.class);

        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
            /// System.out.println(value.toString());
            StringTokenizer itr = new StringTokenizer(value.toString(),",");
            int count =1;
            Text keyval = new Text();
            while (count<18 && itr.hasMoreTokens()) {
                if(count==5){
                    String elem = itr.nextToken().toString();
                    keyval.set(elem.trim());
                }
                if(count==17){
                    Text val = new Text();
                    String elemval = itr.nextToken().toString()+"**";
                    val.set(elemval.trim());
                    // System.out.println(elemval);
                    context.write(val,keyval);
                    System.out.println(val.toString()+","+keyval.toString());
                    break;
                }
                else{
                    itr.nextToken().toString();
                }
                count++;
            }
        }
    }

    public static class IntSumReducer
            extends Reducer<Text,Text,Text,Text> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            HashMap<String,Integer> hsmap = new HashMap();
            String newval ="";
            StringBuilder sb = new StringBuilder();
            int result =0;
            int count =0;
            for (Text value : values) {
                try {
                    int sum = Integer.parseInt(value.toString());
                    result = result +  sum;
                } catch (NumberFormatException e) {

                }
                count++;
            }
            int average = result/count;
            Text word = new Text();
            word.set(String.valueOf(average));
            System.out.println(key.toString()+","+word.toString());
            context.write(key,word);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(AveragePerYear.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
