import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;


public class MaxTem {

    public static class Map extends Mapper<LongWritable,Text,IntWritable,IntWritable>
    {
        public void map(LongWritable key, Text value, Context context)  throws IOException,InterruptedException
        {

            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line,",");
            int i=0;
            int year =0,temp=0;

            while(tokenizer.hasMoreTokens())
            {
                if(i==0)  year = Integer.parseInt(tokenizer.nextToken());
                else if(i==1) temp = Integer.parseInt(tokenizer.nextToken());
                else  break;
                i++;
            }
            context.write(new IntWritable(year), new IntWritable(temp));
        }

    }

    public static class Reduce extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable>
    {
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)  throws IOException,InterruptedException
        {
            Integer year =0,temp=0;

            int max = 0;
            // TODO Auto-generated method stub
            for(IntWritable x: values)
            {
                temp = Math.max(max,x.get());
            }


            context.write(key, new IntWritable(temp));

        }
    }

    public static void main(String[] args) throws Exception {
        // TODO Auto-generated method stubwhat is meaning of value in bigdata

        //JobConf conf = new JobConf(WordCount.class);
        Configuration conf= new Configuration();


        //conf.setJobName("mywc");
        Job job = Job.getInstance(conf);

        job.setJarByClass(MaxTem.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        //conf.setMapperClass(Map.class);
        //conf.setReducerClass(Reduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);



        Path outputPath = new Path(args[1]);

        //Configuring the input/output path from the filesystem into the job

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //deleting the output path automatically from hdfs so that we don't have delete it explicitly

        outputPath.getFileSystem(conf).delete(outputPath);

        //exiting the job only if the flag value becomes false

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}