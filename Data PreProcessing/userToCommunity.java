import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class userToCommunity {
  //mapper class
  public static class CommunityMapper
    extends Mapper<Object, Text, Text,Text>{

	//declaring mapper's output key and value holders
    private Text memberOut = new Text();
    private Text idOut = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      String[] array = value.toString().split("\\s+");
      //extracting the community id
      idOut.set(array[0]);
      String member = new String();
      for(int i=1;i<array.length;i++)
      {
    	  member = array[i];
    	  memberOut.set(member);
    	  context.write(memberOut, idOut);
      }
    }
  }

  //reducer class
  public static class CommunityReducer
       extends Reducer<Text,Text,Text,Text> {
    private Text result = new Text();

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      String out = new String(); 
      for (Text val : values) {
        out = out + val.toString();
        out = out + "\t";
      }
      result.set(out);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf);
    //setting job properties
    job.setJarByClass(userToCommunity.class);
    job.setMapperClass(CommunityMapper.class);
    job.setCombinerClass(CommunityReducer.class);
    job.setNumReduceTasks(1);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    //adding input and output file paths
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}