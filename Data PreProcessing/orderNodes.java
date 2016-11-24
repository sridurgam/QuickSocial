import java.io.IOException;
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

public class orderNodes {
  //mapper class
  public static class NodeMapper
    extends Mapper<Object, Text, IntWritable, Text>{

	//declaring mapper's output key and value holders
    private IntWritable count = new IntWritable();
    private Text node = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      String[] array = value.toString().split("\\s+");

      //extracting the community id
      count.set(Integer.parseInt(array[1]));
      node.set(array[0]);
      
      context.write(count,node);
    }
  }

  //reducer class
  public static class NodeReducer
       extends Reducer<IntWritable,Text,IntWritable,Text> {
    private IntWritable result = new IntWritable();

    public void reduce(IntWritable key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      for (Text node : values) {
    	  context.write(key, node);
      }
    }
  }

  public static void main(String[] args) throws Exception {
	try{
	    Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf);
	    //setting job properties
	    job.setJarByClass(orderNodes.class);
	    job.setMapperClass(NodeMapper.class);
	    job.setNumReduceTasks(1);
	    job.setReducerClass(NodeReducer.class);
	    job.setOutputKeyClass(IntWritable.class);
	    job.setOutputValueClass(Text.class);
	    //adding input and output file paths
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    job.waitForCompletion(true);
	    System.exit(1);
	}
	catch(Exception e){
		e.getMessage();
	}
  }
}
