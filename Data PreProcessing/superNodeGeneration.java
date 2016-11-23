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

public class superNodeGeneration {
  //mapper class
  public static class superNodeMapper
    extends Mapper<Object, Text, Text,IntWritable>{

	//declaring mapper's output key and value holders
    private Text s1 = new Text();
    private Text s2 = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      String[] array = value.toString().split("\\s+");

      //extracting the community id
      s1.set(array[0]);
      s2.set(array[1]);
      
      context.write(s1, new IntWritable(1));
      context.write(s2, new IntWritable(1));
    }
  }

  //reducer class
  public static class superNodeReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      Integer out = 0; 
      for (IntWritable val : values) {
        out = out + 1;
      }
      result.set(out);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
	try{
	    Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf);
	    //setting job properties
	    job.setJarByClass(superNodeGeneration.class);
	    job.setMapperClass(superNodeMapper.class);
	    job.setNumReduceTasks(1);
	    job.setReducerClass(superNodeReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    //adding input and output file paths
	    FileInputFormat.setInputDirRecursive(job, true);
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