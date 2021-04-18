// Importing libraries
import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class DCReducer extends MapReduceBase implements Reducer<Text,
									IntWritable, Text, FloatWritable> {

	// Reduce function
	public void reduce(Text key, Iterator<IntWritable> value,
				OutputCollector<Text, FloatWritable> output,
							Reporter rep) throws IOException
	{
		float count = 0;

		while (value.hasNext())
		{
			IntWritable i = value.next();
			count += i.get();
		}
		System.out.println(key + " " +value);
		if(key.compareTo(new Text("TYPE-1"))==0)
		{
			output.collect(key, new FloatWritable((count/5)*100));
		}
		if(key.compareTo(new Text("TYPE-2"))==0)
		{
			output.collect(key, new FloatWritable((count/8)*100));
		}
		if(key.compareTo(new Text("TYPE-3"))==0)
		{
			output.collect(key, new FloatWritable((count/2)*100));
		}
		if(key.compareTo(new Text("TYPE-4"))==0)
		{
			output.collect(key, new FloatWritable((count/2)*100));
		}
	}
}
