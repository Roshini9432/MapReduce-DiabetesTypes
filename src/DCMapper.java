// Importing libraries
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class DCMapper extends MapReduceBase implements Mapper<LongWritable,
												Text, Text, IntWritable> {

	// Map function
	public void map(LongWritable key, Text value, OutputCollector<Text,
				IntWritable> output, Reporter rep) throws IOException
	{
		
		String line = value.toString();

		for (String parameter : line.split(" "))
		{
			if (parameter.length() > 0)
			{
				String[] factors = parameter.split("-",2);
				System.out.println(factors[0]+" HI "+factors[1]);
				if(factors[0].compareTo("FAMILYHISTORY")==0 && factors[1].compareTo("YES")==0)
				{
					output.collect(new Text("TYPE-1"), new IntWritable(1));
				}
				else
				{
					if(factors[0].compareTo("FAMILYHISTORY")==0 && !(factors[1].compareTo("YES")==0))
					{
						output.collect(new Text("TYPE-1"), new IntWritable(0));
					}
				}
				if(factors[0].compareTo("AGE")==0)
				{
					if(Integer.parseInt(factors[1])<14)
					{
						output.collect(new Text("TYPE-1"), new IntWritable(1));
					}
					else
					{
						if(!(Integer.parseInt(factors[1])<14))
						{
							output.collect(new Text("TYPE-1"), new IntWritable(0));
						}
					}
					if(Integer.parseInt(factors[1])>45)
					{
						output.collect(new Text("TYPE-2"), new IntWritable(1));
					}
					else
					{
						if(!(Integer.parseInt(factors[1])>45))
						{
							output.collect(new Text("TYPE-2"), new IntWritable(0));
						}
					}
					if(Integer.parseInt(factors[1])>60)
					{
						output.collect(new Text("TYPE-4"), new IntWritable(1));
					}
					else
					{
						if(!(Integer.parseInt(factors[1])>60))
						{
							output.collect(new Text("TYPE-4"), new IntWritable(0));
						}
					}
				}
				if(factors[0].compareTo("BSL")==0 && Integer.parseInt(factors[1])>160)
				{
					output.collect(new Text("TYPE-1"), new IntWritable(1));
					output.collect(new Text("TYPE-2"), new IntWritable(1));
				}
				else
				{
					if(factors[0].compareTo("BSL")==0 && !(Integer.parseInt(factors[1])>160))
					{
						output.collect(new Text("TYPE-1"), new IntWritable(0));
						output.collect(new Text("TYPE-2"), new IntWritable(0));
					}
				}
				if(factors[0].compareTo("POLYURIA")==0 && factors[1].compareTo("YES")==0)
				{
					output.collect(new Text("TYPE-1"), new IntWritable(1));
				}
				else
				{
					if(factors[0].compareTo("POLYURIA")==0 && !(factors[1].compareTo("YES")==0))
					{
						output.collect(new Text("TYPE-1"), new IntWritable(0));
					}
				}
				if(factors[0].compareTo("POLYDIPSIA")==0 && factors[1].compareTo("YES")==0)
				{
					output.collect(new Text("TYPE-1"), new IntWritable(1));
				}
				else
				{
					if(factors[0].compareTo("POLYDIPSIA")==0 && !(factors[1].compareTo("YES")==0))
					{
						output.collect(new Text("TYPE-1"), new IntWritable(0));
					}
				}
				if(factors[0].compareTo("PCOS")==0 && factors[1].compareTo("YES")==0)
				{
					output.collect(new Text("TYPE-2"), new IntWritable(1));
				}
				else
				{
					if(factors[0].compareTo("PCOS")==0 && !(factors[1].compareTo("YES")==0))
					{
						output.collect(new Text("TYPE-2"), new IntWritable(0));
					}
				}
				if(factors[0].compareTo("BMI")==0 && Integer.parseInt(factors[1])>25)
				{
					output.collect(new Text("TYPE-2"), new IntWritable(1));
				}
				else
				{
					if(factors[0].compareTo("BMI")==0 && !(Integer.parseInt(factors[1])>25))
					{
						output.collect(new Text("TYPE-2"), new IntWritable(0));
					}
				}
				if(factors[0].compareTo("HDL")==0 && Integer.parseInt(factors[1])<40)
				{
					output.collect(new Text("TYPE-2"), new IntWritable(1));
				}
				else
				{
					if(factors[0].compareTo("HDL")==0 && !(Integer.parseInt(factors[1])<40))
					{
						output.collect(new Text("TYPE-2"), new IntWritable(0));
					}
				}
				if(factors[0].compareTo("BTL")==0 && Integer.parseInt(factors[1])>150)
				{
					output.collect(new Text("TYPE-2"), new IntWritable(1));
				}
				else
				{
					if(factors[0].compareTo("BTL")==0 && !(Integer.parseInt(factors[1])>150))
					{
						output.collect(new Text("TYPE-2"), new IntWritable(0));
					}
				}
				if(factors[0].compareTo("GCL")==0 && Integer.parseInt(factors[1])<40)
				{
					output.collect(new Text("TYPE-2"), new IntWritable(1));
				}
				else
				{
					if(factors[0].compareTo("GCL")==0 && !(Integer.parseInt(factors[1])<40))
					{
						output.collect(new Text("TYPE-2"), new IntWritable(0));
					}
				}
				if(factors[0].compareTo("INACTIVITY")==0 && factors[1].compareTo("YES")==0)
				{
					output.collect(new Text("TYPE-2"), new IntWritable(1));
				}
				else
				{
					if(factors[0].compareTo("INACTIVITY")==0 && !(factors[1].compareTo("YES")==0))
					{
						output.collect(new Text("TYPE-2"), new IntWritable(0));
					}
				}
				if(factors[0].compareTo("PDH")==0 && factors[1].compareTo("YES")==0)
				{
					output.collect(new Text("TYPE-3"), new IntWritable(1));
				}
				else
				{
					if(factors[0].compareTo("PDH")==0 && !(factors[1].compareTo("YES")==0))
					{
						output.collect(new Text("TYPE-3"), new IntWritable(0));
					}
				}
				if(factors[0].compareTo("AD")==0 && factors[1].compareTo("YES")==0)
				{
					output.collect(new Text("TYPE-3"), new IntWritable(1));
				}
				else
				{
					if(factors[0].compareTo("AD")==0 && !(factors[1].compareTo("YES")==0))
					{
						output.collect(new Text("TYPE-3"), new IntWritable(0));
					}
				}
				if(factors[0].compareTo("TREGS")==0 && Integer.parseInt(factors[1])>2)
				{
					output.collect(new Text("TYPE-4"), new IntWritable(1));
				}
				else
				{
					if(factors[0].compareTo("TREGS")==0 && !(Integer.parseInt(factors[1])>2))
					{
						output.collect(new Text("TYPE-4"), new IntWritable(0));
					}
				}
				
			}
			
		}
	}
}
