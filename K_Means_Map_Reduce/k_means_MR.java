
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Scanner;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class k_means_MR extends Configured implements Tool 
{
	static int nc = 5;
	static float[][] new_cluster_mean = new float[nc][18];
	static float[][] old_cluster_mean = new float[nc][18];
	
	static HashMap<Integer,List<Float>> current_cluster_data = new HashMap<Integer,List<Float>>();
	static int iteration=1; static int reducer_count=0;
	
	public static class readClusterfile
	{
		public static float[][] readFile() throws FileNotFoundException
		{
			
			float[][] cluster_centroid = new float[nc][18];
			  int row=0,col=0;
			//System.out.println("in readfile");
			if(iteration == 1)
			{
				File file=new File("/home/srikanth/Desktop/data mining/cho_input.txt");
				Scanner scan=new Scanner(file);
				while(scan.hasNextLine())
		        {
		        	StringTokenizer st = new StringTokenizer(scan.nextLine());
		        	col=0;
		        	while(st.hasMoreTokens()) 
		        	{
		        		String token = st.nextToken();
		        		cluster_centroid[row][col] = Float.parseFloat(token);
		        	        	
		        		col++;
		        	}
		        	row++;
		        }
			}
			else
			{
				int cur_loc = iteration-1;
				String path = "/home/srikanth/workspace/DIC/output"+cur_loc+"/part-r-00000";
				File file=new File(path);
				Scanner scan=new Scanner(file);
				while(scan.hasNextLine())
		        {
		        	
		        	Matcher m = Pattern.compile("\\[(.*?)\\]").matcher(scan.nextLine());
		        	
		        	while(m.find()) 
		        	{
		        	    String[] s = m.group(1).split("\\s*,\\s*");
		        	    cluster_centroid[row][0] = row+1;
		        	    for(int i=1;i<s.length;i++)
		        	    {
		        	    	cluster_centroid[row][i] = Float.parseFloat(s[i]);
		        	    }
		        	}
		        	
		        	row++;
		        }
			}
				
			return cluster_centroid;
			
		}
	}
	
	
	public static class k_means_mapper  extends 
		Mapper<Object,Text,IntWritable, ArrayPrimitiveWritable>
	{
		static int count = 0;
	
		public void map(Object key, Text value, Context context)
		throws IOException, InterruptedException
		{
			
			StringTokenizer itr = new StringTokenizer(value.toString());
			float[] current_gene = new float[18];
			int col=0;

			while(itr.hasMoreTokens())
			{
				String token = itr.nextToken();
        		current_gene[col] = Float.parseFloat(token);

				col++;
			}
			
			float[][] cluster_means = new float[5][18];  
			
			
			readClusterfile rcf = new readClusterfile();
			cluster_means =  rcf.readFile();
				
			
			clusters c = new clusters();
		//	System.out.println(Arrays.toString(current_gene));
			int cluster_id = c.distance(current_gene, cluster_means, 5);
			
		//	System.out.println(cluster_id+"  "+current_gene[0]);
		//	System.out.println(Arrays.toString(current_gene));
			
			ArrayPrimitiveWritable awp = new ArrayPrimitiveWritable(current_gene);
			context.write(new IntWritable(cluster_id), awp);
				
			
		}
	}
	
	public static class k_means_Reducer extends
	Reducer<IntWritable,ArrayPrimitiveWritable, IntWritable, Text> 
	{
	private IntWritable result = new IntWritable();

	public void reduce(IntWritable key, Iterable<ArrayPrimitiveWritable> values,
		Context context) throws IOException, InterruptedException 
	{	
		reducer_count++;
		
		int cluster_id = key.get();
		int sum = 0;
		int count_gene=0;
		float[] current_gene = new float[18];
		for (ArrayPrimitiveWritable val : values) 
		{
			sum+=1;
			current_gene = (float[]) val.get();
			//System.out.println(key.toString()+" "+current_gene.toString());
			count_gene++;
					
			for(int i=2;i<18;i++)
			{
				new_cluster_mean[cluster_id-1][i]+=current_gene[i];
			}
			
			
			if (current_cluster_data.get(cluster_id) == null)  //gets the value for an id)
			    current_cluster_data.put(cluster_id, new ArrayList<Float>()); //no ArrayList assigned, create new ArrayList

			current_cluster_data.get(cluster_id).add(current_gene[0]);
		
			//System.out.println(key.toString()+" "+val.toString());
		}
		
		for(int i=2;i<18;i++)
		{
			new_cluster_mean[cluster_id-1][i] = new_cluster_mean[cluster_id-1][i] /count_gene;
			System.out.print(new_cluster_mean[cluster_id-1][i]+" ");
		}
		
		//	System.out.println(iteration+"----"+ Arrays.toString(new_cluster_mean[cluster_id-1]));
		//System.out.println(cluster_id+" "+sum);
		
		
		result.set(sum);
		context.write(key, new Text(Arrays.toString(new_cluster_mean[cluster_id-1])));
	}
}
	

	
	public int run(String[] args) throws Exception 
	{
		// TODO Auto-generated method stub
	
				
				Configuration conf = new Configuration();
					
				Job job = Job.getInstance(conf, "K-Means MR");
				
				job.setJarByClass(k_means_MR.class);
				
				job.setMapperClass(k_means_mapper.class);
				job.setReducerClass(k_means_Reducer.class);
				job.setOutputKeyClass(IntWritable.class);
				job.setOutputValueClass(Text.class);
				job.setMapOutputKeyClass(IntWritable.class);
				job.setMapOutputValueClass(ArrayPrimitiveWritable.class);
					
				FileInputFormat.setInputPaths(job, new Path(args[0]));
				
				FileOutputFormat.setOutputPath(job, new Path(args[iteration]));
				//iteration++;
				
				 return job.waitForCompletion(true)?0:1;
	}
	
	
	public static void main(String[] args) throws Exception
	{
		long startTime = System.currentTimeMillis();
		
		HashMap<Integer,List<Float>> previous_cluster_data = new HashMap<Integer,List<Float>>();
		int x =3;
		for(int i=1;i<9;i++)
		{
			System.out.println("--------------- "+i+"  -------------------------------------------------------");
			
			
			x = ToolRunner.run(new k_means_MR(), args);
			
			/*if(current_cluster_data.equals(previous_cluster_data) )
			{
				System.out.println(current_cluster_data);
				for(int j=1;j<=5;j++)
				{
					System.out.println(j+" "+current_cluster_data.get(j).size());
				}
					
				System.out.println(previous_cluster_data);
				for(int j=1;j<=5;j++)
				{
					System.out.println(j+" "+previous_cluster_data.get(j).size());
				}
				System.exit(0);
			}
			else
			{
				previous_cluster_data.clear();
				previous_cluster_data = current_cluster_data;
				current_cluster_data.clear();
				
			}*/
			
			iteration++;
				
			for(int j=0;j<5;j++)
				for(int k=0;k<18;k++)
					new_cluster_mean[j][k]= 0;
			
			
			System.out.println(current_cluster_data);
			for(int j=1;j<=5;j++)
			{
				System.out.println(j+" "+current_cluster_data.get(j).size());
			}
			current_cluster_data.clear();
			
			if(i==7)
			{
				long stopTime = System.currentTimeMillis();
			      long elapsedTime = stopTime - startTime;
			      System.out.println("Elapsed Time: "+elapsedTime);
				
				System.exit(0);
			}
			
		}
		
		
		
	}
}