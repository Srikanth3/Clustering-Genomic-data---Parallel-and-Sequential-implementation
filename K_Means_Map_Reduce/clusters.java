import org.apache.catalina.tribes.util.Arrays;


public class clusters {
	
	public static int distance(float[] each_row , float[][] clusters, int n)
	{
		
		float[] distance = new float[n];
		
		/*for(int i=0;i<each_row.length;i++)
		{
			System.out.print(each_row[i]+" ");			
		}*/
		
		for(int i=0;i<n;i++)
		{
			for( int j=2;j<each_row.length;j++)
			{
				distance[i]+= (float) (Math.pow((each_row[j]- clusters[i][j]),2.0) );
			}
		}
		
		for(int i=0;i<n;i++)
		{
			distance[i] = (float) Math.sqrt(distance[i]); 
		//	System.out.print(distance[i]+"		");
		}
		//System.out.println();
		
		 float min = (float) 999999999999.9999;
	     int index =-1;
		
	    for(int i=0;i<n;i++)
	    {
	    	if(distance[i]<min)
        	{
        		index = i+1;
        		min = distance[i];
        		//count[index+1] = count[index+1]+1;
        	}
	    }
		
     return index;
	} 

}
