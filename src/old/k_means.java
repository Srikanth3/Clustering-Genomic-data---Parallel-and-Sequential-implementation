package old;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

@SuppressWarnings("Duplicates")
public class k_means {

    static HashMap <Integer,List<Float>> cluster_data;
    static int count[] =new int[5];

    public static void main(String[] args) throws FileNotFoundException
    {
        int n =5; // clusters
        // (1) new_dataset1.txt (2) new_dataset2.text (3) iyer.txt (4) cho.txt
        float[][] data = ReadFile.readFile();
        int cols = data[0].length;
        float[][] clusters_mean = new float[n][cols-2];

        clusters_mean = initial_cluster(data,n);
        //	print_data.print(clusters,n,cols-2);

        HashMap <Integer,List<Float>> cluster_data = null;
        HashMap <Integer,List<Float>> new_cluster_data;
        HashMap <Integer,List<Float>> prev_cluster_data = null;
        for(int count = 0;count<50;count++)
        {
            if(count == 0)

            {
                clusters_mean = initial_cluster(data,n);
                cluster_data = categorise_data(data,clusters_mean,n);

                System.out.println(cluster_data);
                print(cluster_data,n);

            }
            else
            {
                if(count==1)
                    clusters_mean = cluster(cluster_data, data,n);
                else
                    clusters_mean = cluster(prev_cluster_data, data,n);

                //print_data.print(cluster,n,cols-2);

                new_cluster_data = categorise_data(data, clusters_mean,n);

                if( new_cluster_data.equals(prev_cluster_data))
                {
                    System.out.println("---------------------------------------------");
                    System.out.println("count: "+count);

                    System.out.println("prev_cluster_data "+ prev_cluster_data);
                    print(prev_cluster_data,n);

                    System.out.println("new_cluster_data  "+new_cluster_data);
                    print(new_cluster_data,n);

                    break;
                }
                else
                    prev_cluster_data = new_cluster_data;

            }
        }
    }

    public static void print(HashMap <Integer,List<Float>> map, int n)
    {
        for( int i=0;i<n;i++)
        {
            int val = i+1;
            System.out.println(val+": "+ map.get(val).size());
        }
    }

    public static float[][] initial_cluster(float[][] data, int n)
    {


        int[] a = new int[n];  // initial cluster id

        int rows = data.length;
        int cols = data[0].length;

        float clusters[][] = new float[n][cols-2];

        //System.out.println(rows+" "+cols);
        int x = (rows-2)/(n-1);
        //System.out.println("x: "+x);
        a[0] = 0;
        for(int i =1;i<n;i++)
        {
            a[i] = a[i-1]+x;

            if(a[i]> rows)
                a[i] = rows-1;
        }
        System.out.println(Arrays.toString(a));

			/*int[] b = {2,5};
			for(int i=0;i<b.length;i++)
			{
				a[i] = b[i];
			}*/
        int j=0,l=0;
        for(int i=0;i<n;i++)
        {
            for(int k=2;k<cols;k++)
            {
                //System.out.println(a[l]);
                clusters[i][j] = data[a[l]][k];
                j++;
            }
            l++;
            j=0;
        }
        //print(clusters, n, cols-2);
        //categorise_data(data,clusters,n);

        return clusters;
    }



    private static void print(float[][] data, int rows, int cols)
    {
        // TODO Auto-generated method stub
        //	int rows = data.length;
        //	int cols = data[0].length;

        for(int i=0;i<rows;i++)
        {
            for(int j=0;j<cols;j++)
            {
                System.out.print(data[i][j]+" ");
            }
            System.out.println();
        }


    }

    public static HashMap <Integer,List<Float>>  categorise_data(float[][] data, float[][] clusters, int num_clusters)
    {
        //HashMap <Integer,List<Float>>

        cluster_data = new HashMap <Integer,List<Float>>();
        int cluster_id=0;
        int k =0;
        int rows = data.length;
        int cols = data[0].length;
        for(int i=0;i<rows;i++)
        {
            float[] each_row = new float[cols-2];
            for(int j=2;j<cols && k<cols-2;j++)
            {
                each_row[k] = data[i][j];
                k++;
            }
            k=0;

            cluster_id = distance(each_row,clusters,num_clusters);
            //System.out.println(cluster_id);

            if (cluster_data.get(cluster_id) == null)  //gets the value for an id)
                cluster_data.put(cluster_id, new ArrayList<Float>()); //no ArrayList assigned, create new ArrayList

            cluster_data.get(cluster_id).add(data[i][0]);
        }

        return cluster_data;
    }

    // calculates the distance between each row and the cluster median. returns the
    // cluster id nearest to it.

    public static int distance(float[] each_row , float[][] clusters, int n)
    {

        float[] distance = new float[n];

        for(int i=0;i<n;i++)
        {
            for( int j=0;j<each_row.length;j++)
            {
                distance[i]+= (float) (Math.pow((each_row[j]- clusters[i][j]),2.0) );
            }
        }

        for(int i=0;i<n;i++)
        {
            distance[i] = (float) Math.sqrt(distance[i]);
        }
        //	System.out.println(Arrays.toString(distance));
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



    public static float[][] cluster(HashMap <Integer,List<Float>> prev_cluster_data, float[][] data,int n)
    {
        float clusters[][] = new float[n][16];
        int rows = data.length;
        int cols = data[0].length;
        for(int i=0;i<n;i++)
        {
            for(int j=0;j<cols-2;j++)
            {
                clusters[i] = cluster_median(prev_cluster_data.get(i+1),data);
                //	System.out.print(clusters[i][j]+" ");
            }
            //System.out.println();
        }
        return clusters;
    }

    private static float[] cluster_median(List<Float> list, float[][] data)
    {

        int rows = data.length;
        int cols = data[0].length;
        float[] m = new float[cols-2];

        Float[] value  = list.toArray(new Float[list.size()]);

        int temp=0;
        String s="";
        for(int j=2;j<cols;j++)
        {
            for (Float aValue : value) {
                s = "" + aValue;
                temp = Integer.valueOf(s.split("\\.")[0]);
                m[j - 2] += data[temp - 1][j];
            }
        }

        for(int i =0;i<m.length;i++)
        {
            m[i] = m[i]/value.length;
        }

        return m;
    }

}


	/*float[] c1 = new float[16];
	float[] c2 = new float[16];
	float[] c3 = new float[16];
	float[] c4 = new float[16];
	float[] c5 = new float[16];

	c1 = cluster2(prev_cluster_data.get(1),data);
	c2 = cluster2(prev_cluster_data.get(2),data);
	c3 = cluster2(prev_cluster_data.get(3),data);
	c4 = cluster2(prev_cluster_data.get(4),data);
	c5 = cluster2(prev_cluster_data.get(5),data);

	for(int i=0;i<16;i++)
	{
		clusters[0][i] = c1[i];
		clusters[1][i] = c2[i];
		clusters[2][i] = c3[i];
		clusters[3][i] = c4[i];
		clusters[4][i] = c5[i];

	}*/


	/*	float d1 = (float) 0.0, d2 = (float) 0.0, d3 = (float) 0.0;
	float d4 = (float) 0.0, d5 = (float) 0.0;
	//System.out.println(Arrays.toString(each_row));
	for(int i=0;i<each_row.length;i++)
	{
	   d1 = (float) (d1 + Math.pow((each_row[i]- clusters[0][i]),2.0));
	   d2 = (float) (d2 + Math.pow((each_row[i]- clusters[1][i]),2.0));
	   d3 = (float) (d3 + Math.pow((each_row[i]- clusters[2][i]),2.0));
	   d4 = (float) (d4 + Math.pow((each_row[i]- clusters[3][i]),2.0));
	   d5 = (float) (d5 + Math.pow((each_row[i]- clusters[4][i]),2.0));
	}

	d1 = (float) Math.sqrt(d1);
	d2 = (float) Math.sqrt(d2);
	d3 = (float) Math.sqrt(d3);
	d4 = (float) Math.sqrt(d4);
	d5 = (float) Math.sqrt(d5);

	// System.out.println(d1+" "+d2+" "+d3+" "+d4+" "+d5);

	float[] a = new float[5];
	a[0] = d1; a[1] = d2; a[2] = d3; a[3] = d4; a[4] = d5;



	for(int i=0;i<a.length;i++)
	{
		if(a[i]<min)
		{
			index = i;
			min = a[i];
			//count[index+1] = count[index+1]+1;
		}

	}
	index = index+1;
	//  System.out.println(min+" index: "+index);
	//  System.out.println(Arrays.toString(count));*/


	/*	List<Float> l1 = prev_cluster_data.get(1);
	Float[] value1  = l1.toArray(new Float[l1.size()]);

	List<Float> l2 = prev_cluster_data.get(2);
	Float[] value2  = l2.toArray(new Float[l2.size()]);

	List<Float> l3 = prev_cluster_data.get(3);
	Float[] value3  = l1.toArray(new Float[l3.size()]);

	List<Float> l4 = prev_cluster_data.get(4);
	Float[] value4  = l1.toArray(new Float[l4.size()]);

	List<Float> l5 = prev_cluster_data.get(5);
	Float[] value5  = l1.toArray(new Float[l5.size()]);

	float[] m1 = new float[16];

	int temp=0;
	String s="";
	for(int j=2;j<18;j++)
	{
		for( int i=0;i<value1.length;i++)
		{
			 s=""+value1[i];
			temp = Integer.valueOf(s);
			m1[j-2]+= data[temp-1][j];
		}
	}

	for(int i =0;i<m1.length;i++)
	{
		m1[i] = m1[i]/value1.length;
	}*/

	/*float c1[] = new float[16];
	float c2[] = new float[16];
	float c3[] = new float[16];
	float c4[] = new float[16];
	float c5[] = new float[16];*/

	/*for(int i=2;i<18;i++)
	{
		c1[j] = data[0][i];
		c2[j] = data[96][i];
		c3[j] = data[194][i];
		c4[j] = data[288][i];
		c5[j] = data[384][i];
		j++;
	}*/

	/*System.out.println("c1: "+Arrays.toString(c1));
	System.out.println("c2: "+Arrays.toString(c2));
	System.out.println("c3: "+Arrays.toString(c3));
	System.out.println("c4: "+Arrays.toString(c4));
	System.out.println("c5: "+Arrays.toString(c5));*/

	/*for(int i=0;i<16;i++)
	{
		clusters[0][i] = c1[i];
		clusters[1][i] = c2[i];
		clusters[2][i] = c3[i];
		clusters[3][i] = c4[i];
		clusters[4][i] = c5[i];

	}*/


