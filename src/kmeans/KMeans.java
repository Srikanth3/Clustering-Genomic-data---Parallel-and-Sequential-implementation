package kmeans;

import common.Jaccard;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

@SuppressWarnings("Duplicates")
public class KMeans {

    static int count[] = new int[5];

    public void main(int n, String fileName) throws IOException {

        int file_int;
        switch (fileName) {
            case "cho":
                file_int = 4;
                break;
            case "iyer":
                file_int = 3;
                break;
            case "new_data_1":
                file_int = 1;
                break;
            default:
                file_int = 2;
                break;
        }


        final long startTime = System.currentTimeMillis();

        float[][] data = ReadFile.readFile(file_int);
        int rows = data.length;
        int cols = data[0].length;
        float[][] clusters_mean = new float[n][cols - 2];


        clusters_mean = initial_cluster(data, n);
        //	print_data.print(clusters,n,cols-2);

        HashMap<Integer, List<Float>> cluster_data = null;
        HashMap<Integer, List<Float>> new_cluster_data = null;
        HashMap<Integer, List<Float>> prev_cluster_data = null;

        for (int count = 0; count < 12; count++) {

            if (count == 0) {
                clusters_mean = initial_cluster(data, n);
                cluster_data = categorise_data(data, clusters_mean, n);

            } else {
                if (count == 1)
                    clusters_mean = cluster(cluster_data, data, n);
                else
                    clusters_mean = cluster(prev_cluster_data, data, n);

                new_cluster_data = categorise_data(data, clusters_mean, n);

                if (new_cluster_data.equals(prev_cluster_data)) {
                    System.out.println("No. of Iterations: " + count);

//                    System.out.println("prev_cluster_data " + prev_cluster_data);
//                    print(prev_cluster_data, n);
//
//                    System.out.println("new_cluster_data  " + new_cluster_data);
//                    print(new_cluster_data, n);

                    break;
                } else
                    prev_cluster_data = new_cluster_data;

            }
        }

        double jc = Jaccard.calculate(data, new_cluster_data);
        System.out.println();
        System.out.println("Jaccard Co-efficient: " + jc);
        final long endTime = System.currentTimeMillis();
        System.out.println("Total execution time: " + (endTime - startTime) + " milliseconds");


        int[] gene_array = pca_output(new_cluster_data, rows);
        write_to_file(gene_array, fileName);
    }

    private static int[] pca_output(HashMap<Integer, List<Float>> new_cluster_data, int rows) {
        int[] gene_array = new int[rows];
        for (Entry<Integer, List<Float>> entry : new_cluster_data.entrySet()) {
            Integer key = entry.getKey();
            List<Float> value = entry.getValue();
            String s;
            int num;

            for (Float aValue : value) {
                s = aValue + "";
                num = Integer.valueOf(s.split("\\.")[0]);
                gene_array[num - 1] = key;
            }
        }

        return gene_array;

    }

    private void write_to_file(int[] gene_array, String fileName) throws IOException {
        try {
            String path = "outputs/" + fileName + "_kmeans.txt";

            File file = new File(path);
            if (file.exists()) {
                file.delete();
            }
            if (!file.exists()) {
                file.createNewFile();
            }


            FileWriter fw = new FileWriter(file.getAbsoluteFile());
            BufferedWriter bw = new BufferedWriter(fw);
            int rows = gene_array.length;
            String s;
            int gene_id, cluster_id;
            for (int i = 0; i < rows; i++) {

                gene_id = i + 1;
                cluster_id = gene_array[i];
                s = gene_id + "	" + cluster_id;
                bw.write(s);
                bw.newLine();


            }
            bw.close();
        } catch (Exception e) {
            System.out.println(e);
        }
    }

    private static float[][] initial_cluster(float[][] data, int n) {
        int[] a = new int[n];  // initial cluster id

        int rows = data.length;
        int cols = data[0].length;

        float clusters[][] = new float[n][cols - 2];

        //System.out.println(rows+" "+cols);
        int x = (rows - 2) / (n - 1);
        //System.out.println("x: "+x);
        a[0] = 0;
        for (int i = 1; i < n; i++) {
            a[i] = a[i - 1] + x;

            if (a[i] > rows)
                a[i] = rows - 1;
        }
        System.out.println(Arrays.toString(a));

        int[] b = {39, 40, 41};
        System.arraycopy(b, 0, a, 0, b.length);
        int j = 0, l = 0;
        for (int i = 0; i < n; i++) {
            for (int k = 2; k < cols; k++) {
                //System.out.println(a[l]);
                clusters[i][j] = data[a[l]][k];
                j++;
            }
            l++;
            j = 0;
        }
        //print(clusters, n, cols-2);
        //categorise_data(data,clusters,n);

        return clusters;
    }


    private static HashMap<Integer, List<Float>> categorise_data(float[][] data, float[][] clusters, int num_clusters) {
        //HashMap <Integer,List<Float>>

        HashMap<Integer, List<Float>> cluster_data = new HashMap<>();
        int cluster_id;
        int k = 0;
        int cols = data[0].length;
        for (float[] aData : data) {
            float[] each_row = new float[cols - 2];
            for (int j = 2; j < cols && k < cols - 2; j++) {
                each_row[k] = aData[j];
                k++;
            }
            k = 0;

            cluster_id = distance(each_row, clusters, num_clusters);
            //System.out.println(cluster_id);

            cluster_data.putIfAbsent(cluster_id, new ArrayList<>()); //no ArrayList assigned, create new ArrayList

            cluster_data.get(cluster_id).add(aData[0]);
        }

        return cluster_data;
    }

    // calculates the distance between each row and the cluster median. returns the
    // cluster id nearest to it.

    private static int distance(float[] each_row, float[][] clusters, int n) {

        float[] distance = new float[n];

        for (int i = 0; i < n; i++) {
            for (int j = 0; j < each_row.length; j++) {
                distance[i] += (float) (Math.pow((each_row[j] - clusters[i][j]), 2.0));
            }
        }

        for (int i = 0; i < n; i++) {
            distance[i] = (float) Math.sqrt(distance[i]);
        }
        //	System.out.println(Arrays.toString(distance));
        float min = (float) 999999999999.9999;
        int index = -1;

        for (int i = 0; i < n; i++) {
            if (distance[i] < min) {
                index = i + 1;
                min = distance[i];
                //count[index+1] = count[index+1]+1;
            }
        }

        return index;
    }

    private static float[][] cluster(HashMap<Integer, List<Float>> prev_cluster_data, float[][] data, int n) {
        float clusters[][] = new float[n][16];
        int cols = data[0].length;
        for (int i = 0; i < n; i++) {
            for (int j = 0; j < cols - 2; j++) {
                clusters[i] = cluster_median(prev_cluster_data.get(i + 1), data);
                //	System.out.print(clusters[i][j]+" ");
            }
            //System.out.println();
        }
        return clusters;
    }

    private static float[] cluster_median(List<Float> list, float[][] data) {

        int cols = data[0].length;
        float[] m = new float[cols - 2];

        Float[] value = list.toArray(new Float[list.size()]);

        int temp = 0;
        String s = "";
        for (int j = 2; j < cols; j++) {
            for (Float aValue : value) {
                s = "" + aValue;
                temp = Integer.valueOf(s.split("\\.")[0]);
                m[j - 2] += data[temp - 1][j];
            }
        }

        for (int i = 0; i < m.length; i++) {
            m[i] = m[i] / value.length;
        }

        return m;
    }

    static HashMap<Integer, List<Integer>> ground_truth(float[][] data) {

        HashMap<Integer, List<Integer>> ground_truth = new HashMap<>();

        String s, s2;
        int num, num2;
        for (float[] aData : data) {
            s = aData[1] + "";
            num = Integer.valueOf(s.split("\\.")[0]);//Integer.parseInt(s);

            s2 = aData[0] + "";
            num2 = Integer.valueOf(s2.split("\\.")[0]);

            ground_truth.putIfAbsent(num, new ArrayList<>()); //no ArrayList assigned, create new ArrayList

            ground_truth.get(num).add(num2);
        }
        return ground_truth;

    }

    private static void print(float[][] data, int rows, int cols) {
        // TODO Auto-generated method stub
        //	int rows = data.length;
        //	int cols = data[0].length;

        for (int i = 0; i < rows; i++) {
            for (int j = 0; j < cols; j++) {
                System.out.print(data[i][j] + " ");
            }
            System.out.println();
        }
    }


    public static void print(HashMap<Integer, List<Float>> map, int n) {
        for (int i = 0; i < n; i++) {
            int val = i + 1;
            System.out.println(val + ": " + map.get(val).size());
        }
    }

    public static void print2(HashMap<Integer, List<Integer>> map, int n) {
        for (int i = 0; i < n; i++) {
            int val = i + 1;
            System.out.println(val + ": " + map.get(val).size());
        }
    }

}

// part of finding the 386x386 arrayf for ground truth

	/*	String s="",s2=""; int num=0,num2=0;
    int[] gene_array = new int[rows];
	for(int i=0;i<rows;i++)
	{
		s=data[i][1]+"";
		num = Integer.valueOf(s.split("\\.")[0]);

		gene_array[i] = num2;

	}*/
