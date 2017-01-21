package common;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Jaccard {


    private static int[][] fillGroundTruthMatrix(float[][] data) {
        int rows = data.length;
        int[][] groundtm = new int[rows][rows];

        for (int i = 0; i < rows; i++) {
            for (int j = 0; j < rows; j++) {
                if (data[i][1] == data[j][1])
                    groundtm[i][j] = groundtm[j][i] = 1;
                else
                    groundtm[i][j] = groundtm[j][i] = 0;
            }
        }
        return groundtm;


    }

    private static int[][] final_cluster_matrix(HashMap<Integer, List<Float>> calculated_cluster_data, int rows) {
        int[][] final_cluster_matrix = new int[rows][rows];
        int[] gene_array = new int[rows];
        for (Map.Entry<Integer, List<Float>> entry : calculated_cluster_data.entrySet()) {
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

        for (int i = 0; i < rows; i++) {
            for (int j = 0; j < rows; j++) {
                if (gene_array[i] == gene_array[j])

                    final_cluster_matrix[i][j] = final_cluster_matrix[j][i] = 1;
                else
                    final_cluster_matrix[i][j] = final_cluster_matrix[j][i] = 0;
            }
        }
        return final_cluster_matrix;

    }

    public static double calculate(float[][] data, HashMap<Integer, List<Float>> final_cluster_data) {
        int rows = data.length;
        int[][] ground_truth_matrix = fillGroundTruthMatrix(data);
        int[][] final_cluster_matrix = final_cluster_matrix(final_cluster_data, rows);

        int m11 = 0, m10 = 0, m01 = 0;

        for (int i = 0; i < ground_truth_matrix.length; i++) {
            for (int j = 0; j < ground_truth_matrix.length; j++) {
                int val = ground_truth_matrix[i][j] * 10 + final_cluster_matrix[i][j];

                if (val == 11)
                    m11++;
                else if (val == 10)
                    m10++;
                else if (val == 1)
                    m01++;
            }
        }

        return (double) m11 / (double) (m11 + m10 + m01);

    }

}
