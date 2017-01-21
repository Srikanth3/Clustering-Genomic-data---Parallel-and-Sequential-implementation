package hierarchical;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

@SuppressWarnings("Duplicates")
public class HierarchicalClustering {

    private TreeMap<Double, Integer> clusterIdMap;

    public void mainHAC(int kValue, String fileName) throws FileNotFoundException {

        List<Cluster> resultClusterList = new ArrayList<>();

        ReadFile rf = new ReadFile();
        List<Cluster> data = rf.readFile(fileName);
        List<Cluster> initialClusterSet = new ArrayList<>();

        initialClusterSet.addAll(data);

        long startTime = System.currentTimeMillis();

        // Build clusters
        while (data.size() > kValue) {

            Cluster nextCluster = getNextSet(data);
            resultClusterList.add(nextCluster);
            // System.out.print(no + ". ");
            // nextCluster.printId();

        }

        clusterIdMap = new TreeMap<>();
        int id = 1;

        // Fill cluster Id Matrix
        for (Cluster c : data) {
            List<List<Double>> entryRow = c.entryRow;

            for (List<Double> gene : entryRow)
                clusterIdMap.put(gene.get(0), id);
            id++;
        }

        System.out.println("Jaccard Coefficient : " + calculateJaccard(initialClusterSet));

        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        System.out.println("Time: " + duration + " milliseconds");

        writeResultsToFile(fileName);
    }

    private Cluster getNextSet(List<Cluster> data) {

        Cluster[] nextSet = new Cluster[2];
        double minDist = Double.MAX_VALUE;

        for (int i = 0; i < data.size(); i++) {
            for (int j = i + 1; j < data.size(); j++) {
                double distance = data.get(i).getDistance(data.get(j));

                if (distance < minDist) {
                    nextSet = new Cluster[]{data.get(i), data.get(j)};
                    minDist = distance;
                }
            }
        }
        return combineClusterSet(nextSet, data, minDist);
        // return nextSet;
    }

    private Cluster combineClusterSet(Cluster[] nextSet, List<Cluster> data, double distance) {

        int index = data.indexOf(nextSet[0]);
        Cluster a = data.get(index);
        data.remove(index);
        int index2 = data.indexOf(nextSet[1]);
        Cluster b = data.get(index2);
        data.remove(index2);

        Cluster nextCluster = new Cluster();
        nextCluster.addRow(a.entryRow);
        nextCluster.addRow(b.entryRow);
        nextCluster.distance = distance;
        data.add(nextCluster);
        return nextCluster;

    }

    private int[][] fillGroundTruthMatrix(List<Cluster> initialClusterSet) {

        int size = initialClusterSet.size();
        int[][] groundTruthMatrix = new int[size][size];

        for (int i = 0; i < size; i++) {
            for (int j = i; j < size; j++) {
                if (initialClusterSet.get(i).getGroundTruth() == initialClusterSet.get(j).getGroundTruth())
                    groundTruthMatrix[i][j] = groundTruthMatrix[j][i] = 1;
                else
                    groundTruthMatrix[i][j] = groundTruthMatrix[j][i] = 0;
            }
        }

        return groundTruthMatrix;

    }

    private int[][] fillClusterIdMatrix(List<Cluster> initialClusterSet) {

        int size = initialClusterSet.size();
        int[][] clusterIdMatrix = new int[size][size];

        for (int i = 0; i < size; i++) {
            for (int j = i; j < size; j++) {
                Cluster first = initialClusterSet.get(i);
                Cluster second = initialClusterSet.get(j);
                double firstGeneId = first.entryRow.get(0).get(0);
                double secondGeneId = second.entryRow.get(0).get(0);

                if (Objects.equals(clusterIdMap.get(firstGeneId), clusterIdMap.get(secondGeneId)))
                    clusterIdMatrix[i][j] = clusterIdMatrix[j][i] = 1;
                else
                    clusterIdMatrix[i][j] = clusterIdMatrix[j][i] = 0;
            }
        }

        return clusterIdMatrix;
    }

    private double calculateJaccard(List<Cluster> initialClusterSet) {

        int[][] clusterIdMatrix = fillClusterIdMatrix(initialClusterSet);
        int[][] groundTruthMatrix = fillGroundTruthMatrix(initialClusterSet);

        int m11 = 0, m10 = 0, m01 = 0;

        for (int i = 0; i < groundTruthMatrix.length; i++) {
            for (int j = 0; j < groundTruthMatrix.length; j++) {
                int val = groundTruthMatrix[i][j] * 10 + clusterIdMatrix[i][j];

                if (val == 11)
                    m11++;
                else if (val == 10)
                    m10++;
                else if (val == 1)
                    m01++;
            }
        }
//		System.out.println(m11 + "|" + m10 + "|" + m01 + "|" + m00);

        return (double) m11 / (double) (m11 + m10 + m01);

    }

    private void writeResultsToFile(String fileName) {

        Iterator it = clusterIdMap.entrySet().iterator();

        List<String> lines = new ArrayList<>();

        while (it.hasNext()) {
            Map.Entry pair = (Map.Entry) it.next();

            double db_gene_id = (double) pair.getKey();
            int gene_id = (int) db_gene_id;
            int cluster_id = (int) pair.getValue();
            lines.add(String.valueOf(gene_id) + " " + cluster_id);
//			System.out.println(sb.toString());
        }
        Path file = Paths.get("outputs/" + fileName + "_hierarchical.txt");
        try {
            Files.write(file, lines, Charset.forName("UTF-8"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
