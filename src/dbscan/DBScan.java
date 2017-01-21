package dbscan;

import common.Jaccard;
import kmeans.ReadFile;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.*;

@SuppressWarnings("Duplicates")
public class DBScan {

    private static HashMap<Integer, ArrayList<Float>> expressionData;
    private static HashMap<Integer, Integer> cluster_map = new HashMap<>();
    private static ArrayList<Integer> visited = new ArrayList<>();
    private static ArrayList<Integer> noise = new ArrayList<>();
    private static HashMap<Integer, List<Float>> clusters = new HashMap<>();

    private static int cluster_num = 0;


    private HashMap<Integer, ArrayList<Float>> readFile(String fileName) throws FileNotFoundException {

        HashMap<Integer, ArrayList<Float>> hashMap = new HashMap<>();

        File file = new File("inputs/" + fileName + ".txt");
        Scanner scan = new Scanner(file);

        int row = 0, col;

        while (scan.hasNextLine()) {
            StringTokenizer st = new StringTokenizer(scan.nextLine());
            col = 0;
            ArrayList<Float> tempList = new ArrayList<>();
            while (st.hasMoreTokens()) {
                String token = st.nextToken();
                if (col > 1) {

                    tempList.add(Float.parseFloat(token));
                }

                col++;
            }
            row++;

            hashMap.put(row, tempList);

        }

        return hashMap;
    }

    public void main(double EPS, int MINPTS, String fileName) throws FileNotFoundException {


        expressionData = readFile(fileName);


        final long startTime = System.currentTimeMillis();
        dbScan(EPS, MINPTS);
        final long endTime = System.currentTimeMillis();

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

        for (int point :
                noise) {

            cluster_map.put(point, -1);

        }


        try {
            PrintWriter writer = new PrintWriter("outputs/" + fileName + "_dbscan.txt", "UTF-8");


            for (int i = 1; i <= expressionData.size(); i++) {

                writer.println(i + "\t" + cluster_map.get(i));

            }


            writer.close();
        } catch (Exception e) {
            // do something
        }


        // Jaccard Calculation
        float[][] data = ReadFile.readFile(file_int);

        double jaccardCoefficient = Jaccard.calculate(data, clusters);


        System.out.println("EPS = " + EPS);
        System.out.println("MINPTS = " + MINPTS);
        System.out.println("Number of Clusters = " + clusters.size());
        System.out.println();
        System.out.println("Jaccard Coefficient = " + jaccardCoefficient);
        System.out.println("Total execution time: " + (endTime - startTime) + " milliseconds");
    }

    private double getDistance(ArrayList<Float> gene1, ArrayList<Float> gene2) {

        double dist = 0;

        for (int index = 0; index < gene1.size(); index++) {
            double diff = gene1.get(index) - gene2.get(index);
            dist += (diff * diff);
        }

        return Math.sqrt(dist);
    }

    private ArrayList<Integer> getNeighbors(int id, double eps) {

        ArrayList<Integer> neighborList = new ArrayList<>();

        for (int i = 1; i <= expressionData.size(); i++) {
            if ((getDistance(expressionData.get(id), expressionData.get(i)) <= eps) && id != i) {
                neighborList.add(i);
            }
        }

        return neighborList;
    }

    private void dbScan(double eps, int minPts) {


        expressionData.keySet().stream().filter(currentPoint -> !visited.contains(currentPoint)).forEach(currentPoint -> {

            visited.add(currentPoint);

            ArrayList<Integer> neighbors = getNeighbors(currentPoint, eps);

            if (!(neighbors.size() < minPts)) {

                cluster_num += 1;

                expandCluster(currentPoint, neighbors, cluster_num, eps, minPts);


            } else {

                noise.add(currentPoint);
            }
        });


    }


    private void expandCluster(int currentPoint, ArrayList<Integer> neighbors, int C, double eps, int minPts) {

        ArrayList<Float> cluster = new ArrayList<>();

        cluster_map.put(currentPoint, C);

        cluster.add((float) currentPoint);

        for (int neighbor : new ArrayList<>(neighbors)) {

            if (!visited.contains(neighbor)) {
                visited.add(neighbor);

                ArrayList<Integer> childNeighbors = getNeighbors(neighbor, eps);

                if (childNeighbors.size() >= minPts) {
                    neighbors.addAll(childNeighbors);
                }

            }

            if (!cluster_map.containsKey(neighbor)) {

                cluster_map.put(neighbor, C);
                cluster.add((float) neighbor);

            }


        }


        clusters.put(C, cluster);


    }


}
