package dbscan;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Scanner;
import java.util.StringTokenizer;

@SuppressWarnings("Duplicates")
public class DBScan_v1 {

    private static HashMap<Integer, ArrayList<Float>> data;
    private static ArrayList<Integer> visited = new ArrayList<>();
    private static ArrayList<Integer> cluster = new ArrayList<>();
    private static ArrayList<ArrayList<Integer>> clusters = new ArrayList<>();


    private HashMap<Integer, ArrayList<Float>> readFile() throws FileNotFoundException {

        HashMap<Integer, ArrayList<Float>> hashMap = new HashMap<>();

        File file = new File("inputs/cho.txt");
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

    public static void main(String[] args) throws FileNotFoundException {


        DBScan_v1 obj = new DBScan_v1();

        data = obj.readFile();

        double EPS = 3;

        int MINPTS = 10;

        int index = 1;

        while (index <= data.size()) {

            cluster.clear();
            obj.dbscan(index, EPS, MINPTS);

            if (cluster.size() >= MINPTS) {
                clusters.add(new ArrayList<>(cluster));
            }

            index += 1;
        }


        System.out.println(clusters.size());

        int total = 0;

        for (ArrayList<Integer> clusterTemp : clusters) {

            total += clusterTemp.size();

        }


        System.out.println(total);

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

        for (int i = 1; i <= data.size(); i++) {
            if (getDistance(data.get(id), data.get(i)) < eps && id != i) {
                neighborList.add(i);
            }
        }

        return neighborList;
    }

    private void dbscan(int currentPoint, double eps, int minPts) {

        if (!visited.contains(currentPoint)) {

            visited.add(currentPoint);
            cluster.add(currentPoint);

            ArrayList<Integer> neighbors = getNeighbors(currentPoint, eps);

            int pts = neighbors.size();

            if (pts >= minPts) {

                for (int neighbor : neighbors) {

                    dbscan(neighbor, eps, minPts);

                }

            }
        }


    }


}
