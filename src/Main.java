import dbscan.DBScan;
import hierarchical.HierarchicalClustering;
import kmeans.KMeans;

import java.io.IOException;

@SuppressWarnings("Duplicates")
public class Main {

    public static void main(String[] args) throws IOException {

        //Variables
        String FILENAME = "new_data_1";

        //DBSCAN
        double EPS = 4;
        int MINPTS = 3;

        //KMEANS
        int NUMCLUSTERS = 3;

        //HIERARCHICAL
        int noOfClusters=2;

        System.out.println("--------------K-Means--------------");
        KMeans kMeans = new KMeans();
        kMeans.main(NUMCLUSTERS,FILENAME);

//        System.out.println();
//        System.out.println("--------------DBSCAN--------------");
//        System.out.println();
//        DBScan dbScan = new DBScan();
//        dbScan.main(EPS, MINPTS, FILENAME);
//
//        System.out.println();
//        System.out.println("--------------Hierarchical--------------");
//        HierarchicalClustering hc = new  HierarchicalClustering();
//        hc.mainHAC(noOfClusters,FILENAME);


    }


}
