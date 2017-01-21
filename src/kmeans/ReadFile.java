package kmeans;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;
import java.util.StringTokenizer;

public class ReadFile {

    public static float[][] readFile(int x) throws FileNotFoundException {

        int a, b;
        String path;
        if (x == 1) {
            path = "inputs/new_data_1.txt";
            a = 150;
            b = 6;
        } else if (x == 2) {
            path = "inputs/new_data_2.txt";
            a = 6;
            b = 7;
        } else if (x == 3) {
            path = "inputs/iyer.txt";
            a = 517;
            b = 14;
        } else {
            path = "inputs/cho.txt";
            a = 386;
            b = 18;
        }
        File file = new File(path);
        Scanner scan = new Scanner(file);
        float[][] data = new float[a][b];


        int row = 0, col;

        while (scan.hasNextLine()) {
            StringTokenizer st = new StringTokenizer(scan.nextLine());
            col = 0;
            while (st.hasMoreTokens()) {
                String token = st.nextToken();
                data[row][col] = Float.parseFloat(token);

                col++;
            }
            row++;
        }

        return data;
    }

}


