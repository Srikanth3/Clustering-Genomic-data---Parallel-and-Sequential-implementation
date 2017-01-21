package hierarchical;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.*;

public class ReadFile {

	public List<Cluster> readFile(String fileName) throws FileNotFoundException {

		File file = new File("inputs/"+ fileName + ".txt");
		Scanner scan = new Scanner(file);

		List<Cluster> data = new ArrayList<Cluster>();

		while (scan.hasNextLine()) {

			StringTokenizer st = new StringTokenizer(scan.nextLine());
			List<Double> tempRow = new ArrayList<Double>();

			while (st.hasMoreTokens())
				tempRow.add(Double.parseDouble(st.nextToken()));

			if (tempRow.get(1) == -1)
				continue;

			List<List<Double>> temp = new ArrayList<List<Double>>();
			temp.add(tempRow);

			data.add(new Cluster(temp));
		}

		return data;

	}

}