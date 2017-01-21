package hierarchical;

import java.util.*;
import java.util.stream.Collectors;

class Cluster {

	List<List<Double>> entryRow;
	double distance;

	Cluster() {

		entryRow = new ArrayList<List<Double>>();
	}

	int getGroundTruth() {

		return this.entryRow.get(0).get(1).intValue();
	}

	void addRow(List<List<Double>> nextSet) {

		entryRow.addAll(nextSet);
	}

	Cluster(List<List<Double>> next) {

		entryRow = next;
	}

	public void printId() {

		System.out.print('[');
		for (List<Double> row : entryRow)
			System.out.print(row.get(0).intValue() + " | ");
		System.out.println(']');
	}

	public List<Double> getId() {

		return entryRow.stream().map(row -> row.get(0)).collect(Collectors.toList());

	}

	public double getMinOriginalValue() {

		double minVal = Integer.MAX_VALUE;

		for (List<Double> row : this.entryRow) {

			int val = row.get(1).intValue();
			if (val == -1)
				continue;
			minVal = Math.min(minVal, row.get(1));
		}

		return minVal;

	}

	double getDistance(Cluster next) {

		double minDist = Integer.MAX_VALUE;

		List<List<Double>> row = next.entryRow;

		for (List<Double> thisRow : this.entryRow) {
			for (List<Double> nextRow : row) {
				double tempDist = 0.0;
				for (int i = 2; i < thisRow.size(); i++) {
					double itemDist = (thisRow.get(i) - nextRow.get(i));
					tempDist += Math.pow(itemDist, 2);
				}
				minDist = Math.min(minDist, Math.sqrt(tempDist));

			}
		}

		return minDist;
	}

}
