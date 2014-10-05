package parallelcloud.classifier;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Random;

import org.apache.mahout.classifier.df.DecisionForest;
import org.apache.mahout.classifier.df.builder.DefaultTreeBuilder;
import org.apache.mahout.classifier.df.data.Data;
import org.apache.mahout.classifier.df.data.DataLoader;
import org.apache.mahout.classifier.df.data.DescriptorException;
import org.apache.mahout.classifier.df.data.Instance;
import org.apache.mahout.classifier.df.ref.SequentialBuilder;
import org.apache.mahout.common.RandomUtils;
import org.uncommons.maths.Maths;

public class RandomForestClassifier {
	private Data data = null;
	private DecisionForest forest = null;

	public void train(String filePath, int numberOfTrees) throws IOException, DescriptorException {
		// This section is to build the tree using Training data
		System.out.println("Starting training...");
		String descriptor = "L N N N N N N";
		String[] trainDataValues = fileAsStringArray(filePath);
		System.out.println("Training data read from file " + filePath);
		data = DataLoader.loadData(
				DataLoader.generateDataset(descriptor, false, trainDataValues),
				trainDataValues);
		System.out.println("Number of trees: " + numberOfTrees);
		forest = buildForest(numberOfTrees, data);
		System.out.println("Built forest");
		//saveTrees(numberOfTrees);
	}

	private void saveTrees(int numberOfTrees) throws FileNotFoundException, IOException {
		DataOutputStream dos = new DataOutputStream(new FileOutputStream("/home/hduser/trees-" + numberOfTrees + ".txt"));
		forest.write(dos);
	}

	/**
	 * @param data
	 * @param forest
	 * @throws Exception
	 */
	public int predictRating(String review) {
		System.out.println("Review:" + review);
		String[] testDataValues = new String[] { review };
		Data test = DataLoader.loadData(data.getDataset(), testDataValues);
		Random rng = RandomUtils.getRandom();
		int label = -1;
		Instance oneSample = test.get(0);
		double classify = forest.classify(test.getDataset(), rng, oneSample);
		label = data.getDataset()
				.valueOf(0, String.valueOf((int) classify));

		System.out.println("Label: " + label);
		return label;
	}

	private DecisionForest buildForest(int numberOfTrees, Data data) {
		System.out.println("Building forest...");
		int m = (int) Math
				.floor(Maths.log(2, data.getDataset().nbAttributes()) + 1);
		DefaultTreeBuilder treeBuilder = new DefaultTreeBuilder();
		treeBuilder.setM(m);
		return new SequentialBuilder(RandomUtils.getRandom(), treeBuilder,
				data.clone()).build(numberOfTrees);
	}

	private String[] fileAsStringArray(String file) throws IOException {
		ArrayList<String> list = new ArrayList<String>();
		DataInputStream in = new DataInputStream(new FileInputStream(file));
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		String strLine;
		br.readLine(); // discard top one (header)
		while ((strLine = br.readLine()) != null) {
			list.add(strLine);
		}
		in.close();
		return list.toArray(new String[list.size()]);
	}

/*	private String[] testFileAsStringArray(String file) throws Exception {
		ArrayList<String> list = new ArrayList<String>();

		DataInputStream in = new DataInputStream(new FileInputStream(file));
		BufferedReader br = new BufferedReader(new InputStreamReader(in));

		String strLine;
		br.readLine(); // discard top one (header)
		while ((strLine = br.readLine()) != null) {
			list.add("-," + strLine);
		}

		in.close();
		return list.toArray(new String[list.size()]);
	}*/
}