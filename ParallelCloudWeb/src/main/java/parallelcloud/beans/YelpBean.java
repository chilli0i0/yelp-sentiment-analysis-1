/**
 * 
 */
package parallelcloud.beans;

import java.io.File;
import java.io.IOException;

import javax.faces.application.FacesMessage;
import javax.faces.bean.ApplicationScoped;
import javax.faces.bean.ManagedBean;
import javax.faces.context.FacesContext;

import org.apache.mahout.classifier.df.data.DescriptorException;

import parallelcloud.classifier.RandomForestClassifier;
import parallelcloud.preprocess.FeatureExtraction;

/**
 * @author hduser
 * 
 */
@ManagedBean
@ApplicationScoped
public class YelpBean {
	private boolean preprocessed = checkIfMROutputExists();
	private boolean trained = false;
	private String review;
	private Integer rating = 0;
	private RandomForestClassifier classifier;
	private int numberOfTrees;
	private static String TRAINING_FILE_PATH = "/home/hduser/output.csv";
	private FeatureExtraction featureExtraction = new FeatureExtraction();

	public void preprocess() {
		String path = "target/parallelcloud-0.0.1-SNAPSHOT-jar-with-dependencies.jar ";
		String inputFile = "/user/hduser/cloud/input/ ";
		String outputFile = "/user/hduser/cloud/output ";
		String mainClass = "parallelcloud/mapreduce/Yelp ";
		String command = "/usr/local/hadoop/hadoop-2.3.0/bin/hadoop jar "
				+ path + mainClass + inputFile + outputFile;
		System.out.println(command);
		Process process;
		try {
			// Ugly and error-prone
			process = Runtime.getRuntime().exec(command);
			/*if (process.exitValue() != 0) {
				generateErrorMessage("Hadoop Job Error.");
			}*/
		} catch (IOException e) {
			generateErrorMessage("Exception when running Hadoop job.");
		}
		preprocessed = checkIfMROutputExists();
	}

	private boolean checkIfMROutputExists() {
		File file = new File(TRAINING_FILE_PATH);
		if (file.exists() && !file.isDirectory()) {
			return true;
		}
		return false;
	}

	private void generateErrorMessage(String message) {
		FacesContext.getCurrentInstance()
				.addMessage(null,new FacesMessage(FacesMessage.SEVERITY_ERROR, "Fatal!",message));
	}

	public void trainReviews() {
		classifier = new RandomForestClassifier();
		try {
			classifier.train(TRAINING_FILE_PATH, numberOfTrees);
		} catch (IOException | DescriptorException e) {
			generateErrorMessage("Mahout Error.");
		}
		trained = true;
	}

	public void predictRating() {
		String features = featureExtraction.extractFeatures(review).toString();
		try {
		rating = Integer.valueOf(classifier.predictRating(features));
		} catch(ArrayIndexOutOfBoundsException ex) {
			generateErrorMessage("Error in prediction");
		} catch (NullPointerException ex) {
			generateErrorMessage("No prediction returned from classifier");
		} catch (NumberFormatException ex) {
			generateErrorMessage("Error in prediction");
		}
	}

	public boolean isPreprocessed() {
		return preprocessed;
	}

	public void setPreprocessed(boolean preprocessed) {
		this.preprocessed = preprocessed;
	}

	public String getReview() {
		return review;
	}

	public void setReview(String review) {
		this.review = review;
	}

	public Integer getRating() {
		return rating;
	}

	public void setRating(Integer rating) {
		this.rating = rating;
	}

	public boolean isTrained() {
		return trained;
	}

	public void setTrained(boolean trained) {
		this.trained = trained;
	}

	public int getNumberOfTrees() {
		return numberOfTrees;
	}

	public void setNumberOfTrees(int numberOfTrees) {
		this.numberOfTrees = numberOfTrees;
	}

}
