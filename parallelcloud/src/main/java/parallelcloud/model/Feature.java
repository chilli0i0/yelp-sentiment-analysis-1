/**
 * 
 */
package parallelcloud.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * @author agasthya
 *
 */
public class Feature implements WritableComparable<Feature>{
	private double onegramScore;
	private double onegramCount;
	private double positiveWordCount;
	private double negativeWordCount;
	private double bigramCount;
	private double bigramScore;
	private String star = "-";
	
	public Feature() {
	}
	
	public Feature(double onegramScore, double onegramCount,
			double positiveWordCount, double negativeWordCount,
			double bigramCount, double bigramScore) {
		this.onegramScore = onegramScore;
		this.onegramCount = onegramCount;
		this.positiveWordCount = positiveWordCount;
		this.negativeWordCount = negativeWordCount;
		this.bigramCount = bigramCount;
		this.bigramScore = bigramScore;
	}

	/**
	 * @return the onegramScore
	 */
	public double getOnegramScore() {
		return onegramScore;
	}

	/**
	 * @param onegramScore the onegramScore to set
	 */
	public void setOnegramScore(double onegramScore) {
		this.onegramScore = onegramScore;
	}

	/**
	 * @return the onegramCount
	 */
	public double getOnegramCount() {
		return onegramCount;
	}

	/**
	 * @param onegramCount the onegramCount to set
	 */
	public void setOnegramCount(double onegramCount) {
		this.onegramCount = onegramCount;
	}

	/**
	 * @return the positiveWordCount
	 */
	public double getPositiveWordCount() {
		return positiveWordCount;
	}

	/**
	 * @param positiveWordCount the positiveWordCount to set
	 */
	public void setPositiveWordCount(double positiveWordCount) {
		this.positiveWordCount = positiveWordCount;
	}

	/**
	 * @return the negativeWordCount
	 */
	public double getNegativeWordCount() {
		return negativeWordCount;
	}

	/**
	 * @param negativeWordCount the negativeWordCount to set
	 */
	public void setNegativeWordCount(double negativeWordCount) {
		this.negativeWordCount = negativeWordCount;
	}

	/**
	 * @return the bigramCount
	 */
	public double getBigramCount() {
		return bigramCount;
	}

	/**
	 * @param bigramCount the bigramCount to set
	 */
	public void setBigramCount(double bigramCount) {
		this.bigramCount = bigramCount;
	}

	/**
	 * @return the bigramScore
	 */
	public double getBigramScore() {
		return bigramScore;
	}

	/**
	 * @param bigramScore the bigramScore to set
	 */
	public void setBigramScore(double bigramScore) {
		this.bigramScore = bigramScore;
	}

	/**
	 * @return the star
	 */
	public String getStar() {
		return star;
	}

	/**
	 * @param star the star to set
	 */
	public void setStar(String star) {
		this.star = star;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		String input = in.readUTF();
		String[] words = input.split(",");
		this.star = words[0];
		this.onegramCount = Double.valueOf(words[1]);
		this.onegramScore = Double.valueOf(words[2]);
		this.positiveWordCount = Double.valueOf(words[3]);
		this.negativeWordCount = Double.valueOf(words[4]);
		this.bigramCount = Double.valueOf(words[5]);
		this.bigramScore = Double.valueOf(words[6]);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		//out.writeChars(this.toString());
		out.writeUTF(this.toString());
	}

	@Override
	public int compareTo(Feature that) {
		if (this.star.compareTo(that.star) < 0)
			return -1;
		else if (this.star.compareTo(that.star) > 0)
			return 1;
		return 0;
	}
	
	@Override
	public String toString() {
		StringBuilder stringBuilder = new StringBuilder();
		stringBuilder.append(this.star).append(",").append(this.onegramCount)
				.append(",").append(this.onegramScore).append(",")
				.append(this.positiveWordCount).append(",")
				.append(this.negativeWordCount).append(",")
				.append(this.bigramCount).append(",").append(this.bigramScore);
		return stringBuilder.toString();
	}
	
	@Override
	public boolean equals(Object o) {
		if (o instanceof Feature) {
			Feature that = (Feature) o;
			return (this.star == that.star
					&& this.onegramCount == that.onegramCount
					&& this.onegramScore == that.onegramScore
					&& this.positiveWordCount == that.positiveWordCount
					&& this.negativeWordCount == that.negativeWordCount
					&& this.bigramCount == that.bigramCount && this.bigramScore == that.bigramScore);
		}
		return false;
	}
	
	@Override
	public int hashCode() {
		return this.hashCode();
	}
}
