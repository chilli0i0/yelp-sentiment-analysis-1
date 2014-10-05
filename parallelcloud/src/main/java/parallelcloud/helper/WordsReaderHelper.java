/**
 * 
 */
package parallelcloud.helper;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author Rajath
 *
 */
public class WordsReaderHelper {
	private static Set<String> stopWords;
	private static Map<String, Integer> onegramPositivityIndex;
	private static Map<String, Integer> bigramPositivityIndex;
	private static Set<String> positiveWords;
	private static Set<String> negativeWords;
	private static String STOPWORDS_FILENAME = "/home/hduser/data/Data/stopwords.txt";
	private static String ONEGRAM_POSITIVITY_INDEX_FILENAME = "/home/hduser/data/Data/onegrampositivityindex.txt";
	private static String BIGRAM_POSITIVITY_INDEX_FILENAME = "/home/hduser/data/Data/bigrampositivityindex.txt";
	private static String POSITIVE_WORDS = "/home/hduser/data/Data/positivewords.txt";
	private static String NEGATIVE_WORDS = "/home/hduser/data/Data/negativewords.txt";
	
	static {
		readStopWords();
		readPositiveWords();
		readNegativeWords();
		readOnegramPositivityIndex();
		readBigramPositivityIndex();
	}
	
	public static void readStopWords() {
		stopWords = new HashSet<String>();
		try {
			BufferedReader reader = new BufferedReader(new FileReader(STOPWORDS_FILENAME));
			String line = null;
			while((line = reader.readLine()) != null) {
				String text = line.toLowerCase().replaceAll("['\\s+]", "");
				String wordsInLine[] = text.split(",");
				for (int i = 0; i < wordsInLine.length; i++) {
					stopWords.add(wordsInLine[i]);
				}
			}
			reader.close();
		} catch (FileNotFoundException e) {
			// TODO: Use logger
			System.err.println("FileNotFoundException in readStop():" + e.toString());
		} catch (IOException e) {
			System.err.println("IOException in readStop():" + e.toString());
		}
	}
	
	public static void readOnegramPositivityIndex() {
		onegramPositivityIndex = new HashMap<String, Integer>();
		try {
			BufferedReader reader = new BufferedReader(new FileReader(ONEGRAM_POSITIVITY_INDEX_FILENAME));
			String line = null;
			while((line = reader.readLine()) != null) {
				String text = line.toLowerCase().replaceAll("[\"{}\\s+]", "");
				String[] splitWords = text.split(",");
				for (String word: splitWords) {
					String[] wordPositivity = word.split(":");
					if (wordPositivity.length == 2) {
						onegramPositivityIndex.put(wordPositivity[0], Integer.valueOf(wordPositivity[1]));
					}
				}
			}
			reader.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (NumberFormatException e) {
			System.err.println("NumberFormatException in readOnegramPositivityIndex():" + e.toString());
		}
		
	}
	
	public static void readBigramPositivityIndex() {
		bigramPositivityIndex = new HashMap<String, Integer>();
		try {
			BufferedReader reader = new BufferedReader(new FileReader(BIGRAM_POSITIVITY_INDEX_FILENAME));
			String line = null;
			while((line = reader.readLine()) != null) {
				String text = line.toLowerCase().replaceAll("['{}\\s+]", "");
				String splitWords[] = text.split(",\\(");
				for (String word : splitWords) {
					String[] bigramPositivity = word.replaceAll("[()]", "").split(":");
					if (bigramPositivity.length == 2) {
						String bigram = bigramPositivity[0];
						String value = bigramPositivity[1];
						String[] bigramArray = bigram.split(",");
						StringBuilder stringBuilder = new StringBuilder();
						stringBuilder.append(bigramArray[0]).append(" ").append(bigramArray[1]);
						bigramPositivityIndex.put(stringBuilder.toString(), Integer.valueOf(value));
					}
				}
			}
			reader.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void readPositiveWords() {
		positiveWords = new HashSet<String>();
		try {
			BufferedReader reader = new BufferedReader(new FileReader(POSITIVE_WORDS));
			String line = null;
			while((line = reader.readLine()) != null) {
				String text = line.toLowerCase().replaceAll("['\\s+]", "");
				String splitWords[] = text.split(",");
				positiveWords.addAll(Arrays.asList(splitWords));
			}
			reader.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void readNegativeWords() {
		negativeWords = new HashSet<String>();
		try {
			BufferedReader reader = new BufferedReader(new FileReader(NEGATIVE_WORDS));
			String line = null;
			while((line = reader.readLine()) != null) {
				String text = line.toLowerCase().replaceAll("['\\s+]", "");
				String splitWords[] = text.split(",");
				negativeWords.addAll(Arrays.asList(splitWords));
			}
			reader.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * @return the stopWords
	 */
	public static Set<String> getStopWords() {
		return stopWords;
	}

	/**
	 * @param stopWords the stopWords to set
	 */
	public static void setStopWords(Set<String> stopWords) {
		WordsReaderHelper.stopWords = stopWords;
	}

	/**
	 * @return the onegramPositivityIndex
	 */
	public static Map<String, Integer> getOnegramPositivityIndex() {
		return onegramPositivityIndex;
	}

	/**
	 * @param onegramPositivityIndex the onegramPositivityIndex to set
	 */
	public static void setOnegramPositivityIndex(
			Map<String, Integer> onegramPositivityIndex) {
		WordsReaderHelper.onegramPositivityIndex = onegramPositivityIndex;
	}

	/**
	 * @return the bigramPositivityIndex
	 */
	public static Map<String, Integer> getBigramPositivityIndex() {
		return bigramPositivityIndex;
	}

	/**
	 * @param bigramPositivityIndex the bigramPositivityIndex to set
	 */
	public static void setBigramPositivityIndex(Map<String, Integer> bigramPositivityIndex) {
		WordsReaderHelper.bigramPositivityIndex = bigramPositivityIndex;
	}

	/**
	 * @return the positiveWords
	 */
	public static Set<String> getPositiveWords() {
		return positiveWords;
	}

	/**
	 * @param positiveWords the positiveWords to set
	 */
	public static void setPositiveWords(Set<String> positiveWords) {
		WordsReaderHelper.positiveWords = positiveWords;
	}

	/**
	 * @return the negativeWords
	 */
	public static Set<String> getNegativeWords() {
		return negativeWords;
	}

	/**
	 * @param negativeWords the negativeWords to set
	 */
	public static void setNegativeWords(Set<String> negativeWords) {
		WordsReaderHelper.negativeWords = negativeWords;
	}

}
