/**
 * 
 */
package parallelcloud.preprocess;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import parallelcloud.helper.WordsReaderHelper;
import parallelcloud.model.Feature;

/**
 * @author agasthya
 * 
 */
public class FeatureExtraction {
	private Set<String> stopWords = WordsReaderHelper.getStopWords();
	private Map<String, Integer> onegramPositivityIndex = WordsReaderHelper.getOnegramPositivityIndex();
	private Map<String, Integer> bigramPositivityIndex = WordsReaderHelper.getBigramPositivityIndex();
	private Set<String> positiveWords = WordsReaderHelper.getPositiveWords();
	private Set<String> negativeWords = WordsReaderHelper.getNegativeWords();
	
	public Feature extractFeatures(String review) {
		System.out.println("Inside FeatureExtraction");
		double onegramScore = 0.0;
		double onegramCount = 0.0;
		double positiveWordCount = 0.0;
		double negativeWordCount = 0.0;
		double bigramCount = 0.0;
		double bigramScore = 0.0;
		String text = normalize(review);
		String[] words = text.split("\\s+");
		List<String> bigrams = ngrams(2, words);
		// Compute onegram score count
		for (String word : words) {
			if (!stopWords.contains(word) && onegramPositivityIndex.containsKey(word)) {
				onegramScore += onegramPositivityIndex.get(word);
				onegramCount += 1;
				if (positiveWords.contains(word)) {
					positiveWordCount += 1;
				}
				if (negativeWords.contains(word)) {
					negativeWordCount += 1;
				}
			}
		}
		
		// Compute bigram scores
		for (String bigram : bigrams) {
			if (bigramPositivityIndex.containsKey(bigram)) {
				bigramCount += 1;
				bigramScore += bigramPositivityIndex.get(bigram);
			}
		}
		
		// Avoid divide-by-zero errors
		if (onegramCount == 0) {
			onegramScore = 0;
		} else {
			onegramScore = (int)((onegramScore * 1.0) / onegramCount);
		}
		
		if (bigramCount == 0) {
			bigramScore = 0;
		} else {
			bigramScore = (int)((bigramScore * 1.0) / bigramCount);
		}
		
		return new Feature(onegramScore, onegramCount, positiveWordCount, negativeWordCount, bigramCount, bigramScore);
	}

	private List<String> ngrams(int n, String[] words) {
		List<String> ngramsList = new ArrayList<String>();
		for (int i = 0; i < words.length - n + 1; i++) {
			ngramsList.add(concat(words, i, i + n));
		}
		return ngramsList;
	}

	private String concat(String[] words, int start, int end) {
		StringBuilder sb = new StringBuilder();
		for (int i = start; i < end; i++) {
			sb.append((i > start ? " " : "") + words[i]);
		}
		return sb.toString();
	}

	private String normalize(String review) {
		// Convert to lower case and remove non-alphanumeric characters
		return review.toLowerCase().replaceAll("[^A-Za-z0-9\\s]", "");
	}
	
}
