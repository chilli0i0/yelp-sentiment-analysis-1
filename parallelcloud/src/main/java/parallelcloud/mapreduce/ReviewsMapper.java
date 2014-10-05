/**
 * 
 */
package parallelcloud.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import parallelcloud.model.Feature;
import parallelcloud.preprocess.FeatureExtraction;

/**
 * @author agasthya
 *
 */
public class ReviewsMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Feature>{
	private final static Logger logger = LoggerFactory.getLogger(ReviewsMapper.class);
	private final static FeatureExtraction featureExtraction = new FeatureExtraction();
	private final static JSONParser parser = new JSONParser();
	private Text word = new Text();

	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<Text, Feature> output, Reporter reporter)
			throws IOException {
		try {
			JSONObject jsonObject = (JSONObject) parser.parse(value.toString());
			if (jsonObject.get("type").equals("review")) {
				String reviewID = (String) jsonObject.get("review_id");
				String reviewText = (String) jsonObject.get("text");
				long stars = (Long) jsonObject.get("stars");
				Feature feature = featureExtraction.extractFeatures(reviewText);
				feature.setStar(String.valueOf(stars));
				word.set(reviewID);
				logger.debug("Feature:" + feature.toString() + " writing to mapper OutputCollector");
				output.collect(word, feature);
				logger.debug("Feature written");
			} else {
				// TODO: Throw exception; How does MapReduce handle exceptions?
			}
		} catch (ParseException e) {
			e.printStackTrace();
		}
	}

}
