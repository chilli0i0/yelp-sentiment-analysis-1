/**
 * 
 */
package parallelcloud.mapreduce;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import parallelcloud.model.Feature;

/**
 * @author agasthya
 *
 */
public class ReviewsReducer extends MapReduceBase implements Reducer<Text, Feature, Feature, NullWritable> {
	private final static Logger logger = LoggerFactory.getLogger(ReviewsReducer.class);
	
	@Override
	public void reduce(Text key, Iterator<Feature> values,
			OutputCollector<Feature, NullWritable> output, Reporter reporter)
			throws IOException {
		logger.debug("Inside reducer");
		Feature feature = null;
		while(values.hasNext()) {
			feature = values.next();
		}
		output.collect(feature, NullWritable.get());
		logger.debug("Feature written to reducer OutputCollector");
	}

}
