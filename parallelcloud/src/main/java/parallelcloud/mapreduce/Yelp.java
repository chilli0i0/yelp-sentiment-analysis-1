/**
 * 
 */
package parallelcloud.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import parallelcloud.model.Feature;

/**
 * @author hduser
 *
 */
public class Yelp extends Configured implements Tool {
	private final static Logger logger = LoggerFactory.getLogger(Yelp.class);

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new Yelp(), args);
	    System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		logger.info("Starting MapReduce Job...");
	    JobConf conf = new JobConf(Yelp.class);
	    conf.setJobName("yelp");
	    logger.info("Job Name:" + conf.getJobName());
	    conf.setOutputKeyClass(Text.class);
	    conf.setOutputValueClass(NullWritable.class);
	    conf.setMapOutputKeyClass(Text.class);
	    conf.setMapOutputValueClass(Feature.class);

	    conf.setMapperClass(ReviewsMapper.class);
	    conf.setReducerClass(ReviewsReducer.class);
	    
	    conf.setJarByClass(Yelp.class);
	    //conf.setJar("target/parallelcloud-0.0.1-SNAPSHOT-jar-with-dependencies.jar");
	    
	    conf.setInputFormat(TextInputFormat.class);
	    conf.setOutputFormat(TextOutputFormat.class);
	    
	    conf.addResource(new Path("/usr/local/hadoop/etc/hadoop/core-site.xml"));
	    conf.addResource(new Path("/usr/local/hadoop/etc/hadoop/hdfs-site.xml"));
	    conf.set("mapreduce.job.tracker", "master:54311");
	    conf.set("mapreduce.framework.name", "yarn");
	    conf.set("yarn.resourcemanager.address", "master:8032");
	    
	    FileInputFormat.addInputPath(conf, new Path(args[1]));
	    FileOutputFormat.setOutputPath(conf, new Path(args[2]));
	    
	    JobClient.runJob(conf);
	    logger.info("MapReduce Job Done!");
	    FileSystem hdfs = FileSystem.get(conf);
	    Path hdfsOutput = new Path(args[2] + "/part-00000");
	    Path localFile = new Path("/home/hduser/output.csv");
	    logger.info("Copying output file from " + hdfsOutput.toString() + " to local directory " + localFile.toString());
	    hdfs.copyToLocalFile(hdfsOutput, localFile);
	    logger.info("Copy to local directory done");
	    logger.info("MapReduce Job Done!");
	    return 0;
	  }

}
