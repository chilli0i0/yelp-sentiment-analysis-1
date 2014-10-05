/**
 * 
 */
package parallelcloud.preprocess;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

/**
 * @author Rajath
 *
 */
public class ExtractReviews {
	public static void main(String[] args) {
		JSONParser parser = new JSONParser();
		int count = 0;
		int total = 0;
		try {
			BufferedReader br = new BufferedReader(new FileReader("C:/Rajath/yelp_dataset_challenge_academic_dataset"));
			String line = br.readLine();
			while ((line = br.readLine()) != null) {
				if (line.contains("yelp_academic_dataset_review.json")) {
					BufferedWriter bw = new BufferedWriter(new FileWriter(new File("C:/Rajath/yelp_reviews.txt")));
					while ((line = br.readLine()) != null) {
						if (line.contains("yelp_academic_dataset")) {
							System.out.println(line);
							break;
						}
						Object o = parser.parse(line);
						JSONObject yelp = (JSONObject) o;
							if (yelp.get("type").equals("review")) {
								bw.write(line);
								bw.newLine();
								count++;
								total++;
							}
					}
					bw.close();
				} else {
					total++;
				}
			}
				br.close();
			System.out.println("Number of reviews:" + count);
			System.out.println("Total number of objects:" + (total - 3));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (org.json.simple.parser.ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	}
}
