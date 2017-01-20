package com.mycompany.app;

import java.util.Arrays;
import java.util.Comparator;

// Basic Spark stuff
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

// For the "Word count" example
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;
import static com.mycompany.app.SerializableComparator.serialize;

// See class LineContains below
import org.apache.spark.api.java.function.Function;
import java.io.Serializable;

/**
 * Disclaimer: The goal of this example is not to shows best practices and really
 * scalable examples (for example, we don't use cache, etc). The sole purpose is
 * to present... examples.
*/

/**
* App
*/
public class App 
{
	public static void main( String[] args )
	{
		new App().Run();
	}
	
	public void Run()
	{
		// --------------------------------------------------------
		// Initialization
		System.out.println( "Hello World!" );

		SparkConf conf = new SparkConf().setAppName("firstSparkProject").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		sc.setLogLevel("WARN"); // Don't want the INFO stuff
		
		String path = "SPARKREADME.md";

		System.out.println("Trying to open: " + path);
		// Reading and replacing some special characters with " "
		JavaRDD<String> jrdd = sc.textFile(path.toString()).map(line -> org.apache.commons.lang.StringUtils.replaceEach(line, new String[] {",", "(", ")", "\"", "[", "]", "#"}, new String[] {"", " ", " ", " ", " ", " ", ""}));
		
		// --------------------------------------------------------
		// First information
		System.out.println("#lines: " + jrdd.count() );
		System.out.println("#lines having > 5 words: " + jrdd.filter(line -> line.split(" ").length > 5 ).count());
		System.out.println("#lines containing Spark: " + jrdd.filter(line -> line.contains("Spark")).count());
		System.out.println("#lines containing Spark: " + jrdd.filter(new LineContains("Spark")).count());
		
		// Number of characters in the file
		JavaRDD<Integer> jrddLength = jrdd.map(line -> line.length());
		System.out.println("#characters: " + jrddLength.reduce((x,y) -> x + y));
		
		// --------------------------------------------------------
		// Word count
		JavaRDD<String> words = jrdd.flatMap(s -> Arrays.asList(s.split(" ")).iterator());
		JavaPairRDD<String, Integer> pairs = words.mapToPair(word -> new Tuple2<String, Integer>(word, word.isEmpty() ? 0 : 1));
		JavaPairRDD<String, Integer> wordCount = pairs.reduceByKey((x,y) -> x + y);
		
		System.out.println("#words: " + words.count());
		System.out.println("#unique words: " + wordCount.count());
		
		System.out.println("Average: " + wordCount.mapToDouble(tuple -> (double)tuple._2).mean());
		
		// --------------------------------------------------------
		// Max
		// First version
		Tuple2<String, Integer> wordMax1 = wordCount.max(new ComparatorT2SI()); // See below
		System.out.println("Max: \"" + wordMax1._1 + "\" appears " + wordMax1._2 + " times");
		
		// Second version
		Tuple2<String, Integer> wordMax2 = wordCount.max(serialize((t1, t2) -> t1._2 - t2._2)); // See SerializableComparator.java
		System.out.println("Max: \"" + wordMax2._1 + "\" appears " + wordMax2._2 + " times");
		
		// Displaying the counting
		//wordCount.foreach(tuple -> System.out.println("\"" + tuple._1 + "\" appears " + tuple._2 + " times"));

		// --------------------------------------------------------
		// De-initialization
		sc.stop();
		sc.close();
		
		System.out.println("END");
	}
}

/**
 * LineContains
 * See second example for "Lines containing Spark above"
 * @author Rafael
 * Note: the class must implements Serializable
 */
class LineContains implements Function<String, Boolean>, Serializable
{
	private String query;
	
	public LineContains(String query) {
		this.query = query;
	}
	
	/**
	 * Check if the string str contains this.query
	 * @param str
	 */
	@Override
	public Boolean call(String str) {
		return str.contains(query);
	}
}

/**
 * ComparatorT2SI
 * @author Rafael
 * Note: the class must implements Serializable
 */
class ComparatorT2SI implements Comparator<Tuple2<String, Integer>>, Serializable
{
	@Override
	public int compare( Tuple2<String, Integer> t1, Tuple2<String, Integer> t2 )
	{
		return t1._2 - t2._2;
	}
}
