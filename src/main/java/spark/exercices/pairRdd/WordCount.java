package spark.exercices.pairRdd;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Map;

public class WordCount {
    public static void main(String[]args)
    {
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setAppName("WordCountUsingPairs").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile("in/word_count.text");
        JavaRDD<String> words = lines.flatMap(line-> Arrays.asList(line.split(" ")).iterator());
        JavaPairRDD<String,Integer> wordspairs = words.mapToPair(word->new Tuple2<>(word,1));
        JavaPairRDD<String,Integer> wordcounts = wordspairs.reduceByKey((x,y)->x+y);
        Map<String,Integer> result = wordcounts.collectAsMap();
        for (Map.Entry<String,Integer> entry:result.entrySet())
        {
            System.out.println(entry.getKey()+" : "+entry.getValue());
        }
    }
}
