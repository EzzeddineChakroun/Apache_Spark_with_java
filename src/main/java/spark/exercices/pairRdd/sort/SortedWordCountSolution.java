package spark.exercices.pairRdd.sort;


import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Map;

public class SortedWordCountSolution {

    public static void main(String[] args) throws Exception {
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setAppName("SortedWordCloud").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile("in/word_count.text");
        JavaRDD<String> words = lines.flatMap(line-> Arrays.asList(line.split(" ")).iterator());
        JavaPairRDD<String,Integer> words_with_number = words.mapToPair(word->new Tuple2<>(word,1));
        JavaPairRDD<String,Integer> countbywords = words_with_number.reduceByKey((x,y)->x+y);
        JavaPairRDD<Integer,String> wordbycount = countbywords.mapToPair(tuple->new Tuple2<>(tuple._2,tuple._1));
        JavaPairRDD<Integer,String> wordbycountsorted = wordbycount.sortByKey(false);
        JavaPairRDD<String,Integer> wordcountsorted = wordbycountsorted.mapToPair(tuple->new Tuple2<>(tuple._2,tuple._1));
        for(Tuple2<String,Integer> entry:wordcountsorted.collect())
        {
            System.out.println(entry._1+" : "+entry._2);
        }

    }
}
