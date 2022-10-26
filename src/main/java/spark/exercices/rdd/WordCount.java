package spark.exercices.rdd;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.Map;


public class WordCount {
    public static void main(String[] args) throws Exception
    {
        // Count Words in the file in/word_count.text and show them in stdout
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setAppName("number_word_Airports").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile("in/word_count.text");
        JavaRDD<String> words = lines.flatMap(line->Arrays.asList(line.split(" ")).iterator());
        Map<String,Long> words_count = words.countByValue();
        for (Map.Entry<String,Long> word_count :words_count.entrySet())
        {
            System.out.println(word_count.getKey()+" : "+word_count.getValue());
        }
    }

}
