package spark.exercices.pairRdd.groupbykey;


import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import spark.exercices.rdd.commons.Utils;

import java.util.Map;

public class AirportsByCountrySolution {
    public static void main(String[]args)
    {
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setAppName("AiportsByCountry").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile("in/airports.text");
        JavaPairRDD<String,String> aiportscountry = lines.mapToPair(line->new Tuple2<>(line.split(Utils.COMMA_DELIMITER)[3],line.split(Utils.COMMA_DELIMITER)[1]));
        JavaPairRDD<String, Iterable<String>> groupaiportsbycountry = aiportscountry.groupByKey();
        Map<String,Iterable<String>> airportsbycountry = groupaiportsbycountry.collectAsMap();
        for(Map.Entry<String,Iterable<String>> entry:airportsbycountry.entrySet())
        {
            System.out.println(entry.getKey()+" : "+entry.getValue());
        }
    }
}
