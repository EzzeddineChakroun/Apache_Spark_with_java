package spark.exercices.pairRdd.mapValues;

import spark.exercices.rdd.commons.Utils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class AirportsUppercaseSolution {

    public static void main(String[] args) throws Exception {
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setAppName("AirportsUpperCase").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile("in/airports.text");
        JavaPairRDD<String,String> airportsPairs = lines.mapToPair(line->{
            String name =line.split(Utils.COMMA_DELIMITER)[1];
            String countryname = line.split(Utils.COMMA_DELIMITER)[3];
            return new Tuple2<>(name,countryname);
        });
        JavaPairRDD<String,String> airportsUpperCase=airportsPairs.mapValues(country->country.toUpperCase());
        airportsUpperCase.saveAsTextFile("out/airports_uppercase.text");
    }
}
