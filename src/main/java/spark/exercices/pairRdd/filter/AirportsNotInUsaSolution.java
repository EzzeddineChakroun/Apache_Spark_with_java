package spark.exercices.pairRdd.filter;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import spark.exercices.rdd.commons.Utils;


public class AirportsNotInUsaSolution {

    public static void main(String[] args) throws Exception {
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setAppName("AirportsnotinUSA").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile("in/airports.text");

        JavaPairRDD<String,String> airportsinfopairs=lines.mapToPair(line->
        {
            String airportname= line.split(Utils.COMMA_DELIMITER)[1];
            String airportcountry= line.split(Utils.COMMA_DELIMITER)[3];
            return new Tuple2<>(airportname,airportcountry);
        }
        );
        JavaPairRDD<String,String> airportsnotinusa = airportsinfopairs.filter(element->!element._2.equals("\"United States\""));
        airportsnotinusa.saveAsTextFile("out/airports_not_in_usa_pair_rdd.text");
    }
}
