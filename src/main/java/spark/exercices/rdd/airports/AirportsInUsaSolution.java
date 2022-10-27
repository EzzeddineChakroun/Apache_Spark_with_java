package spark.exercices.rdd.airports;

import spark.exercices.rdd.commons.Utils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class AirportsInUsaSolution {

    public static void main(String[] args) throws Exception {
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf sparkConf = new SparkConf().setAppName("AirportsInUsa").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        JavaRDD<String> airports = sc.textFile("in/airports.text");
        JavaRDD<String> airports_USA = airports.filter(line -> line.split(Utils.COMMA_DELIMITER)[3].equals("\"United States\""));
        JavaRDD<String> airports_USA_infos = airports_USA.map(
                line -> {
                    String airports_name = line.split(Utils.COMMA_DELIMITER)[1];
                    String city_name = line.split(Utils.COMMA_DELIMITER)[2];
                    return StringUtils.join(new String[]{airports_name, city_name, ","});
                }
        );
        airports_USA_infos.saveAsTextFile("out/airports_in_usa.text");

    }
}
