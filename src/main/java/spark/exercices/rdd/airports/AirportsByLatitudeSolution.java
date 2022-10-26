package spark.exercices.rdd.airports;

import com.sparkTutorial.rdd.commons.Utils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class AirportsByLatitudeSolution {

    public static void main(String[] args) throws Exception {
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setAppName("AirportsByLatitude").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile("in/airports.text");
        JavaRDD<String> largest_airports = lines.filter(line->Float.valueOf(line.split(Utils.COMMA_DELIMITER)[6])>40.0);
        JavaRDD<String> largest_airports_info = largest_airports.map(line->{
            String airport_name = line.split(Utils.COMMA_DELIMITER)[1];
            String airport_latitude=line.split(Utils.COMMA_DELIMITER)[6];
            return StringUtils.join(new String[]{airport_name,airport_latitude,","});
        });
        largest_airports_info.saveAsTextFile("out/airports_with_large_latitude.text");
    }
}
