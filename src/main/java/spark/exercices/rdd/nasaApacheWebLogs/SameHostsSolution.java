package spark.exercices.rdd.nasaApacheWebLogs;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SameHostsSolution {

    public static void main(String[] args) throws Exception {
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setAppName("SameHost").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> nasaJulyLogs = sc.textFile("in/nasa_19950701.tsv");
        JavaRDD<String> nasaAugustLogs = sc.textFile("in/nasa_19950801.tsv");
        JavaRDD<String> nasaJulyHosts = nasaJulyLogs.map(line->line.split("\t")[0]);
        JavaRDD<String> nasaAugustHosts = nasaAugustLogs.map(line->line.split("\t")[0]);
        JavaRDD<String> nasaBothMonthsHosts= nasaJulyHosts.intersection(nasaAugustHosts);
        JavaRDD<String> nasaHostsWithoutHeaders= nasaBothMonthsHosts.filter(line->isnotheader(line));
        nasaHostsWithoutHeaders.saveAsTextFile("out/nasa_logs_same_hosts.csv");
    }

    private static Boolean isnotheader(String line) {
        return !line.startsWith("host");
    }
}
