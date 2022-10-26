package spark.exercices.rdd.nasaApacheWebLogs;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class UnionLogsSolution {

    public static void main(String[] args) throws Exception {
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setAppName("NasaLogsUnion").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> nasaJulyLogs = sc.textFile("in/nasa_19950701.tsv");
        JavaRDD<String> nasaAugustLogs = sc.textFile("in/nasa_19950801.tsv");
        JavaRDD<String> nasalogs=nasaJulyLogs.union(nasaAugustLogs);
        JavaRDD<String> nasalogs_without_headers =nasalogs.filter(line->isnotheader(line));
        JavaRDD<String> nasalogs_sample=nasalogs_without_headers.sample(false, 0.1);
        nasalogs_sample.saveAsTextFile("out/sample_nasa_logs.tsv");
    }

    private static Boolean isnotheader(String line) {
        return !line.startsWith("host	logname	time	method	url	response	bytes");
    }
}
