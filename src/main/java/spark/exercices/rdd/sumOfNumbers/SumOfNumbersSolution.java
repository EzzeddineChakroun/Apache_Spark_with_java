package spark.exercices.rdd.sumOfNumbers;


import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

public class SumOfNumbersSolution {
    public static void main(String[]args)
    {
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setAppName("SumOfNumbers").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile("in/prime_nums.text");
        JavaRDD<String> primenumbers = lines.flatMap(line->Arrays.asList(line.replaceAll("\\s+", " ").split(" ")).iterator());
        JavaRDD<String> primenumbernotempty = primenumbers.filter(number->!number.isEmpty());
        JavaRDD<Long> primenumberslong = primenumbernotempty.map(stringnumber->Long.valueOf(stringnumber));
        Long primenumberssum = primenumberslong.reduce((x, y) -> x + y);
        System.out.println("Sum of the first 100 prime numbers : "+primenumberssum);
    }
}
