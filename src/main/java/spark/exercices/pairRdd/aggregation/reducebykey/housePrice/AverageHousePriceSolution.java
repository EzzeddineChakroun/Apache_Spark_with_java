package spark.exercices.pairRdd.aggregation.reducebykey.housePrice;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Map;

public class AverageHousePriceSolution {

    public static void main(String[] args) throws Exception {
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setAppName("AverageHousePrice").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile("in/RealEstate.csv");
        JavaRDD<String> lineswithoutheaders = lines.filter(line->!line.startsWith("MLS,Location,Price,Bedrooms,Bathrooms,Size,Price SQ Ft,Status"));
        JavaPairRDD<String,Float> nbbedromsprices = lineswithoutheaders.mapToPair(line-> {
            String numberofbedrooms = line.split(",")[3];
            Float price = Float.valueOf(line.split(",")[2]);
            return new Tuple2<>(numberofbedrooms, price);
        }
        );
        JavaPairRDD<String,Tuple2<Float,Integer>> pricepernbbedroms = nbbedromsprices.mapValues(price->new Tuple2<>(price,1));
        JavaPairRDD<String,Tuple2<Float,Integer>> Totalpricecountpernbbedroms = pricepernbbedroms.reduceByKey((tuple1,tuple2)->new Tuple2<>(tuple1._1+ tuple2._1,tuple1._2+ tuple2._2));
        JavaPairRDD<String,Float> avgpricepernbbedroms = Totalpricecountpernbbedroms.mapValues(tuple->tuple._1/ tuple._2);
        Map<String,Float> avgpriceperbedrom = avgpricepernbbedroms.collectAsMap();
        for(Map.Entry<String,Float> entry:avgpriceperbedrom.entrySet())
        {
            System.out.println(entry.getKey()+", "+ entry.getValue());
        }
    }
}
