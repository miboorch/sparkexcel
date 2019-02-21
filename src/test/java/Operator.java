import org.apache.spark.SparkConf;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Operator {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("test").setMaster("local[*]");
        SparkSession session = SparkSession.builder().config(conf).getOrCreate();
        DataFrameReader dfr = session.read()
                .option("rid",1)
                .option("start",0)
                .option("end",100)
                .format("ExcelSource");
        Dataset<Row> data =  dfr.load("C:\\Users\\SDATA11\\Desktop\\测试1.xlsx");
        data.show();
    }
}
