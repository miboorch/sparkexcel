import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.sources.TableScan;
import org.apache.spark.sql.types.StructType;
import pojo.Column;
import utils.ExcelUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class ExcelRelation extends BaseRelation implements Serializable, TableScan {

    private transient SQLContext sqlContext;

    private StructType schema;

    private String path;

    private String rid;

    private Integer start=0;

    private Integer end=10000000;

    private Integer maxIndex;

    private List<Column> columns;

    public ExcelRelation(SQLContext sqlContext, String path,
                         StructType schema,String rid,Integer start,
                         Integer end,Integer maxIndex,List<Column> columns) {
        this.sqlContext = sqlContext;
        this.path = path;
        this.schema = schema;
        this.rid=rid;
        this.start=start;
        this.end=end;
        this.maxIndex=maxIndex;
        this.columns=columns;
    }

    @Override
    public SQLContext sqlContext() {
        return this.sqlContext;
    }

    @Override
    public StructType schema() {
        return this.schema;
    }

    @Override
    public RDD<Row> buildScan() {
        ExcelUtils excelUtils=new ExcelUtils();
        try {
            LinkedList<Row> excelData = excelUtils.getExcelData(path, schema, start, end, rid,maxIndex,columns);
            excelData.removeFirst();
            Dataset<Row> ds = sqlContext.createDataFrame(excelData, schema);
            RDD<Row> rows = ds.rdd();
            return rows;
        }catch (Exception e){
            return null;
        }
    }
}
