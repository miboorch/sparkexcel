import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.sources.RelationProvider;
import org.apache.spark.sql.sources.SchemaRelationProvider;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructType;
import pojo.Column;
import pojo.SheetDetail;
import scala.collection.immutable.Map;
import utils.DatasetUtils;
import utils.ExcelUtils;
import utils.SparkDataTypeConvertion;

import java.util.LinkedList;
import java.util.List;

public class ExcelSource implements RelationProvider, SchemaRelationProvider, DataSourceRegister {

    /**
     * For Reading
     * @param sqlContext
     * @param map
     * @return
     */
    @Override
    public BaseRelation createRelation(SQLContext sqlContext, Map<String, String> map) {
        String path = map.get("path").get();
        String rid=map.get("rid").get();
        Integer start=Integer.valueOf(map.get("start").get());
        Integer end =Integer.valueOf(map.get("end").get());
        ExcelUtils excelUtils=new ExcelUtils();
        try {
            SheetDetail sheetDetail = excelUtils.previewExcelData(path, rid);
            List<Column> columns = sheetDetail.getColumns();
            StructType schema=new StructType();
            for (Column col : columns) {
                schema=schema.add(col.getCol_name(),
                        SparkDataTypeConvertion.toSparkType(col.getCol_datatype(), col.getScale()), true, Metadata.empty());
            }
            int maxIndex = sheetDetail.getMaxIndex();
            return new ExcelRelation(sqlContext,path,schema,rid,start,end,maxIndex,columns);
        }catch (Exception e){
            return null;
        }
    }

    /**
     * For Reading
     *
     * @param sqlContext
     * @param map
     * @param structType
     * @return
     */
    @Override
    public BaseRelation createRelation(SQLContext sqlContext, Map<String, String> map, StructType structType) {
        String path = map.get("path").get();
        String rid=map.get("rid").get();
        Integer start=Integer.valueOf(map.get("start").get());
        Integer end =Integer.valueOf(map.get("end").get());
        Integer maxIndex=null;
        if (map.contains("maxIndex")){
            maxIndex=Integer.valueOf(map.get("maxIndex").get());
        }
        return new ExcelRelation(sqlContext, path, structType,rid,start,end,maxIndex,null);
    }

    @Override
    public String shortName() {
        return "excel";
    }
}
