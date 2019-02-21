import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.sources.RelationProvider;
import org.apache.spark.sql.sources.SchemaRelationProvider;
import org.apache.spark.sql.types.StructType;
import pojo.SheetDetail;
import scala.collection.immutable.Map;
import utils.ExcelUtils;

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
        try {
            return new ExcelRelation(sqlContext,path,null,rid,start,end);
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
        return new ExcelRelation(sqlContext, path, structType,rid,start,end);
    }

    @Override
    public String shortName() {
        return "excel";
    }
}
