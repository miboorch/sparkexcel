package utils;

import common.MetadataConstants;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType$;


public class SparkDataTypeConvertion {


    public static DataType toSparkType(int type, Integer scale) {
        switch (type) {
            case MetadataConstants.DataType.STRING:
                return DataTypes.StringType;
            case MetadataConstants.DataType.DATE:
                return DataTypes.DateType;
            case MetadataConstants.DataType.TIMESTAMP:
                return DataTypes.TimestampType;
            case MetadataConstants.DataType.DECIMAL:
                if(null == scale){
                    return DataTypes.DoubleType;
                }else {
                    return DataTypes.createDecimalType(DecimalType$.MODULE$.MAX_PRECISION(), scale);
                }
        }
        return null;
    }

}
