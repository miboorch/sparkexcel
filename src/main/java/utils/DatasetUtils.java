package utils;

import common.MetadataConstants;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import pojo.Column;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class DatasetUtils {

    private static Pattern pString = Pattern.compile("[`~!@#$%^&*()+=|{}':;',\\[\\].<>/?~！@#￥%……&*（）——+|{}【】‘；：”“’。，、？\\s*]");

    private static Pattern pNumeric = Pattern.compile("-?\\d+(\\.\\d+)?");

    private static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    /**
     * 将每行数据line Object[]转换为Spark Row
     */
    public static Row convertLineToRow(Object[] line, List<Column> columns) {
        for (int i = 0; i < line.length; i++) {
            // 获取列字段数据类型
            int dataType = columns.get(i).getCol_datatype();
            // 将原始数据中的每一行中的每一列数据根据不同数据类型修改
            switch (dataType) {
                case MetadataConstants.DataType.STRING:
                    if (null == line[i]) {
                        line[i] = null;
                    } else {
                        line[i] = String.valueOf(line[i]).trim();
                    }
                    break;
                case MetadataConstants.DataType.DATE:
                    if (!(line[i] instanceof Date)){
                        if (null != line[i] && !String.valueOf(line[i]).trim().equals("")) {
                            line[i] = new Date(Long.valueOf(String.valueOf(line[i]).trim()));
                        } else {
                            line[i] = null;
                        }
                    }
                    break;
                case MetadataConstants.DataType.TIMESTAMP:
                    if (!(line[i] instanceof Timestamp)){
                        if (null != line[i] && !String.valueOf(line[i]).trim().equals("")) {
                            line[i] = new Timestamp(Long.valueOf(String.valueOf(line[i]).trim()));
                        } else {
                            line[i] = null;
                        }
                    }
                    break;
                case MetadataConstants.DataType.DECIMAL:
                    if (null != line[i] && !String.valueOf(line[i]).trim().equals("")) {
                        Integer scale = columns.get(i).getScale();
                        if(null == scale){
                            line[i] = new Double(String.valueOf(line[i]).trim());
                        }else{
                            line[i] = new BigDecimal(String.valueOf(line[i]).trim());
                        }
                    } else {
                        line[i] = null;
                    }
                    break;
                default:
            }
        }
        return RowFactory.create(line);
    }


    /**
     * 根据数据集前五行生成schema
     *
     * @param rows
     * @return
     */
    public static LinkedList<Column> buildColumns(List<Row> rows, int rowLength) {
        LinkedList<Column> cols = new LinkedList<>();
        Row first = rows.get(0);
        Map<String, Integer> map = new HashMap<>();
        for (int i = 0; i < rowLength; i++) {
            Object o = first.get(i);
            if (null != o) {
                String col_name = o.toString();
                if (map.containsKey(col_name)) {
                    col_name = col_name + i;
                } else {
                    Matcher matcher = pString.matcher(col_name);
                    if (matcher.find()) {
                        col_name = matcher.replaceAll("").trim();
                    }
                }
                map.put(col_name, i);
                cols.add(new Column(col_name, i, MetadataConstants.DataType.DECIMAL));
            } else {
                cols.add(new Column("_c" + i, i, MetadataConstants.DataType.DECIMAL));
            }
        }
        int index = 0;
        for (Row row : rows) {
            if (0 == index) {
                index++;
                continue;
            }
            for (int i = 0; i < rowLength; i++) {
                if (null == row.get(i)) {
                    cols.get(i).setCol_datatype(MetadataConstants.DataType.STRING);
                } else {
                    dataTypePattern(row.get(i).toString(), cols.get(i));
                }
            }
        }
        return cols;
    }

    /**
     * 字符串类型判断
     *
     * @param text
     * @param column
     */
    private static void dataTypePattern(String text, Column column) {
        try {
            switch (column.getCol_datatype()) {
                case MetadataConstants.DataType.DECIMAL:
                    if (pNumeric.matcher(text).matches()) {
                        break;
                    } else {
                        dateFormat.parse(text);
                        column.setCol_datatype(MetadataConstants.DataType.TIMESTAMP);
                    }
                    break;
                case MetadataConstants.DataType.TIMESTAMP:
                    dateFormat.parse(text);
                    break;
                case MetadataConstants.DataType.STRING:
                default:
                    break;
            }
        } catch (Exception e) {
            column.setCol_datatype(MetadataConstants.DataType.STRING);
        }
    }
}

