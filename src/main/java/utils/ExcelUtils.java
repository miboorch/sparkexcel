package utils;

import org.apache.commons.collections.CollectionUtils;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.xssf.eventusermodel.XSSFReader;
import org.apache.poi.xssf.model.SharedStringsTable;
import org.apache.poi.xssf.model.StylesTable;
import org.apache.poi.xssf.usermodel.XSSFCellStyle;
import org.apache.poi.xssf.usermodel.XSSFRichTextString;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.*;
import org.xml.sax.helpers.DefaultHandler;
import org.xml.sax.helpers.XMLReaderFactory;
import pojo.Column;
import pojo.SheetDetail;

import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Pattern;

public class ExcelUtils {

    private final static Logger log = LoggerFactory.getLogger(ExcelUtils.class);

    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    // 数组初始大小
    private int arrayLength = 20;

    // 最大列数
    private int maxColIndex = 0;

    private LinkedList<Row> data;

    private List<Column> columns;

    private boolean notPreviewFlag=false;

    public LinkedList<Row> getExcelData(String filePath, StructType schema, Integer start, Integer end, String rid, Integer maxIndex, List<Column> columns)throws Exception{
        this.data = new LinkedList<>();
        this.notPreviewFlag=true;
        OPCPackage pkg =OPCPackage.open(filePath);
        XSSFReader r = new XSSFReader(pkg);
        StylesTable stylesTable = r.getStylesTable();
        SharedStringsTable sst = r.getSharedStringsTable();
        XMLReader parser = fetchSheetParser(sst, stylesTable, start, end);
        Iterator<InputStream> it = r.getSheetsData();
        int i = 1;
        while (it.hasNext()) {
            InputStream sheet = it.next();
            if (Integer.valueOf(rid) == i) {
                this.arrayLength=maxIndex;
                this.columns=columns;
                InputSource sheetSource = new InputSource(sheet);
                parser.parse(sheetSource);
                sheet.close();
                return data;
            } else {
                sheet.close();
            }
            i++;
        }
        return null;
    }

    public SheetDetail previewExcelData(String filePath, String rid)throws Exception{
        this.data = new LinkedList<>();
        OPCPackage pkg =OPCPackage.open(filePath);
        XSSFReader r = new XSSFReader(pkg);
        StylesTable stylesTable = r.getStylesTable();
        SharedStringsTable sst = r.getSharedStringsTable();
        XMLReader parser = fetchSheetParser(sst, stylesTable, 0, 6);
        Iterator<InputStream> it = r.getSheetsData();
        int i = 1;
        while (it.hasNext()) {
            InputStream sheet = it.next();
            if (Integer.valueOf(rid) == i) {
                SheetDetail sheetDetail=new SheetDetail();
                InputSource sheetSource = new InputSource(sheet);
                parser.parse(sheetSource);
                sheetDetail.setColumns(DatasetUtils.buildColumns(data, maxColIndex));
                sheetDetail.setMaxIndex(maxColIndex);
                sheet.close();
                return sheetDetail;
            } else {
                sheet.close();
            }
            i++;
        }
        return null;
    }

    /**
     * 设置自定义的handler
     */
    private XMLReader fetchSheetParser(SharedStringsTable sst, StylesTable stylesTable, Integer startRow,
        Integer endRow) throws SAXException {
        XMLReader parser = XMLReaderFactory.createXMLReader("org.apache.xerces.parsers.SAXParser");
        ContentHandler handler = new PagingHandler(sst, stylesTable, startRow, endRow);
        parser.setContentHandler(handler);
        return parser;
    }

    /**
     * sheet中每个单元格都会走一遍 顺序为startElement -> characters -> endElement 当为最后一个单元格时走endDocument
     */
    private class PagingHandler extends DefaultHandler {

        private final static String CELL_FLAG = "c";

        private final static String CELL_TYPE_FLAG = "t";

        private final static String CELL_COORDINATE_FLAG = "r";

        private final static String CELL_VALUE_FLAG = "v";

        private final Pattern FIRST_COL_REGEX = Pattern.compile("^A[0-9]+$");

        private final Pattern CELL_DATE_TYPE_REGEX = Pattern.compile("^[-\\+]?[\\d]*$");

        private SharedStringsTable sst;

        private StylesTable stylesTable;

        // 设定的起始行
        private Integer startRow;

        // 设定的结束行
        private Integer endRow;

        // 当前行号
        private int currentRow = 0;

        // 记录当前单元格内容，字符串格式可能记录的是单元格位置
        private String currentContents;

        //当前单元格类型是否是文本
        private boolean currentIsString;

        //当前单元格类型是否是日期
        private boolean currentIsDate;

        //单元格格式对应的标号，用于获取日期类型
        private int style;

        //列的标号 A-->1
        private int colIndex;

        //保存该行的数据
        private Object[] line;

        private PagingHandler(SharedStringsTable sst, StylesTable stylesTable) {
            this.sst = sst;
            this.stylesTable = stylesTable;
            this.currentRow = 0;
        }

        private PagingHandler(SharedStringsTable sst, StylesTable stylesTable, int startRow, int endRow) {
            this.sst = sst;
            this.stylesTable = stylesTable;
            this.startRow = startRow;
            this.endRow = endRow;
            this.currentRow = 0;
        }

        @Override
        public void startElement(String uri, String localName, String name, Attributes attributes) throws SAXException {
            // 此处c为cell的意思
            if (CELL_FLAG.equals(name)) {
                // r为cellRef 获取的是单元格的坐标，如B13，A3之类的
                String cellCoordinate = attributes.getValue(CELL_COORDINATE_FLAG);
                this.colIndex = excelColStrToNum(cellCoordinate) - 1;
                if (currentRow <= 6) {
                    maxColIndex = Math.max(this.colIndex + 1, maxColIndex);
                }
                // 判断是否是新的一行
                if (FIRST_COL_REGEX.matcher(cellCoordinate).find()) {
                    if (null != line && isInRowRange() && line.length != 0) {
                        for (int i = 0; i < line.length; i++) {
                            if (line[i] == null) {
                                line[i] = "";
                            }
                        }
                        try {
                            if (notPreviewFlag && this.currentRow != 1) {
                                // 若不存在asset对象则不需要指定
                                Row row = DatasetUtils.convertLineToRow(line, columns);
                                data.add(row);
                            } else {
                                // 第一行以及预览时，直接create row
                                data.add(RowFactory.create(line));
                            }
                        } catch (Exception e) {
                            log.error(e.getMessage(), e);
                        }
                    }
                    line = new Object[arrayLength];
                    this.currentRow++;
                }
                if (isInRowRange()) {
                    // 获取单元格类型
                    String cellType = attributes.getValue(CELL_TYPE_FLAG);
                    if (cellType != null && cellType.equals("s")) {
                        this.currentIsString = true;
                    } else {
                        cellType = attributes.getValue("s");
                        this.currentIsString = false;
                    }
                    if (cellType != null && CELL_DATE_TYPE_REGEX.matcher(cellType).matches()) {
                        this.style = Integer.parseInt(cellType);
                        this.currentIsDate = true;
                    } else {
                        this.currentIsDate = false;
                    }
                }
            }
            this.currentContents = "";
        }

        @Override
        public void characters(char[] ch, int start, int length) throws SAXException {
            if (isInRowRange()) {
                // 如果是数值类型（包含日期），则currentContents获取到的就是单元格内容，如果是字符串类型，则获取的是单元格在文件整体中的序号
                this.currentContents += new String(ch, start, length);
            }
        }

        @Override
        public void endElement(String uri, String localName, String name) throws SAXException {
            try {
                if (isInRowRange()) {
                    boolean cellDateFlag = false;
                    if (this.currentIsString) {
                        int idx = Integer.parseInt(this.currentContents);
                        // 字符串类型，lastContents保存的是单元格的整体下标，需要进一步获取内容
                        this.currentContents = new XSSFRichTextString(sst.getEntryAt(idx)).toString();
                        this.currentIsString = false;
                    }
                    // 如果是日期类型需特殊处理
                    if (this.currentIsDate && !"".equals(this.currentContents)) {
                        XSSFCellStyle style = stylesTable.getStyleAt(this.style);
                        short formatIndex = style.getDataFormat();
                        String formatString = style.getDataFormatString();
                        if (null == formatString) {
                            formatString = "yyyy-MM-dd HH:mm:ss";
                        }
                        DataFormatter formatter = new DataFormatter();
                        this.currentContents = formatter.formatRawCellContents(Double.parseDouble(this.currentContents),
                            formatIndex, formatString);
                        this.currentIsDate = false;
                        cellDateFlag = true;
                    }
                    // v代表单元格内容
                    if (CELL_VALUE_FLAG.equals(name)) {
                        // 若line长度不够则手动扩容
                        if (colIndex >= arrayLength) {
                            arrayLength = arrayLength * 2;
                            Object[] linesCopy = new Object[arrayLength];
                            System.arraycopy(line, 0, linesCopy, 0, line.length);
                            line = linesCopy;
                        }
                        if (colIndex <= maxColIndex - 1) {
                            if (cellDateFlag) {
                                try {
                                    line[colIndex] = new java.sql.Timestamp(sdf.parse(currentContents).getTime());
                                } catch (Exception e) {
                                    line[colIndex] = this.currentContents;
                                }
                            } else {
                                line[colIndex] = this.currentContents;
                            }
                        }
                    }
                }
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }

        @Override
        public void endDocument() throws SAXException {
            // 结束此sheet处理
            if (line != null && isInRowRange() && line.length != 0) {
                for (int i = 0; i < line.length; i++) {
                    if (line[i] == null) {
                        line[i] = "";
                    }
                }
                try {
                    if (notPreviewFlag && this.currentRow != 1) {
                        // 若不存在asset对象则不需要指定
                        Row row = DatasetUtils.convertLineToRow(line, columns);
                        data.add(row);
                    } else {
                        // 第一行以及预览时，直接create row
                        data.add(RowFactory.create(line));
                    }
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }
            }
        }
        /**
         * 判断是否是在读取范围内
         */
            private boolean isInRowRange() {
                if (null == this.startRow || null == this.endRow) {
                    return true;
                } else {
                    if (this.currentRow >= this.startRow && this.currentRow <= this.endRow) {
                        return true;
                    }
                    return false;
                }
            }

            /**
             * 获取列号 A-->1 B-->2
             */
            private int excelColStrToNum(String colStr) {
                int num = 0;
                int result = 0;
                colStr = colStr.replaceAll("[^A-Z]", "");
                int length = colStr.length();
                for (int i = 0; i < length; i++) {
                    char ch = colStr.charAt(length - i - 1);
                    num = (int)(ch - 'A' + 1);
                    num *= Math.pow(26, i);
                    result += num;
                }
                return result;
            }
        }


}
