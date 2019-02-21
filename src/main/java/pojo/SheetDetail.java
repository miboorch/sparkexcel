package pojo;

import lombok.Data;

import java.util.List;

@Data
public class SheetDetail {

    private String sheetName;

    private int rid;

    List<Column> columns;

    private int maxIndex;

}
