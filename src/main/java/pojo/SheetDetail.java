package pojo;

import lombok.Data;

import java.util.List;

@Data
public class SheetDetail {

    private String sheetName;

    private int rid;

    List<Column> columns;

    private int maxIndex;

    private List<Object[]> previewData;

    public SheetDetail(String sheetName, int rid) {
        this.sheetName = sheetName;
        this.rid=rid;
    }

    public SheetDetail() {
    }
}
