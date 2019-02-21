package pojo;


public class Column implements Comparable<Column>{

    private String asset_id;

    private String col_name;

    private int col_index;

    private String col_alias;

    private String col_desc;

    private int col_datatype;

    private Integer scale;

    private Integer precision;

    private String textList;

    public Column() {
    }

    public Column(String col_name, int col_index, int col_datatype) {
        this.col_name = col_name;
        this.col_index = col_index;
        this.col_datatype = col_datatype;
    }

    public Column(String col_name, int col_index, int col_datatype, Integer scale) {
        this.col_name = col_name;
        this.col_index = col_index;
        this.col_datatype = col_datatype;
        this.scale = scale;
    }

    public String getAsset_id() {
		return asset_id;
	}

	public void setAsset_id(String asset_id) {
		this.asset_id = asset_id;
	}

	public String getCol_name() {
        return col_name;
    }

    public void setCol_name(String col_name) {
        this.col_name = col_name;
    }

    public int getCol_index() {
        return col_index;
    }

    public void setCol_index(int col_index) {
        this.col_index = col_index;
    }

    public String getCol_alias() {
        return col_alias;
    }

    public void setCol_alias(String col_alias) {
        this.col_alias = col_alias;
    }

    public String getCol_desc() {
        return col_desc;
    }

    public void setCol_desc(String col_desc) {
        this.col_desc = col_desc;
    }

    public int getCol_datatype() {
        return col_datatype;
    }

    public void setCol_datatype(int col_datatype) {
        this.col_datatype = col_datatype;
    }

    public Integer getScale() {
        return scale;
    }

    public void setScale(Integer scale) {
        this.scale = scale;
    }

    public String getTextList() {
        return textList;
    }

    public void setTextList(String textList) {
        this.textList = textList;
    }

    @Override
    public int compareTo(Column o) {
        return col_index - o.col_index;
    }

    public Integer getPrecision() {
        return precision;
    }

    public void setPrecision(Integer precision) {
        this.precision = precision;
    }
}
