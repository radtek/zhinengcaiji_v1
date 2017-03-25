package templet;

/**
 * 模板信息结构（对应数据库IGP_CONF_TEMPLET表中一条记录）
 * 
 * @author ltp May 7, 2010
 * @since 3.0
 */
public class TempletRecord {

	private int id;

	private int type;

	private String name;

	private String edition;

	private String fileName;

	public TempletRecord() {
		super();
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public int getType() {
		return type;
	}

	public void setType(int type) {
		this.type = type;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getEdition() {
		return edition;
	}

	public void setEdition(String edition) {
		this.edition = edition;
	}

	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

}
