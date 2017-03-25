package db.pojo;

/**
 * 厂商POJO "IGP_CONF_VENDOR"表
 * 
 * @author YangJian
 * @since 1.0
 */
public class Vendor {

	private long id;

	private String nameCH;

	private String nameEN;

	public Vendor() {
		super();
	}

	public Vendor(int id, String nameCH, String nameEN) {
		super();
		this.id = id;
		this.nameCH = nameCH;
		this.nameEN = nameEN;
	}

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	public String getNameCH() {
		return nameCH;
	}

	public void setNameCH(String nameCH) {
		this.nameCH = nameCH;
	}

	public String getNameEN() {
		return nameEN;
	}

	public void setNameEN(String nameEN) {
		this.nameEN = nameEN;
	}

}
