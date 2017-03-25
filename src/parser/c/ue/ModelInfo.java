package parser.c.ue;

public class ModelInfo {

	public short id;

	public String vendor;

	public String model;

	public ModelInfo(short id, String vendor, String model) {
		super();
		this.id = id;
		this.vendor = vendor;
		this.model = model;
	}

	public ModelInfo() {
		super();
	}

	@Override
	public String toString() {
		return "ModelInfo [id=" + id + ", vendor=" + vendor + ", model=" + model + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((model == null) ? 0 : model.hashCode());
		result = prime * result + ((vendor == null) ? 0 : vendor.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ModelInfo other = (ModelInfo) obj;
		if (model == null) {
			if (other.model != null)
				return false;
		} else if (!model.equals(other.model))
			return false;
		if (vendor == null) {
			if (other.vendor != null)
				return false;
		} else if (!vendor.equals(other.vendor))
			return false;
		return true;
	}

}
