package parser.xparser.tag;

import java.sql.Timestamp;

import framework.ConstDef;

public class ProcessElement extends Tag {

	private OwnerElement owner;

	public ProcessElement() {
		super("process");
	}

	public OwnerElement findOwner(String file, Timestamp time) {
		if (hasChild()) {
			for (Tag t : getChild()) {
				OwnerElement o = (OwnerElement) t;
				if (o.hasChild()) {
					for (Tag tt : o.getChild()) {
						RecordElement re = (RecordElement) tt;
						if (ConstDef.ParseFilePath(re.getFile(), time).equals(file)) {
							return o;
						}
					}
				}
			}
		}
		if (hasChild()) {
			return (OwnerElement) getChild()[0];
		}
		return null;
	}

	@Override
	public Object apply(Object params) {
		// TODO Auto-generated method stub
		return null;
	}

	public OwnerElement getOwner() {
		return owner;
	}

	public void setOwner(OwnerElement owner) {
		this.owner = owner;
	}
}
