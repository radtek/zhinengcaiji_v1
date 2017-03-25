package util.mr;

public class JavaLocator implements ILocator {

	CMRLocation loc = null;

	private int contextAppendtype;

	public int CreateLocator(int nMRSource) {
		loc = new CMRLocation(nMRSource);
		return loc.create();
	}

	public int DeleteLocator(int handle) {
		loc.destroy(handle);
		return 1;
	}

	public void initCMRLocation() {
		loc.setContextAppendtype(contextAppendtype);
	}

	public int ProcessLocation(int handle, String strInput, String strOutput, int isLoc, LocationInfo info) {
		return loc.processMRLocation(handle, strInput, strOutput, isLoc != 0, info);
	}

	public int SetSiteDatabase(int handle, String strPath) {
		return loc.readSiteDatabase(strPath);
	}

	public int getContextAppendtype() {
		return contextAppendtype;
	}

	public void setContextAppendtype(int contextAppendtype) {
		this.contextAppendtype = contextAppendtype;
	}

	public CMRLocation getCMRLocation() {
		return loc;
	}

}
