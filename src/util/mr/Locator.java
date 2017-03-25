package util.mr;

public class Locator implements ILocator {

	private int contextAppendtype;

	private native int nativeCreateLocator();

	private native int nativeDeleteLocator(int iHandle);

	private native int nativeSetSiteDatabase(int iHandle, String strPath);

	private native int nativeProcessLocation(int iHandle, String strInput, String strOupt, int isLoc);

	public CMRLocation getCMRLocation() {
		// TODO Auto-generated method stub
		return null;
	}

	public int CreateLocator(int nMRSource) {
		return nativeCreateLocator();
	}

	public int DeleteLocator(int iHandle) {
		return nativeDeleteLocator(iHandle);
	}

	public int SetSiteDatabase(int iHandle, String strPath) {
		return nativeSetSiteDatabase(iHandle, strPath);
	}

	public int ProcessLocation(int iHandle, String strInput, String strOutput, int isLoc, ILocator.LocationInfo info) {
		return nativeProcessLocation(iHandle, strInput, strOutput, isLoc);
	}

	static {
		try {
			System.loadLibrary("Locator");
		} catch (SecurityException e) {
			e.printStackTrace();
		} catch (UnsatisfiedLinkError e) {
			e.printStackTrace();
		}
	}

	public int getContextAppendtype() {
		return contextAppendtype;
	}

	public void setContextAppendtype(int contextAppendtype) {
		this.contextAppendtype = contextAppendtype;
	}

	public void initCMRLocation() {
		// TODO Auto-generated method stub

	}
	/*
	 * public static void main( String [] args ) { Locator loc = new Locator(); int iLoc = loc.CreateLocator(); loc.SetSiteDatabase( iLoc,
	 * "site_database.txt" ); loc.DeleteLocator( iLoc ); }
	 */
}
