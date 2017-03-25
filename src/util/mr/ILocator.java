package util.mr;

/**
 * 定位接口
 * 
 * @author sunxg
 * 
 */
public interface ILocator {

	public class LocationInfo {

		public int nSrcMRCount = 0;

		public int nLocatedCount = 0;
	};

	public void setContextAppendtype(int contextAppendtype);

	public int CreateLocator(int nMRSource);

	public int DeleteLocator(int iHandle);

	public int SetSiteDatabase(int iHandle, String strPath);

	public void initCMRLocation();

	public CMRLocation getCMRLocation();

	public int ProcessLocation(int iHandle, String strInput, String strOutput, int isLoc, LocationInfo info);
}
