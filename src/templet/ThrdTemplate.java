package templet;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;

import util.Parsecmd;
import util.Util;
import util.mr.ILocator;

import com.Process.CMainThread;

import distributor.DistributeSqlLdr;
import distributor.DistributeTemplet;
import framework.ConstDef;
import framework.SystemConfig;

/**
 * 第三方工具分析方式的模板解析
 * 
 * @author miniz
 */
public class ThrdTemplate extends Atemplate {

	private ArrayList<String> newfilelist;

	public ThrdTemplate() {
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
	}

	public void parse() {
		Parsecmd parsecmd = new Parsecmd();
		String strCurrentPath = SystemConfig.getInstance().getCurrentPath();
		ArrayList<String> flist = super.getFilelist();
		for (int i = 0; i < flist.size(); i++) {
			String strFileName = (String) flist.get(i);
			util.mr.ILocator loc = null;
			SimpleDateFormat spformat = new SimpleDateFormat("yyyyMMddHHmmss");
			Calendar cdar = Calendar.getInstance();
			long t1 = System.currentTimeMillis();
			// 调用定位算法
			System.out.println("线程：" + this.m_TaskInfo.getTaskID() + ",Locating start:" + spformat.format(cdar.getTime()));
			if (0 == ((ThirdTempletP) m_TaskInfo.getParseTemplet()).nLocateJava) {
				loc = new util.mr.Locator();
			} else {
				loc = new util.mr.JavaLocator();
			}
			Date now = new Date(m_TaskInfo.getLastCollectTime().getTime());
			String strTime = spformat.format(now);
			int iHandle = loc.CreateLocator(((ThirdTempletP) m_TaskInfo.getParseTemplet()).nMRSource);
			// 初始化Site数据库

			int iRet = loc.SetSiteDatabase(iHandle, "site_database.txt");
			if (iRet != 0) {
				System.out.println("SetSiteDatabase Error!");
				loc.DeleteLocator(iHandle);
				return;
			}

			// modify by YangJian Cause:原始MR记录经过定位程序定位后输出到
			// "afterLocated\"+cityCode 为分类的文件夹下，修改之前的输出为 直接放在根目录下；
			String cityCode = String.valueOf(m_TaskInfo.getDevInfo().getCityID());
			// String strCityCodePrefix = "afterLocated" + File.separator +
			// cityCode;
			String strCityCodePrefix = cityCode;

			String strTxtName = m_TaskInfo.getGroupId() + "_" + m_TaskInfo.getTaskID() + "_" + strTime;
			String strOutCitypath = SystemConfig.getInstance().getMROutputPath() + File.separatorChar + strCityCodePrefix;
			String strOutput = strCurrentPath + File.separator + strTxtName + ".txt";
			String strTmpDir = strCurrentPath + File.separatorChar + "temp";
			File temp = new File(strTmpDir);
			if (!temp.exists())
				temp.mkdirs();
			File outputpath = new File(strOutCitypath);
			if (!outputpath.exists())
				outputpath.mkdirs();

			int nLocate = ((ThirdTempletP) m_TaskInfo.getParseTemplet()).nLocate;
			int ncontextappendtype = ((ThirdTempletP) m_TaskInfo.getParseTemplet()).getNcontextappendtype();
			int nfilesplittype = ((ThirdTempletP) m_TaskInfo.getParseTemplet()).getNfilenamesplittype();
			loc.setContextAppendtype(ncontextappendtype);
			loc.getCMRLocation().setFilesplittype(nfilesplittype);
			loc.initCMRLocation();

			ILocator.LocationInfo info = new ILocator.LocationInfo();
			iRet = loc.ProcessLocation(iHandle, strFileName, strOutput, nLocate, info);

			if (iRet != 0) {
				System.out.println("ProcessLocation Error:" + strFileName + "to " + strOutput + ",code=" + iRet);
				loc.DeleteLocator(iHandle);
				return;
			}
			System.out.println("ProcessLocation iret=" + iRet);
			loc.DeleteLocator(iHandle);
			// 定位算法结束
			try {
				String[] outputfiles = loc.getCMRLocation().getFilesplitadapt().getoutputfilenames();
				for (int o = 0; o < outputfiles.length; o++) {
					// 将移动文件动作缓存起来.最后一次性提交
					parsecmd.addfile(outputfiles[o], strOutCitypath);
				}
			} catch (Exception e) {

			}
			System.out.println("线程：" + this.m_TaskInfo.getTaskID() + ",Locating end:" + spformat.format(Calendar.getInstance().getTime()) + ",耗时："
					+ (System.currentTimeMillis() - t1) + "MR数" + info.nSrcMRCount + "," + "定位数" + info.nLocatedCount);
			m_TaskInfo.m_nAllRecordCount = info.nLocatedCount;
			if (((DistributeTemplet) m_TaskInfo.getDistributeTemplet()).stockStyle == ConstDef.COLLECT_DISTRIBUTE_SQLLDR) {
				System.out.println("MR入库开始");
				// 调用Sqlldr导入数据
				DistributeSqlLdr sqlldr = new DistributeSqlLdr(m_TaskInfo);
				sqlldr.buildSqlLdr(i, strTxtName);
			}
		}
		// 开始移动文件到指定的目录
		parsecmd.comitmovefiles();
		newfilelist = parsecmd.getFilelist();
		String[] filenames = (String[]) newfilelist.toArray(new String[0]);
		if (SystemConfig.getInstance().isMRSingleCal()) {
			CMainThread cthread = new CMainThread();
			String sdatatime = Util.getDateString_yyyyMMddHH(this.m_TaskInfo.getLastCollectTime());
			String paras = this.m_TaskInfo.getDevInfo().getCityID() + "," + this.m_TaskInfo.getDevInfo().getCityID() + ","
					+ this.m_TaskInfo.getDevInfo().getVendor() + "," + sdatatime + "," + filenames[0];
			// String
			// paras="755,深圳,ZY0808,200906050310,d:\\data\\2000001_20001201_20090524095000.txt";
			cthread.startsinglefile(paras);
		}
	}

	public ArrayList<String> getNewfilelist() {
		return newfilelist;
	}

	public void setNewfilelist(ArrayList<String> newfilelist) {
		this.newfilelist = newfilelist;
	}
}
