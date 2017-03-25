package util.mr;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Random;
import java.util.Vector;

import framework.SystemConfig;

class CellInfo {

	int ci;

	int lac;

	double longi;

	double lati;

	int freq_range; // 频段 900 or 1800

	float anten_direct; // 天线方向角(081017,int->float)

	int bsic;

	int bcch;

	float anten_half_power; // 天线半功率角(081017,int->float)

	float anten_height; // 天线高度

	int full_power_send; // 发射满功率(db)

	int cellType; // 室内(2)或室外(1)

	float N_Value;

	float keyX;

	int city_id;

	String ne_sys_id;
};

class LACCIs {

	Vector<Integer> v = new Vector<Integer>();
};

class NEIB implements Cloneable {

	int Rxlev;

	int bcch;

	int NCC;

	int BCC;

	int lac;

	int ci;

	int bsic;

	public Object clone() throws CloneNotSupportedException {
		return super.clone();
	}
};

class MR implements Cloneable {

	MR() {
		for (int i = 0; i < 6; i++) {
			neib_ci[i] = new NEIB();
		}
	}

	public Object clone() throws CloneNotSupportedException {
		MR mr = (MR) super.clone();
		mr.neib_ci = new NEIB[6];

		for (int i = 0; i < 6; i++) {
			mr.neib_ci[i] = (NEIB) neib_ci[i].clone();
		}

		return mr;
	}

	String report_date = null;

	String report_time = null;

	String CGI = null;

	int TRX = 0;

	int time_slot = 0;

	int result_num = 0;

	int is_valid = 0;

	int has_next = 0;

	int channel_type = 0;

	int DTXd = 0;

	int RXLEV_FULL_up = 0;

	int RXLEV_SUB_up = 0;

	int RXQUAL_FULL_up = 0;

	int RXQUAL_SUB_up = 0;

	int Power_Level = 0;

	int MS_Power_Level = 0;

	int Timing_Advance = 0;

	int BA_USED = 0;

	int DTX_USED = 0;

	int RXLEV_FULL_SERVING = 0;

	int BA_USED_3G = 0;

	int MEAS_VALID = 0;

	int RXLEV_SUB_SERVING = 0;

	int RXLEV_SUB_nonPC = 0;

	int RXQUAL_FULL_SERVING = 0;

	int RXQUAL_SUB_SERVING = 0;

	int decline_diff = 0;

	int instant_power = 0;

	int neib_num = 0; // v1.5

	NEIB neib_ci[] = new NEIB[6];

	int lac = 0;

	int ci = 0;

	String MSG_Time = null;

	double longi = 0.0; // 经纬度，由Location返回

	double lati = 0.0;

	int city_id = 0;

	String ne_sys_id;

	float dir; // xurc

	int cellType; // xurc 室内室外

	int nBand; // 频段 900 or 1800

	float N_Value;

	float keyX;

	double dCellLon;

	double dCellLat;
};

class TMP_NEIB_CELL_INFO {

	int lac = 0;

	int ci = 0;

	double longi = 0.0;

	double lati = 0.0;

	double sqr_distance = 0.0; // 与主服务小区经纬度差值平方

	int Rxlev = 0;

	float dir; // 方向角 xurc

	float N_Value;

	// 排序比较函数
	static Comparator<TMP_NEIB_CELL_INFO> comp_neib_rxlev = new Comparator<TMP_NEIB_CELL_INFO>() {

		public int compare(TMP_NEIB_CELL_INFO o1, TMP_NEIB_CELL_INFO o2) {
			TMP_NEIB_CELL_INFO p1 = (TMP_NEIB_CELL_INFO) o1;
			TMP_NEIB_CELL_INFO p2 = (TMP_NEIB_CELL_INFO) o2;
			if (p1.Rxlev < p2.Rxlev)
				return 1;
			else
				return 0;
		}
	};

	static Comparator<TMP_NEIB_CELL_INFO> comp_neib_nearer = new Comparator<TMP_NEIB_CELL_INFO>() {

		public int compare(TMP_NEIB_CELL_INFO o1, TMP_NEIB_CELL_INFO o2) {
			TMP_NEIB_CELL_INFO p1 = (TMP_NEIB_CELL_INFO) o1;
			TMP_NEIB_CELL_INFO p2 = (TMP_NEIB_CELL_INFO) o2;
			if (p1.sqr_distance < p2.sqr_distance)
				return 1;
			else
				return 0;
		}
	};

};

class TmpNeis {

	Vector<TMP_NEIB_CELL_INFO> v = new Vector<TMP_NEIB_CELL_INFO>();
};

class Cells {

	HashMap<Integer, CellInfo> m = new HashMap<Integer, CellInfo>();
};

class BcchBSICs {

	HashMap<Integer, LACCIs> m = new HashMap<Integer, LACCIs>();
}

class LocationLonLat {

	double dLon;

	double dLat;
};

class LocPoints {

	Vector<LocationLonLat> v = new Vector<LocationLonLat>();
};

class BsicbcchNeibs {

	HashMap<Integer, TMP_NEIB_CELL_INFO> m = new HashMap<Integer, TMP_NEIB_CELL_INFO>();
}

class CellNeibs {

	HashMap<Integer, BsicbcchNeibs> m = new HashMap<Integer, BsicbcchNeibs>();
}

class MR_ADJ {

	int nIndex;

	int nCount;

	MR mrs[] = new MR[15];

	MR_ADJ() {
		nIndex = 0;
		nCount = 0;
	}
};

class MapTrxTSMR {

	HashMap<Integer, MR_ADJ> m = new HashMap<Integer, MR_ADJ>();
};

class MapMRADJs {

	HashMap<Integer, MapTrxTSMR> m = new HashMap<Integer, MapTrxTSMR>();
}

class LocComm {

	final static int Serving_Rxlev_Min = -96;

	final static int Neib_Rxlev_Min = -110;

	final static int TA_MAX = 7;

	final static int TA_Distance = 544;

	final static int Rxlev_Max = 43;

	final static double RANGE_SEZE = 0.001;

	final static int VALUE_NORMAL = 0;

	final static int VALUE_FAILED_FILE_SITEDATABASE_OPEN = 1;

	final static int VALUE_FAILED_FILE_MRINPUT_OPEN = 2;

	final static int VALUE_FAILED_FILE_MROUT_OPEN = 3;

	final static int VALUE_FILE_END = 4;

	final static int VALUE_FILE_NO_Cell = 5;

	final static int VALUE_LONLAT_NOVALID = 6;

};

public class CMRCalculate {

	// 小区信息 LAC+CI
	private Cells m_lac_ci_map = new Cells();

	// 同BSIC同BCCH小区组合
	private BcchBSICs m_bsic_bcch_map = new BcchBSICs();

	private double m_dbInvalidDistance;

	private double m_dbWeigBase;

	private double m_disperX;

	double m_disperY;

	private CellNeibs pCellNeis[] = new CellNeibs[100];

	private LocationLonLat pLocPtsFirst[][] = new LocationLonLat[100][];

	private LocationLonLat pLocPtsSecond[][] = new LocationLonLat[100][];

	private int pLocNum[] = new int[100];

	double pi = 3.14159265358979324;

	public static boolean bFast = false;

	Random rnd = new Random();

	class InOut {

		int nInCount;

		int nOutCount;
	}

	public CMRCalculate() {
		m_dbInvalidDistance = 0.02;
		m_dbWeigBase = 1.0;

		for (int i = 0; i < 100; i++) {
			pCellNeis[i] = null;
			pLocNum[i] = 0;
			pLocPtsFirst[i] = null;
			pLocPtsSecond[i] = null;
		}
	};

	public void Close() {
	};

	final public int getHandle() {
		int nID = -1;
		for (int i = 0; i < 100; i++) {
			if (pCellNeis[i] == null) {
				pCellNeis[i] = new CellNeibs();
				pLocNum[i] = 1000;
				pLocPtsFirst[i] = new LocationLonLat[1000];
				pLocPtsSecond[i] = new LocationLonLat[1000];
				nID = i;
				break;
			} else
				continue;
		}

		return nID;
	};

	public void destroyHandle(int nHandle) {
	};

	final public int getSiteDatabase(String szFileName) {
		String sitedis = "";

		sitedis = SystemConfig.getInstance().getSiteDistRange() + "";
		try {
			this.m_dbInvalidDistance = Double.parseDouble(sitedis);
		} catch (Exception e) {
			e.printStackTrace();
		}
		BufferedReader br;
		try {
			br = new BufferedReader(new FileReader(szFileName));
		} catch (Exception e) {
			e.printStackTrace();
			return LocComm.VALUE_FAILED_FILE_SITEDATABASE_OPEN;
		}

		CellInfo cellInfo = null;
		try {

			br.readLine();

			CellInfo info;
			while (true) {
				String s = br.readLine();
				if (s == null)
					break;

				String[] sa = s.split("\t");

				info = new CellInfo();
				if (sa.length != 16)
					continue;

				info.ci = Integer.parseInt(sa[0]);
				info.longi = Double.parseDouble(sa[1]);
				info.lati = Double.parseDouble(sa[2]);
				info.freq_range = Integer.parseInt(sa[3]);
				info.lac = Integer.parseInt(sa[4]);
				info.anten_direct = (float) Double.parseDouble(sa[5]);
				info.bsic = Integer.parseInt(sa[6]);
				info.bcch = Integer.parseInt(sa[7]);
				info.anten_half_power = (float) Double.parseDouble(sa[8]);
				info.anten_height = (float) Double.parseDouble(sa[9]);
				info.full_power_send = Integer.parseInt(sa[10]);
				info.cellType = Integer.parseInt(sa[11]);
				info.N_Value = (float) Double.parseDouble(sa[12]);
				info.keyX = (float) Double.parseDouble(sa[13]);
				info.city_id = Integer.parseInt(sa[14]);
				info.ne_sys_id = sa[15];

				int lacci = (info.lac) << 16 | info.ci;

				if (lacci != 0) {
					cellInfo = info;

					if (m_lac_ci_map.m.get(lacci) == null)
						m_lac_ci_map.m.put(lacci, info);

					LACCIs v_tmp = m_bsic_bcch_map.m.get((info.bsic) << 16 | info.bcch);
					if (v_tmp == null) {
						v_tmp = new LACCIs();
						v_tmp.v.add(lacci);
						m_bsic_bcch_map.m.put(info.bsic << 16 | info.bcch, v_tmp);
					} else {
						if (-1 == v_tmp.v.indexOf(lacci)) {
							v_tmp.v.add(lacci);
						}
					}
				}

			} // while

			br.close();

		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("读取Sitebase" + szFileName + "过程中出现错误");
		}

		if (cellInfo != null && cellInfo.lac > 0) {
			double dx = cellInfo.longi;
			double dy = cellInfo.lati;

			m_disperX = calcDistance(dx - 1, dy, dx + 1, dy) / 2;
			m_disperY = calcDistance(dx, dy - 1, dx, dy + 1) / 2;
		} else
			return LocComm.VALUE_FILE_NO_Cell;

		return LocComm.VALUE_NORMAL;
	}

	final public boolean bMRCellExist(MR mr) {
		CellInfo cellinfo = m_lac_ci_map.m.get((mr.lac) << 16 | mr.ci);

		if (cellinfo != null) {
			mr.dCellLon = cellinfo.longi;
			mr.dCellLat = cellinfo.lati;
			mr.dir = cellinfo.anten_direct;
			mr.cellType = cellinfo.cellType;
			mr.nBand = cellinfo.freq_range;
			mr.N_Value = cellinfo.N_Value;
			mr.keyX = cellinfo.keyX;

			mr.city_id = cellinfo.city_id;
			mr.ne_sys_id = cellinfo.ne_sys_id;

			return true;
		}

		return false;
	}

	final public int processMR(int nHandle, MR mr, boolean bLoc) {
		int ulpl = 43 - mr.MS_Power_Level * 2 - mr.RXLEV_SUB_up;
		int dlpl = 43 - mr.Power_Level * 2 - mr.RXLEV_SUB_SERVING;

		mr.decline_diff = ulpl - dlpl;

		if (bLoc) {
			TmpNeis neib_vec = new TmpNeis(); // 邻区经纬度数组，用于存储最多6个邻区

			FindNeibCell(nHandle, mr, neib_vec);

			if (!bFast) {
				GetNewLocInfo(nHandle, mr, neib_vec);

				double lon = mr.longi;
				double lat = mr.lati;

				GetLocInfo(mr, neib_vec);

				mr.longi = lon + (mr.longi - lon) * mr.keyX;
				mr.lati = lat + (mr.lati - lat) * mr.keyX;
			} else {
				GetLocInfo(mr, neib_vec);
			}
		}

		return LocComm.VALUE_NORMAL;
	}

	final public void FindNeibCell(int nHandle, MR mr, TmpNeis neib_vec) {
		int bsic = 0;
		int bcch = 0;
		int i = 0, j = 0;

		int lacci = (mr.lac) << 16 | mr.ci;

		BsicbcchNeibs neis;
		boolean bExist = false;

		CellNeibs pOneCellNeis = pCellNeis[nHandle];

		neis = pOneCellNeis.m.get(lacci);
		if (neis != null) {
			bExist = true;
		} else {
			neis = new BsicbcchNeibs();
		}

		for (; i < mr.neib_num; i++) {
			bsic = mr.neib_ci[i].bsic;
			bcch = mr.neib_ci[i].bcch;

			// 用于临时存储同一BSIC BCCH的多个小区
			TmpNeis tmp_neib_vec = new TmpNeis();
			if (bcch != 0xffff && bsic != 0xff) {
				int bsicbcch = ((int) bsic) << 16 | bcch;

				TMP_NEIB_CELL_INFO tnci = neis.m.get(bsicbcch);
				if (tnci != null) {
					tnci.Rxlev = mr.neib_ci[i].Rxlev;
					neib_vec.v.add(tnci);
					continue;
				}

				LACCIs laccis = m_bsic_bcch_map.m.get(((int) bsic) << 16 | bcch);

				if (laccis != null) {
					// 距离最近的小区由由经纬度差值平方来确定（v1.0）
					for (j = 0; j < laccis.v.size(); j++) {
						CellInfo cellinfo = m_lac_ci_map.m.get(laccis.v.get(j));

						if (cellinfo != null) {
							TMP_NEIB_CELL_INFO tmp_neib = new TMP_NEIB_CELL_INFO();
							tmp_neib.lac = cellinfo.lac;
							tmp_neib.ci = cellinfo.ci;
							tmp_neib.longi = cellinfo.longi;
							tmp_neib.lati = cellinfo.lati;
							tmp_neib.dir = cellinfo.anten_direct;
							tmp_neib.N_Value = cellinfo.N_Value;
							tmp_neib.sqr_distance = Math.pow((cellinfo.longi - mr.dCellLon), 2) + Math.pow(cellinfo.lati - mr.dCellLat, 2);
							tmp_neib.Rxlev = mr.neib_ci[i].Rxlev;

							tmp_neib_vec.v.add(tmp_neib);
						}
					}

					Collections.sort(tmp_neib_vec.v, TMP_NEIB_CELL_INFO.comp_neib_nearer);

					mr.neib_ci[i].lac = tmp_neib_vec.v.get(0).lac;
					mr.neib_ci[i].ci = tmp_neib_vec.v.get(0).ci;

					// 距离超过0.05也视为非法点(v1.1.1)
					// 距离超过0.03视为非法点(v1.1.2)
					double invalid_distance = m_dbInvalidDistance;
					if (tmp_neib_vec.v.get(0).sqr_distance <= invalid_distance * invalid_distance) {
						neib_vec.v.add(tmp_neib_vec.v.get(0));

						neis.m.put(bsicbcch, tmp_neib_vec.v.get(0));
					}
				}
			}
		}

		if (!bExist) {
			pOneCellNeis.m.put(lacci, neis);
		}

		Collections.sort(neib_vec.v, TMP_NEIB_CELL_INFO.comp_neib_rxlev);
	}

	final protected void GetLocInfo(MR mr, TmpNeis neib_vec) {
		double longi = mr.dCellLon;
		double lati = mr.dCellLat;
		int i = 0;

		// 先去除非法点（v1.1）
		double x_ave = longi, y_ave = lati, x_sqrt = 0, y_sqrt = 0;
		for (i = 0; i < neib_vec.v.size(); i++) {
			x_ave += neib_vec.v.get(i).longi;
			y_ave += neib_vec.v.get(i).lati;
		}
		x_ave /= (neib_vec.v.size() + 1);
		y_ave /= (neib_vec.v.size() + 1);

		x_sqrt = Math.pow(longi - x_ave, 2);
		y_sqrt = Math.pow(lati - y_ave, 2);
		for (TMP_NEIB_CELL_INFO tnci : neib_vec.v) {
			x_sqrt += Math.pow(tnci.longi - x_ave, 2);
			y_sqrt += Math.pow(tnci.lati - y_ave, 2);
		}
		x_sqrt /= neib_vec.v.size();
		y_sqrt /= neib_vec.v.size();
		x_sqrt = Math.sqrt(x_sqrt);
		y_sqrt = Math.sqrt(y_sqrt);

		for (Iterator<TMP_NEIB_CELL_INFO> neib_vec_itr = neib_vec.v.iterator(); neib_vec_itr.hasNext();) {
			TMP_NEIB_CELL_INFO tnci = neib_vec_itr.next();
			if (Math.abs(tnci.longi - longi) >= 2.5 * x_sqrt || Math.abs(tnci.lati - lati) >= 2.5 * y_sqrt) {
				neib_vec.v.remove(tnci); // 删除非法点（v1.1）
				neib_vec_itr = neib_vec.v.iterator();
			}
		}

		int last_neib_rxlev = 0;
		if (neib_vec.v.size() > 0) {
			last_neib_rxlev = neib_vec.v.get(neib_vec.v.size() - 1).Rxlev;
		} else {
			last_neib_rxlev = mr.RXLEV_SUB_SERVING;
		}

		// 权值由1调整为0.6 0.8 1.2 1.4 1.6，测试结果以确定合理权值(v1.1.1)
		// 权值输出0.8 1.0 1.2(v1.1.2)
		double weig_base = m_dbWeigBase;
		if (mr.RXLEV_SUB_SERVING < last_neib_rxlev) {
			last_neib_rxlev = mr.RXLEV_SUB_SERVING;
		}
		double weig_cell = (mr.RXLEV_SUB_SERVING - last_neib_rxlev) * 1.0 / 10 + weig_base;
		double weig_sum = 0; // 加权值（v1.1）
		if (weig_cell >= weig_base) {
			weig_sum += weig_cell;
			longi *= weig_cell;
			lati *= weig_cell;
		}

		for (TMP_NEIB_CELL_INFO tnci : neib_vec.v) {
			weig_cell = (tnci.Rxlev - last_neib_rxlev) * 1.0 / 10 + weig_base;
			if (weig_cell >= weig_base) {
				longi += (tnci.longi * weig_cell);
				lati += (tnci.lati * weig_cell);
				weig_sum += weig_cell;
			}
		}
		longi /= weig_sum;
		lati /= weig_sum;

		// 去掉定位直线，v1.7
		if (mr.dCellLon < 500 && neib_vec.v.size() < 3) {
			int nRand = rnd.nextInt();

			double dbDistance = Math.sqrt((longi - mr.dCellLon) * (longi - mr.dCellLon) + (lati - mr.dCellLat) * (lati - mr.dCellLat));
			longi = longi - dbDistance * 0.996 * (nRand % 2 == 0 ? 1 : -1);
			lati = lati + dbDistance * 0.0872 * (nRand % 2 == 0 ? 1 : -1);
		}

		mr.longi = longi;
		mr.lati = lati;
	}

	// 袁国中算法
	final protected boolean GetNewLocInfo(int nHandle, MR mr, TmpNeis neib_vec) {
		if (mr.RXLEV_SUB_SERVING >= -1 /* || mr.Timing_Advance > TA_MAX */)
			return false;

		// 室内
		if (mr.cellType == 2) {
			GetInsideMRLonLat(mr);

			return true;
		}

		int ta = mr.Timing_Advance;
		if (ta > LocComm.TA_MAX)
			ta = 4;

		double dTA = (ta + 1) * 544;

		double dLonMin = mr.dCellLon - dTA / m_disperX;
		double dLonMax = mr.dCellLon + dTA / m_disperX;
		double dLatMin = mr.dCellLat - dTA / m_disperY;
		double dLatMax = mr.dCellLat + dTA / m_disperY;

		int nCount = ((int) ((dLonMax - dLonMin) * 1000) + 1) * ((int) ((dLatMax - dLatMin) * 1000) + 1);

		if (nCount > pLocNum[nHandle]) {
			pLocPtsFirst[nHandle] = new LocationLonLat[nCount];

			pLocPtsSecond[nHandle] = new LocationLonLat[nCount];

			pLocNum[nHandle] = nCount;
		}

		double dLon = dLonMin;

		int nIndex = 0;

		LocationLonLat[] pIniPts = pLocPtsFirst[nHandle];
		while (dLon < dLonMax) {
			double dLat = dLatMin;
			while (dLat < dLatMax) {
				LocationLonLat pt = new LocationLonLat();
				pt.dLon = dLon;
				pt.dLat = dLat;

				pIniPts[nIndex] = pt;
				nIndex++;

				dLat += LocComm.RANGE_SEZE;
			}
			dLon += LocComm.RANGE_SEZE;
		}

		// 排除ＴＡ和方向角左右60度范围外
		double dTAMin = 0, dTAMax;
		dTAMax = (ta + 1) * LocComm.TA_Distance;
		if (ta >= 2)
			dTAMin = (ta - 2) * LocComm.TA_Distance;

		double dDR = (dTAMax - dTAMin) / 2;
		double dist = (dTAMax + dTAMin) / 2;

		LocationLonLat[] pTaPts = pLocPtsSecond[nHandle];
		int nTaCount = 0;
		InOut inout = new InOut();
		inout.nInCount = nIndex;
		inout.nOutCount = nTaCount;
		if (!GetCommPoints(pIniPts, pTaPts, inout, mr.dCellLon, mr.dCellLat, mr.dir, dist, dDR)) {
			return false;
		}
		nIndex = inout.nInCount;
		nTaCount = inout.nOutCount;

		if ((ta == 0 && mr.RXLEV_SUB_SERVING < LocComm.Serving_Rxlev_Min) || neib_vec.v.size() == 0) {
			GetRadomLocFromPts(mr, pTaPts, nTaCount);

			return true;
		}

		float n = mr.N_Value;

		// ////////////////////////////////////////////////////////////////////////
		// 服务小区的Rxlev排除
		// UL: + 4 + 16 - 4 = 16
		int ulPL;
		if (mr.nBand == 900) {
			if (mr.MS_Power_Level < 5)
				ulPL = 43 + 16 - mr.RXLEV_SUB_up;
			else
				ulPL = 43 - (mr.MS_Power_Level - 5) * 2 + 16 - mr.RXLEV_SUB_up;
		} else
			ulPL = 33 - mr.MS_Power_Level * 2 + 16 - mr.RXLEV_SUB_up;

		// DL : - 5 - 4 + 16 = 7
		int dlPL = 43 - mr.Power_Level * 2 + 7 - mr.RXLEV_SUB_SERVING;

		double dPathLoss = (double) (ulPL + dlPL) / 2;

		// dist = GetDistanceByRxlev(mr.RXLEV_SUB_SERVING, n);

		dist = Math.pow(10, dPathLoss / 10 / n);

		LocationLonLat[] pRxPts = pLocPtsFirst[nHandle];
		int nRxCount = 0;
		LocationLonLat[] pNeiPts;
		inout.nInCount = nTaCount;
		inout.nOutCount = nRxCount;
		if (!GetCommPoints(pTaPts, pRxPts, inout, mr.dCellLon, mr.dCellLat, mr.dir, dist, 272)) {
			nTaCount = inout.nInCount;
			nRxCount = inout.nOutCount;

			pRxPts = pTaPts;
			nRxCount = nTaCount;

			pNeiPts = pLocPtsFirst[nHandle];
		} else {
			nTaCount = inout.nInCount;
			nRxCount = inout.nOutCount;
			pNeiPts = pLocPtsSecond[nHandle];
		}
		// ////////////////////////////////////////////////////////////////////////

		// ////////////////////////////////////////////////////////////////////////
		// 优化修改
		int nMaxNei = 0;
		int nNeiCount = 0;
		for (int j = 0; j < nRxCount; j++) {
			int nNei = 0;

			for (int i = 0; i < neib_vec.v.size(); i++) {
				TMP_NEIB_CELL_INFO tmpNei = neib_vec.v.get(i);

				if (tmpNei.Rxlev == 0 || tmpNei.Rxlev < LocComm.Neib_Rxlev_Min)
					continue;

				dist = GetDistanceByRxlev(tmpNei.Rxlev, tmpNei.N_Value);

				if (IsPointInSectorArea(pRxPts[j].dLon, pRxPts[j].dLat, tmpNei.longi, tmpNei.lati, tmpNei.dir, dist, 272)) {
					nNei++;
				}
			}

			if (nNei > 0) {
				if (nNei > nMaxNei) {
					nNeiCount = 0;
					pNeiPts[nNeiCount] = pRxPts[j];
					nNeiCount++;

					nMaxNei = nNei;
				} else if (nNei == nMaxNei) {
					pNeiPts[nNeiCount] = pRxPts[j];
					nNeiCount++;
				}
			}
		}

		if (nNeiCount > 0) {
			GetRadomLocFromPts(mr, pNeiPts, nNeiCount);
		} else {
			GetRadomLocFromPts(mr, pRxPts, nRxCount);
		}

		return true;
	}

	final protected void GetInsideMRLonLat(MR mr) {
		int nAngle = rnd.nextInt() % 360;
		int nRadio = rnd.nextInt() % 50;

		// double dist = GetDistanceByRxlev(mr.RXLEV_SUB_SERVING, mr.N_Value);

		double dx = nRadio * Math.cos(nAngle) / m_disperX;
		double dy = nRadio * Math.sin(nAngle) / m_disperY;

		mr.longi = mr.dCellLon + dx;
		mr.lati = mr.dCellLat + dy;
	}

	final private double calcDistance(double lon1, double lat1, double lon2, double lat2) {
		double dLonDec = Math.abs(lon1 - lon2);
		double dLatDec = Math.abs(lat1 - lat2);
		if (dLonDec < 1E-9 && dLatDec < 1E-9)
			return 0;

		double detaLon = dLonDec;
		if (detaLon > 180)
			detaLon = 360 - detaLon;

		double cosvalue = 1
				- (Math.cos(lat1 * pi / 180.0) * Math.cos(lat1 * pi / 180.0) + Math.cos(lat2 * pi / 180.0) * Math.cos(lat2 * pi / 180.0) - 2.0
						* Math.cos(lat1 * pi / 180.0) * Math.cos(lat2 * pi / 180.0) * Math.cos(dLonDec * pi / 180.0)) / 2.0
				/ Math.cos(dLatDec * pi / 180.0);
		cosvalue = Math.sin(lat1 * pi / 180.0) * Math.sin(lat2 * pi / 180.0) + Math.cos(lat1 * pi / 180.0) * Math.cos(lat2 * pi / 180.0)
				* Math.cos(detaLon * pi / 180.0);
		double acosvalue = Math.acos(cosvalue);

		return 6371004 * acosvalue;
	}

	final private boolean IsPointInSectorArea(double dLon, double dLat, double dSectorLon, double dSectorLat, float dir, double dist, double dDR) {
		double dDistPtP = calcDistance(dLon, dLat, dSectorLon, dSectorLat);

		if (dDistPtP > dist + dDR || dDistPtP < dist - dDR)
			return false;

		if (360 == (int) dir || dDistPtP < 1E-9)
			return true;

		double pi = 3.14159265358979324;

		double dRange;
		double dTemp;
		if (dLon >= dSectorLon && dLat >= dSectorLat) {
			dTemp = calcDistance(dSectorLon, dSectorLat, dLon, dSectorLat);
			dRange = (Math.asin(dTemp / dDistPtP)) * 180 / pi;
		} else if (dLon >= dSectorLon && dLat < dSectorLat) {
			dTemp = calcDistance(dSectorLon, dSectorLat, dLon, dSectorLat);
			dRange = 180 - (Math.asin(dTemp / dDistPtP)) * 180 / pi;
		} else if (dLon < dSectorLon && dLat >= dSectorLat) {
			dTemp = calcDistance(dSectorLon, dSectorLat, dLon, dSectorLat);
			dRange = 360 - (Math.asin(dTemp / dDistPtP)) * 180 / pi;
		} else {
			dTemp = calcDistance(dSectorLon, dSectorLat, dLon, dSectorLat);
			dRange = 180 + (Math.asin(dTemp / dDistPtP)) * 180 / pi;
		}

		float RangeMin = dir - 60;
		if (RangeMin < 0)
			RangeMin += 360;

		float RangMax = dir + 60;
		if (RangMax >= 360)
			RangMax -= 360;

		if (dir >= 60 && dir < 300) {
			if (dRange < RangeMin || dRange > RangMax)
				return false;
		} else {
			if (dRange < RangeMin && dRange > RangMax)
				return false;
		}

		return true;
	}

	final private double GetDistanceByRxlev(int rxlev, float n) {
		// DL : - 5 - 4 + 16 = 7
		double dA1 = (double) (LocComm.Rxlev_Max - 0.605596 * 2 + 7 - rxlev);

		return Math.pow(10, dA1 / 10 / n);
	}

	final private boolean GetCommPoints(LocationLonLat[] pIntpts, LocationLonLat[] pOutpts, InOut inout, double dSectorLon, double dSectorLat,
			float dir, double dist, double dDR) {
		inout.nOutCount = 0;
		for (int i = 0; i < inout.nInCount; i++) {
			if (IsPointInSectorArea(pIntpts[i].dLon, pIntpts[i].dLat, dSectorLon, dSectorLat, dir, dist, dDR)) {
				pOutpts[inout.nOutCount] = pIntpts[i];
				inout.nOutCount++;
			}
		}

		if (inout.nOutCount > 0)
			return true;

		return false;
	}

	final private void GetRadomLocFromPts(MR mr, LocationLonLat[] pPts, int nCount) {

		int nPos = rnd.nextInt(Integer.MAX_VALUE) % (nCount);

		LocationLonLat pt = pPts[nPos];

		double nPosX = (rnd.nextInt() % 1000) * 0.000001;
		double nPosY = (rnd.nextInt() % 1000) * 0.000001;

		pt.dLon += nPosX;
		pt.dLat += nPosY;

		mr.longi = pt.dLon;
		mr.lati = pt.dLat;

		return;
	}

}
