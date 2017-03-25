package util.mr;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import framework.ConstDef;
import framework.SystemConfig;

public class CMRLocation {

	// MR 采集源，包括中创与中兴等格式
	public int nMR_SourceMode = ConstDef.MR_SOURCE_ZCTT;

	public String strMR_InputFileName = null;

	private CMRCalculate pMRCalculate = new CMRCalculate();

	private MapMRADJs[] pMapMRAdjs = new MapMRADJs[100];

	// 写文件内容方式: 新建文件(false),追究内容到文件内容最后(true)
	private int contextAppendtype;

	// 定位文件拆分方式 0(不对内容进行拆分) 1(对定位后文件的MR内容进行按1小时拆分)
	private int filesplittype;

	private LocalorFileSplitAdapt filesplitadapt;

	public CMRLocation(int nMRSource) {
		nMR_SourceMode = nMRSource;

	}

	public int create() {
		int nID = pMRCalculate.getHandle();

		if (nID != -1) {
			for (int i = 0; i < 100; i++) {
				if (pMapMRAdjs[i] == null) {
					pMapMRAdjs[i] = new MapMRADJs();
					break;
				} else
					continue;
			}
		}

		return nID;
	}

	public void destroy(int nHandle) {
		pMRCalculate.destroyHandle(nHandle);

		if (pMapMRAdjs[nHandle] != null) {
			pMapMRAdjs[nHandle] = null;
		}
	}

	public int readSiteDatabase(String szFileName) {
		return pMRCalculate.getSiteDatabase(szFileName);
	}

	public int processMRLocation(int nHandle, String szOpenFileName, String szSaveFileName, boolean bLoc, ILocator.LocationInfo info) {
		strMR_InputFileName = szOpenFileName;

		BufferedReader br;
		BufferedWriter bw;
		try {
			br = new BufferedReader(new FileReader(szOpenFileName));
		} catch (Exception e) {
			System.out.println(szOpenFileName + " 没有打开");
			return LocComm.VALUE_FAILED_FILE_MRINPUT_OPEN;
		}

		try {
			// 增加了如果定位文件已经存在,并且需要以追究方式写文件时则先判断文件是否存在,文件存在则以追加方式写文件,需要移到filesplitadapt内处理
			// File file=new File(szSaveFileName);
			// if (file.exists() && file.isFile() && file.canWrite() &&
			// this.contextAppendtype==1)
			// bw = new BufferedWriter(new FileWriter(szSaveFileName,true));
			// else
			// bw = new BufferedWriter(new FileWriter(szSaveFileName));
			// 拆分文件适配器
			filesplitadapt = new LocalorFileSplitAdapt(this.filesplittype);

			filesplitadapt.setFileprefix(szSaveFileName.substring(0, szSaveFileName.lastIndexOf("_")));
			filesplitadapt.setContextAppendtype(this.contextAppendtype);
			filesplitadapt.setSourceSaveFileName(szSaveFileName);
			filesplitadapt.initsourcebw(szSaveFileName);
			bw = filesplitadapt.getSourcebw();

		} catch (Exception e) {
			System.out.println(szSaveFileName + " 没有打开");
			return LocComm.VALUE_FAILED_FILE_MROUT_OPEN;
		}

		try {
			// 跳过文件头
			br.readLine();

			if (nMR_SourceMode == ConstDef.MR_SOURCE_ZTE1 || nMR_SourceMode == ConstDef.MR_SOURCE_ZTE2) {
				br.readLine();
			}

			MR mr = new MR();

			if (nMR_SourceMode == ConstDef.MR_SOURCE_MOTO) {
				int nIndex = szOpenFileName.lastIndexOf(File.separator);

				String fileName = szOpenFileName.substring(nIndex + 1);

				String[] sa = fileName.split("_");

				mr.lac = Integer.parseInt(sa[0]);
				mr.ci = Integer.parseInt(sa[1]);
			}

			if (SystemConfig.getInstance().isMRFast()) {
				info.nSrcMRCount = 0;
				info.nLocatedCount = 0;
				int readReturn = getOneMRInit(br, mr, bLoc);
				while (readReturn != LocComm.VALUE_FILE_END /* && nCount < 99999 */) {
					info.nSrcMRCount++;
					if (readReturn != LocComm.VALUE_LONLAT_NOVALID) {
						info.nLocatedCount++;
						try {
							getOneMRLocation(nHandle, mr, bLoc);

							saveOneMRtoFile(bw, mr);
						} catch (Exception e) {
							// 捕获一条mr问题，继续
							e.printStackTrace();
						}
					}

					readReturn = getOneMRInit(br, mr, bLoc);
				}
			} else {
				MapMRADJs pMapADJ = pMapMRAdjs[nHandle];

				info.nSrcMRCount = 0;
				info.nLocatedCount = 0;
				int readReturn = getOneMRInit(br, mr, bLoc);
				while (readReturn != LocComm.VALUE_FILE_END /* && nCount < 99999 */) {
					info.nSrcMRCount++;
					if (readReturn != LocComm.VALUE_LONLAT_NOVALID) {
						info.nLocatedCount++;
						try {
							getOneMRLocation(nHandle, mr, bLoc);

							int lacci = mr.lac << 16 | mr.ci;
							int trxts = (mr.TRX << 8 | mr.time_slot);

							MapTrxTSMR pTrxTs = pMapADJ.m.get(lacci);

							if (pTrxTs != null) {

								MR_ADJ pMRAdj = pTrxTs.m.get(trxts);
								if (pMRAdj != null) {
									pMRAdj.nCount++;

									pMRAdj.nIndex %= 15;

									if (pMRAdj.nCount > 15) {
										int zero = 0;
										double longi_ave = 0, lati_ave = 0;
										for (int addIndex = 1; addIndex < 15; addIndex++) {
											int nID = (pMRAdj.nIndex + addIndex) % 15;

											if (pMRAdj.mrs[nID].longi > 0 && pMRAdj.mrs[nID].longi < 180) {
												longi_ave += pMRAdj.mrs[nID].longi;
												lati_ave += pMRAdj.mrs[nID].lati;
											} else
												zero++;
										}

										if (zero < 14) {
											pMRAdj.mrs[pMRAdj.nIndex].longi = longi_ave / (14 - zero);
											pMRAdj.mrs[pMRAdj.nIndex].lati = lati_ave / (14 - zero);
										}

										// System.out.println(pMRAdj.nIndex+"="+pMRAdj.mrs[pMRAdj.nIndex].longi+"--"+pMRAdj.mrs[pMRAdj.nIndex].ci);

										saveOneMRtoFile(bw, pMRAdj.mrs[pMRAdj.nIndex]);

										pMRAdj.mrs[pMRAdj.nIndex] = (MR) mr.clone();

										pMRAdj.nIndex++;
									} else {
										pMRAdj.mrs[pMRAdj.nCount - 1] = (MR) mr.clone();
									}
								} else {
									MR_ADJ mrAdj = new MR_ADJ();
									mrAdj.mrs[mrAdj.nIndex] = (MR) mr.clone();
									mrAdj.nCount = 1;

									pTrxTs.m.put(trxts, mrAdj);
								}
							} else {
								pTrxTs = new MapTrxTSMR();
								MR_ADJ mrAdj = new MR_ADJ();
								mrAdj.mrs[mrAdj.nIndex] = (MR) mr.clone();
								mrAdj.nCount = 1;

								pTrxTs.m.put(trxts, mrAdj);
								pMapADJ.m.put(lacci, pTrxTs);
							}
						} catch (Exception e) {
							// 捕获一条mr问题，继续
							e.printStackTrace();
						}
					}

					readReturn = getOneMRInit(br, mr, bLoc);
				}

				for (MapTrxTSMR pTrxTs : pMapADJ.m.values()) {

					for (MR_ADJ pMRAdj : pTrxTs.m.values()) {

						int nSize;
						if (pMRAdj.nCount > 15) {
							nSize = 15;
						} else {
							nSize = pMRAdj.nCount;
						}

						for (int addIndex = 0; addIndex < nSize; addIndex++) {
							int nID = (pMRAdj.nIndex + addIndex) % 15;

							if (pMRAdj.mrs[nID].longi > 0 && pMRAdj.mrs[nID].longi < 180) {
								saveOneMRtoFile(bw, pMRAdj.mrs[nID]);
							}
						}

					}
				}
			}

			br.close();
			bw.close();
		} catch (Exception e) {
			e.printStackTrace();
			return LocComm.VALUE_LONLAT_NOVALID;
		} finally {
			filesplitadapt.destory();
		}
		return LocComm.VALUE_NORMAL;
	}

	private int getOneMRInit(BufferedReader br, MR mr, boolean loc) {
		switch (nMR_SourceMode) {
			case ConstDef.MR_SOURCE_ZCTT :
				return getOneMRInitZCTT(br, mr, loc);
			case ConstDef.MR_SOURCE_ZTE1 :
				return getOneMRInitZTE1(br, mr, loc);
			case ConstDef.MR_SOURCE_ZTE2 :
				return getOneMRInitZTE2(br, mr, loc);
			case ConstDef.MR_SOURCE_MOTO :
				return getOneMRInitMOTO(br, mr, loc);
		}
		return LocComm.VALUE_LONLAT_NOVALID;
	}

	/**
	 * 中创读取一行
	 * 
	 * @param br
	 * @param mr
	 * @param bLoc
	 * @return
	 */
	final int getOneMRInitZCTT(BufferedReader br, MR mr, boolean bLoc) {

		try {
			String s = br.readLine();

			if (s == null) {
				return LocComm.VALUE_FILE_END;
			}

			String[] sa = s.split("\t+");

			if (bLoc && sa.length != 50) {
				return LocComm.VALUE_LONLAT_NOVALID;
			} else if (!bLoc && sa.length != 52) {
				return LocComm.VALUE_LONLAT_NOVALID;
			}

			mr.report_date = sa[0];
			mr.report_time = sa[1];
			mr.CGI = sa[2];

			mr.TRX = Integer.parseInt(sa[3]);
			mr.time_slot = Integer.parseInt(sa[4]);
			mr.result_num = Integer.parseInt(sa[5]);
			mr.is_valid = Integer.parseInt(sa[6]);
			mr.has_next = Integer.parseInt(sa[7]);
			mr.channel_type = Integer.parseInt(sa[8]);
			mr.DTXd = Integer.parseInt(sa[9]);
			mr.RXLEV_FULL_up = Integer.parseInt(sa[10]);
			mr.RXLEV_SUB_up = Integer.parseInt(sa[11]);
			mr.RXQUAL_FULL_up = Integer.parseInt(sa[12]);
			mr.RXQUAL_SUB_up = Integer.parseInt(sa[13]);
			mr.Power_Level = Integer.parseInt(sa[14]);
			mr.MS_Power_Level = Integer.parseInt(sa[15]);
			mr.Timing_Advance = Integer.parseInt(sa[16]);
			mr.BA_USED = Integer.parseInt(sa[17]);
			mr.DTX_USED = Integer.parseInt(sa[18]);
			mr.RXLEV_FULL_SERVING = Integer.parseInt(sa[19]);
			mr.BA_USED_3G = Integer.parseInt(sa[20]);
			mr.MEAS_VALID = Integer.parseInt(sa[21]);
			mr.RXLEV_SUB_SERVING = Integer.parseInt(sa[22]);
			mr.RXQUAL_FULL_SERVING = Integer.parseInt(sa[23]);
			mr.RXQUAL_SUB_SERVING = Integer.parseInt(sa[24]);
			mr.neib_num = Integer.parseInt(sa[25]);
			mr.neib_ci[0].Rxlev = Integer.parseInt(sa[26]);
			mr.neib_ci[0].bcch = Integer.parseInt(sa[27]);
			mr.neib_ci[0].NCC = Integer.parseInt(sa[28]);
			mr.neib_ci[0].BCC = Integer.parseInt(sa[29]);
			mr.neib_ci[1].Rxlev = Integer.parseInt(sa[30]);
			mr.neib_ci[1].bcch = Integer.parseInt(sa[31]);
			mr.neib_ci[1].NCC = Integer.parseInt(sa[32]);
			mr.neib_ci[1].BCC = Integer.parseInt(sa[33]);
			mr.neib_ci[2].Rxlev = Integer.parseInt(sa[34]);
			mr.neib_ci[2].bcch = Integer.parseInt(sa[35]);
			mr.neib_ci[2].NCC = Integer.parseInt(sa[36]);
			mr.neib_ci[2].BCC = Integer.parseInt(sa[37]);
			mr.neib_ci[3].Rxlev = Integer.parseInt(sa[38]);
			mr.neib_ci[3].bcch = Integer.parseInt(sa[39]);
			mr.neib_ci[3].NCC = Integer.parseInt(sa[40]);
			mr.neib_ci[3].BCC = Integer.parseInt(sa[41]);
			mr.neib_ci[4].Rxlev = Integer.parseInt(sa[42]);
			mr.neib_ci[4].bcch = Integer.parseInt(sa[43]);
			mr.neib_ci[4].NCC = Integer.parseInt(sa[44]);
			mr.neib_ci[4].BCC = Integer.parseInt(sa[45]);
			mr.neib_ci[5].Rxlev = Integer.parseInt(sa[46]);
			mr.neib_ci[5].bcch = Integer.parseInt(sa[47]);
			mr.neib_ci[5].NCC = Integer.parseInt(sa[48]);
			mr.neib_ci[5].BCC = Integer.parseInt(sa[49]);

			if (!bLoc) {

				mr.longi = Double.parseDouble(sa[50]);
				mr.lati = Double.parseDouble(sa[51]);

			}

			String[] sa1 = mr.CGI.split("-");

			mr.lac = Integer.parseInt(sa1[2]);
			mr.ci = Integer.parseInt(sa1[3]);
		} catch (Exception e) {
			e.printStackTrace();
			return LocComm.VALUE_LONLAT_NOVALID;
		}

		if (mr.result_num == 255 || mr.MEAS_VALID == 255 || mr.MEAS_VALID == 1 || mr.neib_num > 7)
			return LocComm.VALUE_LONLAT_NOVALID;

		if (mr.has_next == 0 || mr.neib_num == 7 || mr.neib_num == 0) {
			mr.neib_num = 0;
			for (int i = 0; i < 6; i++) {
				mr.neib_ci[i].bsic = 255;
			}
		} else {
			for (int i = 0; i < mr.neib_num; i++) {
				mr.neib_ci[i].bsic = (mr.neib_ci[i].NCC * 10) + mr.neib_ci[i].BCC;
			}
		}

		if (!pMRCalculate.bMRCellExist(mr)) {
			return LocComm.VALUE_LONLAT_NOVALID;
		}

		return LocComm.VALUE_NORMAL;
	}

	/**
	 * 中兴读取1
	 * 
	 * @param br
	 * @param mr
	 * @param bLoc
	 * @return
	 */
	final int getOneMRInitZTE1(BufferedReader br, MR mr, boolean bLoc) {

		try {
			String s = br.readLine();

			if (s == null) {
				return LocComm.VALUE_FILE_END;
			}

			String[] sa = s.split(",");

			if (sa.length != 45) {
				return LocComm.VALUE_LONLAT_NOVALID;
			}

			mr.report_date = sa[0];

			mr.lac = Integer.parseInt(sa[4]);
			mr.ci = Integer.parseInt(sa[5]);
			mr.TRX = Integer.parseInt(sa[6]);
			mr.channel_type = Integer.parseInt(sa[7]);
			mr.time_slot = Integer.parseInt(sa[8]);
			mr.result_num = Integer.parseInt(sa[9]);
			mr.MEAS_VALID = Integer.parseInt(sa[10]);
			mr.DTXd = Integer.parseInt(sa[11]);
			mr.DTX_USED = Integer.parseInt(sa[12]);

			mr.RXLEV_FULL_up = Integer.parseInt(sa[14]) - 110;
			mr.RXLEV_SUB_up = Integer.parseInt(sa[15]) - 110;
			mr.RXLEV_FULL_SERVING = Integer.parseInt(sa[16]) - 110;
			mr.RXLEV_SUB_SERVING = Integer.parseInt(sa[17]) - 110;
			mr.RXQUAL_FULL_up = Integer.parseInt(sa[18]);
			mr.RXQUAL_SUB_up = Integer.parseInt(sa[19]);
			mr.RXQUAL_FULL_SERVING = Integer.parseInt(sa[20]);
			mr.RXQUAL_SUB_SERVING = Integer.parseInt(sa[21]);
			mr.Power_Level = Integer.parseInt(sa[22]);
			mr.MS_Power_Level = Integer.parseInt(sa[23]);
			mr.Timing_Advance = Integer.parseInt(sa[24]);
			mr.BA_USED = Integer.parseInt(sa[25]);
			mr.neib_num = Integer.parseInt(sa[26]);

			int nSubIndex = 27;
			for (int i = 0; i < 6; i++) {
				mr.neib_ci[i].bsic = Integer.parseInt(sa[nSubIndex++]);
				mr.neib_ci[i].bcch = Integer.parseInt(sa[nSubIndex++]);
				mr.neib_ci[i].Rxlev = Integer.parseInt(sa[nSubIndex++]) - 110;
			}

		} catch (Exception e) {
			e.printStackTrace();
			return LocComm.VALUE_LONLAT_NOVALID;
		}

		if (mr.result_num == 255 || mr.MEAS_VALID == 255 || mr.MEAS_VALID == 1 || mr.neib_num > 7)
			return LocComm.VALUE_LONLAT_NOVALID;

		if (mr.neib_num == 7)
			mr.neib_num = 0;

		if (!pMRCalculate.bMRCellExist(mr)) {
			return LocComm.VALUE_LONLAT_NOVALID;
		}

		return LocComm.VALUE_NORMAL;
	}

	/**
	 * 中兴读取2
	 * 
	 * @param br
	 * @param mr
	 * @param bLoc
	 * @return
	 */
	final int getOneMRInitZTE2(BufferedReader br, MR mr, boolean bLoc) {

		try {
			String s = br.readLine();

			if (s == null) {
				return LocComm.VALUE_FILE_END;
			}

			String[] sa = s.split(",");

			if (sa.length != 48) {
				return LocComm.VALUE_LONLAT_NOVALID;
			}

			mr.report_date = sa[0];

			mr.lac = Integer.parseInt(sa[4 + 3]);
			mr.ci = Integer.parseInt(sa[5 + 3]);
			mr.TRX = Integer.parseInt(sa[6 + 3]);
			mr.channel_type = Integer.parseInt(sa[7 + 3]);
			mr.time_slot = Integer.parseInt(sa[8 + 3]);
			mr.result_num = Integer.parseInt(sa[9 + 3]);
			mr.MEAS_VALID = Integer.parseInt(sa[10 + 3]);
			mr.DTXd = Integer.parseInt(sa[11 + 3]);
			mr.DTX_USED = Integer.parseInt(sa[12 + 3]);

			mr.RXLEV_FULL_up = Integer.parseInt(sa[14 + 3]) - 110;
			mr.RXLEV_SUB_up = Integer.parseInt(sa[15 + 3]) - 110;
			mr.RXLEV_FULL_SERVING = Integer.parseInt(sa[16 + 3]) - 110;
			mr.RXLEV_SUB_SERVING = Integer.parseInt(sa[17 + 3]) - 110;
			mr.RXQUAL_FULL_up = Integer.parseInt(sa[18 + 3]);
			mr.RXQUAL_SUB_up = Integer.parseInt(sa[19 + 3]);
			mr.RXQUAL_FULL_SERVING = Integer.parseInt(sa[20 + 3]);
			mr.RXQUAL_SUB_SERVING = Integer.parseInt(sa[21 + 3]);
			mr.Power_Level = Integer.parseInt(sa[22 + 3]);
			mr.MS_Power_Level = Integer.parseInt(sa[23 + 3]);
			mr.Timing_Advance = Integer.parseInt(sa[24 + 3]);
			mr.BA_USED = Integer.parseInt(sa[25 + 3]);
			mr.neib_num = Integer.parseInt(sa[26 + 3]);

			int nSubIndex = 27 + 3;
			for (int i = 0; i < 6; i++) {
				mr.neib_ci[i].bsic = Integer.parseInt(sa[nSubIndex++]);
				mr.neib_ci[i].bcch = Integer.parseInt(sa[nSubIndex++]);
				mr.neib_ci[i].Rxlev = Integer.parseInt(sa[nSubIndex++]) - 110;
			}

		} catch (Exception e) {
			e.printStackTrace();
			return LocComm.VALUE_LONLAT_NOVALID;
		}

		if (mr.result_num == 255 || mr.MEAS_VALID == 255 || mr.MEAS_VALID == 1 || mr.neib_num > 7)
			return LocComm.VALUE_LONLAT_NOVALID;

		if (mr.neib_num == 7)
			mr.neib_num = 0;

		if (!pMRCalculate.bMRCellExist(mr)) {
			return LocComm.VALUE_LONLAT_NOVALID;
		}

		return LocComm.VALUE_NORMAL;
	}

	/**
	 * 摩托读取
	 * 
	 * @param br
	 * @param mr
	 * @param bLoc
	 * @return
	 */
	final int getOneMRInitMOTO(BufferedReader br, MR mr, boolean bLoc) {
		try {
			String s = br.readLine();

			if (s == null) {
				return LocComm.VALUE_FILE_END;
			}

			String[] sa = s.split(",");

			mr.TRX = Integer.parseInt(sa[2]);

			Date date = new Date(((long) Double.parseDouble(sa[5])) * 1000);
			SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			format.setTimeZone(TimeZone.getTimeZone("GMT"));
			mr.report_date = format.format(date);

			mr.time_slot = Integer.parseInt(sa[6]);
			mr.result_num = Integer.parseInt(sa[7]);
			mr.RXLEV_SUB_up = Integer.parseInt(sa[8]) - 110;
			mr.RXQUAL_SUB_up = Integer.parseInt(sa[9]);
			mr.Power_Level = Integer.parseInt(sa[11]);
			mr.MS_Power_Level = Integer.parseInt(sa[14]);
			mr.Timing_Advance = Integer.parseInt(sa[15]);
			mr.MEAS_VALID = Integer.parseInt(sa[19]);
			mr.RXLEV_SUB_SERVING = Integer.parseInt(sa[16]) - 110;
			mr.RXQUAL_SUB_SERVING = Integer.parseInt(sa[17]);
			mr.neib_num = Integer.parseInt(sa[32]);

			int nSubIndex = 33;
			for (int i = 0; i < mr.neib_num; i++) {
				mr.neib_ci[i].Rxlev = Integer.parseInt(sa[nSubIndex++]) - 110;
				mr.neib_ci[i].bcch = Integer.parseInt(sa[nSubIndex++]);
				nSubIndex++; // Neib Index
				mr.neib_ci[i].bsic = Integer.parseInt(sa[nSubIndex++]);
			}
		} catch (Exception e) {
			e.printStackTrace();
			return LocComm.VALUE_LONLAT_NOVALID;
		}

		if (mr.result_num == 255 || mr.MEAS_VALID == 255 || mr.MEAS_VALID == 1 || mr.neib_num > 7)
			return LocComm.VALUE_LONLAT_NOVALID;

		if (mr.neib_num == 7)
			mr.neib_num = 0;

		if (!pMRCalculate.bMRCellExist(mr)) {
			return LocComm.VALUE_LONLAT_NOVALID;
		}

		return LocComm.VALUE_NORMAL;
	}

	final int getOneMRLocation(int nHandle, MR mr, boolean bLoc) {
		return pMRCalculate.processMR(nHandle, mr, bLoc);
	}

	final int saveOneMRtoFile(BufferedWriter bw, MR mr) {
		if (filesplitadapt != null)
			bw = filesplitadapt.getSplitCalculateBw(mr.report_date);
		switch (nMR_SourceMode) {
			case ConstDef.MR_SOURCE_ZCTT :
				return saveOneMRtoFileZCTT(bw, mr);
			case ConstDef.MR_SOURCE_ZTE1 :
			case ConstDef.MR_SOURCE_ZTE2 :
				return saveOneMRtoFileZTE(bw, mr);
			case ConstDef.MR_SOURCE_MOTO :
				return saveOneMRtoFileMOTO(bw, mr);
		}
		return LocComm.VALUE_NORMAL;
	}

	final int saveOneMRtoFileMOTO(BufferedWriter bw, MR mr) {
		try {
			String s = "0;" + mr.report_date + ";" + mr.lac + ";" + mr.ci + ";" + mr.TRX + ";" + mr.time_slot + ";" + mr.result_num + ";;;"
					+ mr.RXLEV_SUB_up + ";;" + mr.RXQUAL_SUB_up + ";" + mr.Power_Level + ";" + mr.MS_Power_Level + ";" + mr.Timing_Advance + ";;"
					+ mr.MEAS_VALID + ";" + mr.RXLEV_SUB_SERVING + ";;;" + mr.RXQUAL_SUB_SERVING + ";" + mr.decline_diff + ";" + mr.neib_num + ";";

			for (int i = 0; i < 6; i++) {
				if (i < mr.neib_num)
					s += mr.neib_ci[i].Rxlev + ";" + mr.neib_ci[i].bcch + ";" + mr.neib_ci[i].bsic + ";";
				else
					s += ";;;";
			}

			s += mr.longi + ";" + mr.lati + ";" + mr.city_id + ";" + mr.ne_sys_id + "\n";

			bw.write(s);
		} catch (IOException e) {
			e.printStackTrace();
		}

		return LocComm.VALUE_NORMAL;
	}

	final int saveOneMRtoFileZTE(BufferedWriter bw, MR mr) {
		try {
			String s = "0;" + mr.report_date + ";" + mr.lac + ";" + mr.ci + ";" + mr.TRX + ";" + mr.time_slot + ";" + mr.result_num + ";"
					+ mr.channel_type + ";" + mr.RXLEV_FULL_up + ";" + mr.RXLEV_SUB_up + ";" + mr.RXQUAL_FULL_up + ";" + mr.RXQUAL_SUB_up + ";"
					+ mr.Power_Level + ";" + mr.MS_Power_Level + ";" + mr.Timing_Advance + ";" + mr.BA_USED + ";" + mr.MEAS_VALID + ";"
					+ mr.RXLEV_SUB_SERVING + ";" + mr.RXLEV_FULL_SERVING + ";" + mr.RXQUAL_FULL_SERVING + ";" + mr.RXQUAL_SUB_SERVING + ";"
					+ mr.decline_diff + ";" + mr.neib_num + ";" + mr.neib_ci[0].Rxlev + ";" + mr.neib_ci[0].bcch + ";" + mr.neib_ci[0].bsic + ";"
					+ mr.neib_ci[1].Rxlev + ";" + mr.neib_ci[1].bcch + ";" + mr.neib_ci[1].bsic + ";" + mr.neib_ci[2].Rxlev + ";"
					+ mr.neib_ci[2].bcch + ";" + mr.neib_ci[2].bsic + ";" + mr.neib_ci[3].Rxlev + ";" + mr.neib_ci[3].bcch + ";" + mr.neib_ci[3].bsic
					+ ";" + mr.neib_ci[4].Rxlev + ";" + mr.neib_ci[4].bcch + ";" + mr.neib_ci[4].bsic + ";" + mr.neib_ci[5].Rxlev + ";"
					+ mr.neib_ci[5].bcch + ";" + mr.neib_ci[5].bsic + ";" + mr.longi + ";" + mr.lati + ";" + mr.city_id + ";" + mr.ne_sys_id + "\n";

			bw.write(s);
		} catch (IOException e) {
			e.printStackTrace();
		}

		return LocComm.VALUE_NORMAL;
	}

	final int saveOneMRtoFileZCTT(BufferedWriter bw, MR mr) {
		int year, month, day, hour, minute, second;

		try {
			String[] sadate = mr.report_date.split("/");
			String[] satime = mr.report_time.split("[:\\.]");

			year = Integer.parseInt(sadate[0]);
			month = Integer.parseInt(sadate[1]);
			day = Integer.parseInt(sadate[2]);
			hour = Integer.parseInt(satime[0]);
			minute = Integer.parseInt(satime[1]);
			second = Integer.parseInt(satime[2]);

		} catch (Exception e) {
			return LocComm.VALUE_NORMAL;
		}

		try {
			String s = "0;" + year + "-" + month + "-" + day + " " + hour + ":" + minute + ":" + second + ";" + mr.lac + ";" + mr.ci + ";" + mr.TRX
					+ ";" + mr.time_slot + ";" + mr.result_num + ";" + mr.channel_type + ";" + mr.RXLEV_FULL_up + ";" + mr.RXLEV_SUB_up + ";"
					+ mr.RXQUAL_FULL_up + ";" + mr.RXQUAL_SUB_up + ";" + mr.Power_Level + ";" + mr.MS_Power_Level + ";" + mr.Timing_Advance + ";"
					+ mr.BA_USED + ";" + mr.MEAS_VALID + ";" + mr.RXLEV_SUB_SERVING + ";" + mr.RXLEV_FULL_SERVING + ";" + mr.RXQUAL_FULL_SERVING
					+ ";" + mr.RXQUAL_SUB_SERVING + ";" + mr.decline_diff + ";" + mr.neib_num + ";" + mr.neib_ci[0].Rxlev + ";" + mr.neib_ci[0].bcch
					+ ";" + mr.neib_ci[0].bsic + ";" + mr.neib_ci[1].Rxlev + ";" + mr.neib_ci[1].bcch + ";" + mr.neib_ci[1].bsic + ";"
					+ mr.neib_ci[2].Rxlev + ";" + mr.neib_ci[2].bcch + ";" + mr.neib_ci[2].bsic + ";" + mr.neib_ci[3].Rxlev + ";"
					+ mr.neib_ci[3].bcch + ";" + mr.neib_ci[3].bsic + ";" + mr.neib_ci[4].Rxlev + ";" + mr.neib_ci[4].bcch + ";" + mr.neib_ci[4].bsic
					+ ";" + mr.neib_ci[5].Rxlev + ";" + mr.neib_ci[5].bcch + ";" + mr.neib_ci[5].bsic + ";" + mr.longi + ";" + mr.lati + ";"
					+ mr.city_id + ";" + mr.ne_sys_id + "\n";

			bw.write(s);
		} catch (IOException e) {
			e.printStackTrace();
		}

		return LocComm.VALUE_NORMAL;
	}

	public int getContextAppendtype() {
		return contextAppendtype;
	}

	public void setContextAppendtype(int contextAppendtype) {
		this.contextAppendtype = contextAppendtype;
	}

	public int getFilesplittype() {
		return filesplittype;
	}

	public void setFilesplittype(int filesplittype) {
		this.filesplittype = filesplittype;
	}

	public LocalorFileSplitAdapt getFilesplitadapt() {
		return filesplitadapt;
	}

	public void setFilesplitadapt(LocalorFileSplitAdapt filesplitadapt) {
		this.filesplitadapt = filesplitadapt;
	}

}
