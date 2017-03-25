/**
 * FaultSerivcesSoap_PortType.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.4 Apr 22, 2006 (06:55:48 PDT) WSDL2Java emitter.
 */

package parser.webservice.js.cu;

public interface FaultSerivcesSoap_PortType extends java.rmi.Remote {

	/**
	 * 获取一定半径内的所有投诉点信息
	 */
	public java.lang.String historyComplain(double longitude, double latitude, double radius) throws java.rmi.RemoteException;

	/**
	 * 获取最新告警信息
	 */
	public java.lang.String getNewAlarms() throws java.rmi.RemoteException;

	/**
	 * 获取已清理的告警信息
	 */
	public java.lang.String getClearedAlarms() throws java.rmi.RemoteException;
}
