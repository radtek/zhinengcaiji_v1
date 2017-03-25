/**
 * FaultSerivces.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.4 Apr 22, 2006 (06:55:48 PDT) WSDL2Java emitter.
 */

package parser.webservice.js.cu;

public interface FaultSerivces extends javax.xml.rpc.Service {

	public java.lang.String getFaultSerivcesSoap12Address();

	public parser.webservice.js.cu.FaultSerivcesSoap_PortType getFaultSerivcesSoap12() throws javax.xml.rpc.ServiceException;

	public parser.webservice.js.cu.FaultSerivcesSoap_PortType getFaultSerivcesSoap12(java.net.URL portAddress) throws javax.xml.rpc.ServiceException;

	public java.lang.String getFaultSerivcesSoapAddress();

	public parser.webservice.js.cu.FaultSerivcesSoap_PortType getFaultSerivcesSoap() throws javax.xml.rpc.ServiceException;

	public parser.webservice.js.cu.FaultSerivcesSoap_PortType getFaultSerivcesSoap(java.net.URL portAddress) throws javax.xml.rpc.ServiceException;
}
