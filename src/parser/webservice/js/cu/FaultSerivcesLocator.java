/**
 * FaultSerivcesLocator.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.4 Apr 22, 2006 (06:55:48 PDT) WSDL2Java emitter.
 */

package parser.webservice.js.cu;

public class FaultSerivcesLocator extends org.apache.axis.client.Service implements parser.webservice.js.cu.FaultSerivces {

	public FaultSerivcesLocator() {
	}

	public FaultSerivcesLocator(org.apache.axis.EngineConfiguration config) {
		super(config);
	}

	public FaultSerivcesLocator(java.lang.String wsdlLoc, javax.xml.namespace.QName sName) throws javax.xml.rpc.ServiceException {
		super(wsdlLoc, sName);
	}

	// Use to get a proxy class for FaultSerivcesSoap12
	private java.lang.String FaultSerivcesSoap12_address = "http://10.12.1.21:8088/cssp2/ty/Services/FaultSerivces.asmx";

	public java.lang.String getFaultSerivcesSoap12Address() {
		return FaultSerivcesSoap12_address;
	}

	// The WSDD service name defaults to the port name.
	private java.lang.String FaultSerivcesSoap12WSDDServiceName = "FaultSerivcesSoap12";

	public java.lang.String getFaultSerivcesSoap12WSDDServiceName() {
		return FaultSerivcesSoap12WSDDServiceName;
	}

	public void setFaultSerivcesSoap12WSDDServiceName(java.lang.String name) {
		FaultSerivcesSoap12WSDDServiceName = name;
	}

	public parser.webservice.js.cu.FaultSerivcesSoap_PortType getFaultSerivcesSoap12() throws javax.xml.rpc.ServiceException {
		java.net.URL endpoint;
		try {
			endpoint = new java.net.URL(FaultSerivcesSoap12_address);
		} catch (java.net.MalformedURLException e) {
			throw new javax.xml.rpc.ServiceException(e);
		}
		return getFaultSerivcesSoap12(endpoint);
	}

	public parser.webservice.js.cu.FaultSerivcesSoap_PortType getFaultSerivcesSoap12(java.net.URL portAddress) throws javax.xml.rpc.ServiceException {
		try {
			parser.webservice.js.cu.FaultSerivcesSoap12Stub _stub = new parser.webservice.js.cu.FaultSerivcesSoap12Stub(portAddress, this);
			_stub.setPortName(getFaultSerivcesSoap12WSDDServiceName());
			return _stub;
		} catch (org.apache.axis.AxisFault e) {
			return null;
		}
	}

	public void setFaultSerivcesSoap12EndpointAddress(java.lang.String address) {
		FaultSerivcesSoap12_address = address;
	}

	// Use to get a proxy class for FaultSerivcesSoap
	private java.lang.String FaultSerivcesSoap_address = "http://10.12.1.21:8088/cssp2/ty/Services/FaultSerivces.asmx";

	public java.lang.String getFaultSerivcesSoapAddress() {
		return FaultSerivcesSoap_address;
	}

	// The WSDD service name defaults to the port name.
	private java.lang.String FaultSerivcesSoapWSDDServiceName = "FaultSerivcesSoap";

	public java.lang.String getFaultSerivcesSoapWSDDServiceName() {
		return FaultSerivcesSoapWSDDServiceName;
	}

	public void setFaultSerivcesSoapWSDDServiceName(java.lang.String name) {
		FaultSerivcesSoapWSDDServiceName = name;
	}

	public parser.webservice.js.cu.FaultSerivcesSoap_PortType getFaultSerivcesSoap() throws javax.xml.rpc.ServiceException {
		java.net.URL endpoint;
		try {
			endpoint = new java.net.URL(FaultSerivcesSoap_address);
		} catch (java.net.MalformedURLException e) {
			throw new javax.xml.rpc.ServiceException(e);
		}
		return getFaultSerivcesSoap(endpoint);
	}

	public parser.webservice.js.cu.FaultSerivcesSoap_PortType getFaultSerivcesSoap(java.net.URL portAddress) throws javax.xml.rpc.ServiceException {
		try {
			parser.webservice.js.cu.FaultSerivcesSoap_BindingStub _stub = new parser.webservice.js.cu.FaultSerivcesSoap_BindingStub(portAddress, this);
			_stub.setPortName(getFaultSerivcesSoapWSDDServiceName());
			return _stub;
		} catch (org.apache.axis.AxisFault e) {
			return null;
		}
	}

	public void setFaultSerivcesSoapEndpointAddress(java.lang.String address) {
		FaultSerivcesSoap_address = address;
	}

	/**
	 * For the given interface, get the stub implementation. If this service has no port for the given interface, then ServiceException is thrown.
	 * This service has multiple ports for a given interface; the proxy implementation returned may be indeterminate.
	 */
	public java.rmi.Remote getPort(Class serviceEndpointInterface) throws javax.xml.rpc.ServiceException {
		try {
			if (parser.webservice.js.cu.FaultSerivcesSoap_PortType.class.isAssignableFrom(serviceEndpointInterface)) {
				parser.webservice.js.cu.FaultSerivcesSoap12Stub _stub = new parser.webservice.js.cu.FaultSerivcesSoap12Stub(new java.net.URL(
						FaultSerivcesSoap12_address), this);
				_stub.setPortName(getFaultSerivcesSoap12WSDDServiceName());
				return _stub;
			}
			if (parser.webservice.js.cu.FaultSerivcesSoap_PortType.class.isAssignableFrom(serviceEndpointInterface)) {
				parser.webservice.js.cu.FaultSerivcesSoap_BindingStub _stub = new parser.webservice.js.cu.FaultSerivcesSoap_BindingStub(
						new java.net.URL(FaultSerivcesSoap_address), this);
				_stub.setPortName(getFaultSerivcesSoapWSDDServiceName());
				return _stub;
			}
		} catch (java.lang.Throwable t) {
			throw new javax.xml.rpc.ServiceException(t);
		}
		throw new javax.xml.rpc.ServiceException("There is no stub implementation for the interface:  "
				+ (serviceEndpointInterface == null ? "null" : serviceEndpointInterface.getName()));
	}

	/**
	 * For the given interface, get the stub implementation. If this service has no port for the given interface, then ServiceException is thrown.
	 */
	public java.rmi.Remote getPort(javax.xml.namespace.QName portName, Class serviceEndpointInterface) throws javax.xml.rpc.ServiceException {
		if (portName == null) {
			return getPort(serviceEndpointInterface);
		}
		java.lang.String inputPortName = portName.getLocalPart();
		if ("FaultSerivcesSoap12".equals(inputPortName)) {
			return getFaultSerivcesSoap12();
		} else if ("FaultSerivcesSoap".equals(inputPortName)) {
			return getFaultSerivcesSoap();
		} else {
			java.rmi.Remote _stub = getPort(serviceEndpointInterface);
			((org.apache.axis.client.Stub) _stub).setPortName(portName);
			return _stub;
		}
	}

	public javax.xml.namespace.QName getServiceName() {
		return new javax.xml.namespace.QName("http://tempuri.org/", "FaultSerivces");
	}

	private java.util.HashSet ports = null;

	public java.util.Iterator getPorts() {
		if (ports == null) {
			ports = new java.util.HashSet();
			ports.add(new javax.xml.namespace.QName("http://tempuri.org/", "FaultSerivcesSoap12"));
			ports.add(new javax.xml.namespace.QName("http://tempuri.org/", "FaultSerivcesSoap"));
		}
		return ports.iterator();
	}

	/**
	 * Set the endpoint address for the specified port name.
	 */
	public void setEndpointAddress(java.lang.String portName, java.lang.String address) throws javax.xml.rpc.ServiceException {

		if ("FaultSerivcesSoap12".equals(portName)) {
			setFaultSerivcesSoap12EndpointAddress(address);
		} else if ("FaultSerivcesSoap".equals(portName)) {
			setFaultSerivcesSoapEndpointAddress(address);
		} else { // Unknown Port Name
			throw new javax.xml.rpc.ServiceException(" Cannot set Endpoint Address for Unknown Port" + portName);
		}
	}

	/**
	 * Set the endpoint address for the specified port name.
	 */
	public void setEndpointAddress(javax.xml.namespace.QName portName, java.lang.String address) throws javax.xml.rpc.ServiceException {
		setEndpointAddress(portName.getLocalPart(), address);
	}

}
