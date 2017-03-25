/**
 * GetAlarmsLocator.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.4 Apr 22, 2006 (06:55:48 PDT) WSDL2Java emitter.
 */

package parser.webservice.js.cu;

public class GetAlarmsLocator extends org.apache.axis.client.Service implements parser.webservice.js.cu.GetAlarms {

	public GetAlarmsLocator() {
	}

	public GetAlarmsLocator(org.apache.axis.EngineConfiguration config) {
		super(config);
	}

	public GetAlarmsLocator(java.lang.String wsdlLoc, javax.xml.namespace.QName sName) throws javax.xml.rpc.ServiceException {
		super(wsdlLoc, sName);
	}

	// Use to get a proxy class for GetAlarmsSoap12
	private java.lang.String GetAlarmsSoap12_address = "http://10.12.1.21:8088/MetarnetAlarmsService/Alarms.asmx";

	public java.lang.String getGetAlarmsSoap12Address() {
		return GetAlarmsSoap12_address;
	}

	// The WSDD service name defaults to the port name.
	private java.lang.String GetAlarmsSoap12WSDDServiceName = "GetAlarmsSoap12";

	public java.lang.String getGetAlarmsSoap12WSDDServiceName() {
		return GetAlarmsSoap12WSDDServiceName;
	}

	public void setGetAlarmsSoap12WSDDServiceName(java.lang.String name) {
		GetAlarmsSoap12WSDDServiceName = name;
	}

	public parser.webservice.js.cu.GetAlarmsSoap_PortType getGetAlarmsSoap12() throws javax.xml.rpc.ServiceException {
		java.net.URL endpoint;
		try {
			endpoint = new java.net.URL(GetAlarmsSoap12_address);
		} catch (java.net.MalformedURLException e) {
			throw new javax.xml.rpc.ServiceException(e);
		}
		return getGetAlarmsSoap12(endpoint);
	}

	public parser.webservice.js.cu.GetAlarmsSoap_PortType getGetAlarmsSoap12(java.net.URL portAddress) throws javax.xml.rpc.ServiceException {
		try {
			parser.webservice.js.cu.GetAlarmsSoap12Stub _stub = new parser.webservice.js.cu.GetAlarmsSoap12Stub(portAddress, this);
			_stub.setPortName(getGetAlarmsSoap12WSDDServiceName());
			return _stub;
		} catch (org.apache.axis.AxisFault e) {
			return null;
		}
	}

	public void setGetAlarmsSoap12EndpointAddress(java.lang.String address) {
		GetAlarmsSoap12_address = address;
	}

	// Use to get a proxy class for GetAlarmsSoap
	private java.lang.String GetAlarmsSoap_address = "http://10.12.1.21:8088/MetarnetAlarmsService/Alarms.asmx";

	public java.lang.String getGetAlarmsSoapAddress() {
		return GetAlarmsSoap_address;
	}

	// The WSDD service name defaults to the port name.
	private java.lang.String GetAlarmsSoapWSDDServiceName = "GetAlarmsSoap";

	public java.lang.String getGetAlarmsSoapWSDDServiceName() {
		return GetAlarmsSoapWSDDServiceName;
	}

	public void setGetAlarmsSoapWSDDServiceName(java.lang.String name) {
		GetAlarmsSoapWSDDServiceName = name;
	}

	public parser.webservice.js.cu.GetAlarmsSoap_PortType getGetAlarmsSoap() throws javax.xml.rpc.ServiceException {
		java.net.URL endpoint;
		try {
			endpoint = new java.net.URL(GetAlarmsSoap_address);
		} catch (java.net.MalformedURLException e) {
			throw new javax.xml.rpc.ServiceException(e);
		}
		return getGetAlarmsSoap(endpoint);
	}

	public parser.webservice.js.cu.GetAlarmsSoap_PortType getGetAlarmsSoap(java.net.URL portAddress) throws javax.xml.rpc.ServiceException {
		try {
			parser.webservice.js.cu.GetAlarmsSoap_BindingStub _stub = new parser.webservice.js.cu.GetAlarmsSoap_BindingStub(portAddress, this);
			_stub.setPortName(getGetAlarmsSoapWSDDServiceName());
			return _stub;
		} catch (org.apache.axis.AxisFault e) {
			return null;
		}
	}

	public void setGetAlarmsSoapEndpointAddress(java.lang.String address) {
		GetAlarmsSoap_address = address;
	}

	/**
	 * For the given interface, get the stub implementation. If this service has no port for the given interface, then ServiceException is thrown.
	 * This service has multiple ports for a given interface; the proxy implementation returned may be indeterminate.
	 */
	public java.rmi.Remote getPort(Class serviceEndpointInterface) throws javax.xml.rpc.ServiceException {
		try {
			if (parser.webservice.js.cu.GetAlarmsSoap_PortType.class.isAssignableFrom(serviceEndpointInterface)) {
				parser.webservice.js.cu.GetAlarmsSoap12Stub _stub = new parser.webservice.js.cu.GetAlarmsSoap12Stub(new java.net.URL(
						GetAlarmsSoap12_address), this);
				_stub.setPortName(getGetAlarmsSoap12WSDDServiceName());
				return _stub;
			}
			if (parser.webservice.js.cu.GetAlarmsSoap_PortType.class.isAssignableFrom(serviceEndpointInterface)) {
				parser.webservice.js.cu.GetAlarmsSoap_BindingStub _stub = new parser.webservice.js.cu.GetAlarmsSoap_BindingStub(new java.net.URL(
						GetAlarmsSoap_address), this);
				_stub.setPortName(getGetAlarmsSoapWSDDServiceName());
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
		if ("GetAlarmsSoap12".equals(inputPortName)) {
			return getGetAlarmsSoap12();
		} else if ("GetAlarmsSoap".equals(inputPortName)) {
			return getGetAlarmsSoap();
		} else {
			java.rmi.Remote _stub = getPort(serviceEndpointInterface);
			((org.apache.axis.client.Stub) _stub).setPortName(portName);
			return _stub;
		}
	}

	public javax.xml.namespace.QName getServiceName() {
		return new javax.xml.namespace.QName("http://tempuri.org/", "GetAlarms");
	}

	private java.util.HashSet ports = null;

	public java.util.Iterator getPorts() {
		if (ports == null) {
			ports = new java.util.HashSet();
			ports.add(new javax.xml.namespace.QName("http://tempuri.org/", "GetAlarmsSoap12"));
			ports.add(new javax.xml.namespace.QName("http://tempuri.org/", "GetAlarmsSoap"));
		}
		return ports.iterator();
	}

	/**
	 * Set the endpoint address for the specified port name.
	 */
	public void setEndpointAddress(java.lang.String portName, java.lang.String address) throws javax.xml.rpc.ServiceException {

		if ("GetAlarmsSoap12".equals(portName)) {
			setGetAlarmsSoap12EndpointAddress(address);
		} else if ("GetAlarmsSoap".equals(portName)) {
			setGetAlarmsSoapEndpointAddress(address);
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
