package net.httpx.net;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Describes an IP address + port number.
 */
public class NodeAddress {
  byte[] address;
  short port;

  public NodeAddress(byte[] address, short port) {
    this.address = address;
    this.port = port;
  }

  public static NodeAddress getAddress(String address, short port) {
    try {
      return new NodeAddress(InetAddress.getByName(address).getAddress(), port);
    }
    catch (UnknownHostException e) {
      return null;
    }
  }

  public short getPort() {
    return port;
  }

  public void setAddress(byte[] address) {
    this.address = address;
  }

  public void setPort(short port) {
    this.port = port;
  }

  public byte[] getAddress() {
    return address;
  }

  public String toString() {
    try {
      return InetAddress.getByAddress(address).getHostAddress() + ":" + port;
    } catch(UnknownHostException e) {
      return "unknown" + ":" + port;
    }
  }

  /**
   * Checks if the two addresses and ports are the same.
   * @param addressA byte[]
   * @param portA int
   * @param addressB byte[]
   * @param portB int
   * @return boolean
   */
  public static boolean doAddressesMatch(byte[] addressA, int portA, byte[] addressB, int portB) {
    if(portA != portB) return false;
    for(int i = 0; i < addressA.length; i ++) {
      if(addressA[i] != addressB[i]) return false;
    }
    return true;
  }

  /**
   * Checks if the two addresses and ports are the same.
   * @param a NodeAddress
   * @param b NodeAddress
   * @return boolean
   */
  public static boolean doAddressesMatch(NodeAddress a, NodeAddress b) {
    return doAddressesMatch(a.getAddress(), a.getPort(), b.getAddress(), b.getPort());
  }
}


