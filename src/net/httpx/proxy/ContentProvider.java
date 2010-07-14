package net.httpx.proxy;

public interface ContentProvider {
  public byte[] getContent(String url);
  public void addContent(String url, byte[] data);
}
