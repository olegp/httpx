package net.httpx;

import net.httpx.proxy.*;
import net.httpx.cache.ContentStore;
import java.io.File;

public class Client {

  public static void main(String[] args) {
    Proxy proxy = new Proxy(8080, "", 8080, 20);
    ContentStore store = new ContentStore(new File("cache"));
    proxy.setDebug(1, System.out);
    proxy.setContentProvider(store);
    proxy.start();

    while (true) {
      try {
        Thread.sleep(3000);
      }
      catch (Exception e) {}
    }
  }
}
