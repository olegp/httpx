package net.httpx.cache;

import java.io.File;
import java.util.Hashtable;
import java.util.Enumeration;
import java.util.Comparator;
import net.httpx.Identifier;
import net.httpx.proxy.ContentProvider;
import net.httpx.util.MD5;


/**
 * Used to keep track of cached content.
 *
 * @author Oleg Podsechin
 * @version 1.0
 */
public class ContentStore implements ContentProvider {

  long maxSize;
  long currentSize;

  File directory;

  // the number of ContentItems
  int itemCount;

  //TODO use a custom implementation?
  // used for fast access by id
  Hashtable contentsHashtable = new Hashtable();

  // sorted by lastAccessed property
  //Object[] contentsArray;

  /**
   * Creates a contents store and loads the ids from the cache directory.
   * @param directory File
   */
  public ContentStore(File directory) {
    this.directory = directory;
    currentSize = 0;

    if(directory.exists()) {
      String[] files = directory.list();
      for (int i = 0; i < files.length; i++) {
        File file = new File(files[i]);
        // only add files to the store
        if (file.isFile()) {
          //TODO check the name of the file, it must match our format
          ContentItem item = new ContentItem(this, file);
          contentsHashtable.put(item.getId(), item);

          currentSize += file.length();
          itemCount ++;
        }
      }
    } else {
      directory.mkdirs();
    }
  }

  /**
   * Returns the file pointing to the cache directory.
   * @return File
   */
  public File getDirectory() {
    return directory;
  }

  /**
   * Flushes the entire store to disk.
   */
  public void flush(boolean removeFromMemory) {
    for (Enumeration e = contentsHashtable.elements(); e.hasMoreElements(); )
      ((ContentItem)e.nextElement()).flush(removeFromMemory);
    System.gc(); // garbage collect to remove the cached files from memory
  }

  /**
   * Trims the size of the store so it does not exceed the maximum size.
   */
  public void trim() {
/*
    for(int i = itemCount - 1; i >= 0 && currentSize > maxSize; i --) {
      ContentItem item = (ContentItem)contentsArray[i];
      long size = item.getSize();
      item.purge();
      contentsArray[i] = null;

      currentSize -= size;
      itemCount --;
    }
*/
  }

  /**
   * Creates an array of items sorted by last accessed day.
   */
  /*
  public void createSortedArray() {
    contentsArray = new ContentItem[contentsHashtable.size()];

    int i = 0;
    for (Enumeration e = contentsHashtable.elements(); e.hasMoreElements(); i ++)
      contentsArray[i] = e.nextElement();

    // sort
    Arrays.sort(contentsArray, new ContentItemComparator());

    // update indices
    for(i = 0; i < contentsArray.length; i ++)
      ((ContentItem)contentsArray[i]).accessedIndex = i;
  }
  */

  /**
   * Adds an item to the store.
   */
  public void addItem(Identifier id, byte[] data) {
    if(data != null) {
      ContentItem item = new ContentItem(this, id, data);
      contentsHashtable.put(id.toString(), item);

      currentSize += item.getSize();
      itemCount++;

      flush(true);
    }
  }

  /**
   * getContent
   *
   * @param url String
   * @return byte[]
   */
  public byte[] getContent(String url) {
    Identifier id = new Identifier(new MD5(url).Final());
    ContentItem item = ((ContentItem)contentsHashtable.get(id.toString()));
    if(item != null)
      return item.getData();
    return null;
  }

  /**
   * addContent
   *
   * @param url String
   * @param data byte[]
   */
  public void addContent(String url, byte[] data) {
    Identifier id = new Identifier(new MD5(url).Final());
    addItem(id, data);
  }

}

/**
 * Sorts the ContentItems in descending order.
 */
class ContentItemComparator implements Comparator
{
  /**
   * compare
   *
   * @param o1 Object
   * @param o2 Object
   * @return int
   */
  public int compare(Object a, Object b) {
    if(((ContentItem)a).lastAccessed < ((ContentItem)b).lastAccessed)
      return 1;
    else if(((ContentItem)a).lastAccessed > ((ContentItem)b).lastAccessed)
      return -1;
    else
      return 0;
  }
}
