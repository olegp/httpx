package net.httpx.cache;

import net.httpx.Identifier;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.FileInputStream;


/**
 * Used to describe a cached item. A cached item is stored on disk as a set
 * of relevant HTTP headers followed by the data itself.
 *  ContentItems are immutable.
 *
 * @author Oleg Podsechin
 * @version 1.0
 */
public class ContentItem {
  // the parent store
  ContentStore store;

  // an index into the lastAccessed array, used by ContentStore
  protected int accessedIndex;

  File file;
  Identifier id;

  // previously accessed is a cached copy of the last accessed date
  long previouslyAccessed, lastAccessed;

  long size;
  byte[] data = null;


  /**
   * Creates a new item by loading it from disk.
   * @param store ContentStore
   * @param file File
   */
  public ContentItem(ContentStore store, File file) {
    this.store = store;
    this.file = file;
    id = new Identifier(file.getName());
    previouslyAccessed = lastAccessed = file.lastModified();
    size = file.length();
  }

  /**
   * Creates a new item by loading it from memory.
   * @param store ContentStore
   * @param id String
   * @param data byte[]
   */
  public ContentItem(ContentStore store, Identifier id, byte[] data) {
    this.store = store;
    this.id = id;
    this.data = data;
    previouslyAccessed = 0;
    size = data.length;
    touch();
  }

  /**
   * Flushes the item to disk.
   */
  public synchronized void flush(boolean removeFromMemory) {
    if(file == null && data != null) {
      file = new File(store.getDirectory().getPath() + "/" + id);
      try {
        OutputStream out = new FileOutputStream(file);
        out.write(data);
        out.close();
      } catch(IOException e) {
        System.out.println(e.toString());
      }

      // remove the content item from memory
      if(removeFromMemory)
        data = null;

    } else {
      // the contents of the file are immutable, simply update the lastAccessed date
      if(lastAccessed != previouslyAccessed) {
        file.setLastModified(lastAccessed);
      }
    }
  }

  public byte[] getData() {
    // if there's no data, load it from disk
    if(data == null) {
      if(file == null)
        file = new File(store.getDirectory().getPath() + "/" + id);
      try {
        InputStream in = new FileInputStream(file);
        data = new byte[in.available()];
        in.read(data);
        in.close();
        touch();
      } catch(IOException e) {
        System.out.println(e.toString());
      }
    }
    return data;
  }

  /**
   * Removes this item from the store.
   */
  public void purge() {
    if(file != null) {
      if(!file.delete()) {
        //TODO handle error, exceptions shouldn't be thrown here
      }
    }

    // remove from hashtable
    store.contentsHashtable.remove(id);
    // we are removed from the array elsewhere
  }

  public File getFile() {
    return file;
  }

  public Identifier getId() {
    return id;
  }

  public long getLastAccessed() {
    return lastAccessed;
  }

  public long getSize() {
    return size;
  }

  public void setLastAccessed(long lastAccessed) {
    this.lastAccessed = lastAccessed;
  }

  public void touch() {
    setLastAccessed(System.currentTimeMillis());
  }

}
