package net.httpx.util;

import java.io.*;
import java.util.*;
import java.awt.*;

/**
 * This is a simplistic ini file class implementation.
 *
 * @author Oleg Podsechin
 * @version 1.0
 */
public class IniFile {
  private String filename;
  private Hashtable keyvaluepairs = new Hashtable();
  private boolean isavailable;

  /**
   * Checks whether the ini file was loaded successfully.
   * @return true if the ini file is available
   */
  public boolean isAvailable() {
    return isavailable;
  }

  /**
   * Returns the name of the file from which this ini file was loaded.
   * @return String
   */
  public String getFilename() {
    return filename;
  }

  /**
   * Creates a list of key value pairs from the specified file name. <br>
   * Use the getString, getInteger and getBoolean methods to retrieve the values.
   * @param name the file name of the ini file (note: files inside jars are not
   * supported)
   */
  public IniFile(String name) {
    this.filename = name;
    isavailable = parse(name);
  }

  /**
   * Parses the file and creates a keyvaluepair list.<br>
   * @param name parses the specified file
   * @return true if it finds the file, false otherwise.
   */
  private boolean parse(String name) {
    BufferedReader in;

    try {
      in = new BufferedReader(new InputStreamReader(new FileInputStream(name)));

    }
    catch (IOException e) {
      return false;
    }

    try {
      while (true) {
        String line = in.readLine();
        if (line != null) {

          // skip the commented lines
          if (!line.startsWith("//") && !line.startsWith(";") &&
              !line.startsWith("#")) {
            int i = line.indexOf('=');

            // we also skip lines without an equal sign
            if (i != -1)
              keyvaluepairs.put(line.substring(0, i).trim().toLowerCase(),
                                line.substring(i + 1).trim());
          }

        }
        else
          break;
      }
    }
    catch (IOException e) {
    }

    return true;
  }

  /**
   * Retrieves the value of a key value pair based on the key name.
   * @param key the name of the key the value of which is retrieved
   * @return the String value that corresponds to the given key name
   */
  public String getString(String key) {
    String value = (String) keyvaluepairs.get(key.toLowerCase());
    return value == null ? "" : value;
  }

  /**
   * Retrieves the value of a key value pair based on the key name.
   * If the key is not found 0 is returned.
   * @param key the name of the key the value of which is retrieved
   * @return the int value that corresponds to the given key name
   */
  public int getInteger(String key) {
    try {
      return Integer.decode(getString(key)).intValue();
    }
    catch (Exception e) {
      return 0;
    }
  }

  /**
   * Retrieves the value of a key value pair based on the key name.
   * If the key is not found false is returned.
   * @param key the name of the key the value of which is retrieved
   * @return the boolean value that corresponds to the given key name
   */
  public boolean getBoolean(String key) {
    try {
      String string = getString(key);
      return string.equalsIgnoreCase("true") || string.equals("1");
    }
    catch (MissingResourceException e) {
      return false;
    }
  }

  /**
   * Retrieves the value of a key value pair based on the key name.
   * If the key is not found null is returned.
   * @param key the name of the key the value of which is retrieved
   * @return the int value that corresponds to the given key name
   */
  public Color getColor(String key) {
    String cs = getString(key);
    cs.trim();
    int i1 = cs.indexOf(','), i2 = cs.lastIndexOf(',');
    if (i1 != i2) {
      try {
        int r = Integer.decode(cs.substring(0, i1).trim()).intValue();
        int g = Integer.decode(cs.substring(i1 + 1, i2).trim()).intValue();
        int b = Integer.decode(cs.substring(i2 + 1).trim()).intValue();

        return new Color(r, g, b);
      }
      catch (Exception e) {}
    }

    return null;
  }

  /**
   * Sets the specified key to the specified value, returns true if a new key
   * was created.
   * @param key the name of the key
   * @param value the new value that replaces the old one
   * @return true if successful
   */
  public boolean setString(String key, String value) {
    return keyvaluepairs.put(key.toLowerCase(), value) == null ? true : false;
  }

  /**
   * Saves the settings to file.
   * @param filename String
   */
  public void save(String filename) throws IOException {
    String newline = System.getProperty("line.separator");
    FileWriter out = new FileWriter(filename);
    Enumeration e = keyvaluepairs.keys();
    while (e.hasMoreElements()) {
      String key = (String) e.nextElement();
      out.write(key + "=" + (String) keyvaluepairs.get(key) + newline);
    }
    out.close();
  }
}
