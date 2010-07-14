package net.httpx;

import java.util.Random;

public class Identifier {

  /**
   * The number of bits in the Identifier.
   */
  public final static int BIT_COUNT = 128;

  //*** derived values

  public final static int BYTE_COUNT = BIT_COUNT >> 3;

  public byte[] data = new byte[BYTE_COUNT];

  public Identifier() {

  }

  /**
   * Constructs an ID by copying over the values from data.
   * If data.length is greater than the ID size defined by BIT_COUNT,
   * the overflowing values are ignored. If it is less, the ID is
   * padded with 0s.
   *
   * @param data byte[]
   */
  public Identifier(byte[] data) {
    for(int i = 0; i < data.length && i < this.data.length; i ++)
      this.data[i] = data[i];
  }

  /**
   * Constructs an ID from a String in hexadecimal, that is performs
   * the reverse of toString().
   * @param id String
   */
  public Identifier(String id) {
    for(int i = 0; i < BYTE_COUNT; i ++) {
      data[i] = (byte)(Integer.parseInt(id.substring(i * 2, i * 2 + 2), 16) & 0xff);
    }
  }

  /**
   * Constructs an ID by copying over the values from the four integers
   * supplied. The overflowing values are ignored.
   *
   * @param a int
   * @param b int
   * @param c int
   * @param d int
   */
  public Identifier(int a, int b, int c, int d) {
    int i;
    for(i = 0; i < 4 && i < data.length; i ++)
      data[i] = (byte)(a >>> (24 - i * 8));

    for(i = 0; i < 4 && 4 + i < data.length; i ++)
      data[4 + i] = (byte)(b >>> (24 - i * 8));

    for(i = 0; i < 4 && 8 + i < data.length; i ++)
      data[8 + i] = (byte)(c >>> (24 - i * 8));

    for(i = 0; i < 4 && 12 + i < data.length; i ++)
      data[12 + i] = (byte)(d >>> (24 - i * 8));
  }

  public int getNumberOfCommonBytes(Identifier id) {
    int count = 0;
    while(count < BYTE_COUNT) {
      if(data[count] != id.data[count]) break;
      count++;
    }
    return count;
  }



  public boolean equals(Identifier id) {
    return getNumberOfCommonBytes(id) == BYTE_COUNT;
  }

  public static char getChar(int value) {
    if(value < 10) return (char)((int)'0' + value);
    return (char)((int)'a' + (value - 10));
  }

  public String toString() {
    char[] buffer = new char[BYTE_COUNT * 2];
    for(int i = 0; i < BYTE_COUNT; i ++) {
      buffer[i * 2] = getChar((data[i] >> 4) & 0x0f);
      buffer[i * 2 + 1] = getChar(data[i] & 0x0f);
    }
    return new String(buffer);
  }

  public String toShortString() {
    return toString().substring(0, 4);
  }

/*
  String idString = null;

  public int hashCode() {
    if(idString == null)
      idString = this.toString();
    return idString.hashCode();
  }
*/

  //*** static

  public static Random random = new Random();

  public static Identifier generateNew() {
    byte[] data = new byte[BYTE_COUNT];
    random.nextBytes(data);
    return new Identifier(data);
  }


  /**
   * Here for testing purposes.
   *
   * @param args String[]
   */
  public static void main(String[] args) {
    Identifier a = new Identifier("12340000ffffffff0000000000000000");
    System.out.println(a.toString());

/*
    //UniqueId idA = generateNew(), idB = generateNew();
    Identifier idA = new Identifier(0x12340000, 0xffffffff, 0x00000000, 0x000000000),
        idB = new Identifier(0x12341000, 0xffffffff, 0x00000000, 0x00000000);

    System.out.println(idA.toString() + " : " + idB.toString() + " : " +
                       com.httpx.net.RoutingTable.getNumberOfCommonDigits(idA, idB));

    System.out.println(com.httpx.net.RoutingTable.getDigit(idA, 0)); */
   }



}
