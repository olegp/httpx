package net.httpx.net;

import net.httpx.Identifier;
import java.io.IOException;

/**
 * Describes a single datagram packet exchanged between Nodes.
*
 * @author Oleg Podsechin
 * @version 1.0
 */
public class Message {

  //*** Constants

  public final static int DEFAULT_SIZE = 1024;

 /**
  * The size of the bare header.
  */
 public final static int BARE_HEADER_SIZE = 2;

 /**
  * The size of the full header including source and destination IDs.
  */
 public final static int FULL_HEADER_SIZE = BARE_HEADER_SIZE + Identifier.BYTE_COUNT * 2;

 public final static String READ_PAST_EOF_MESSAGE = "Read past EOF marker";
 public final static String WRITE_PAST_EOF_MESSAGE = "Write past EOF marker";

  //*** Messages sent only to neighbor and gateway Nodes

  public final static byte TYPE_JOIN_REQUEST = 0;
  public final static byte TYPE_JOIN_RESPONSE = 1;

  public final static byte TYPE_CONNECT_REQUEST = 2;
  public final static byte TYPE_CONNECT_RESPONSE = 3;

  public final static byte TYPE_PING = 4;
  public final static byte TYPE_ACK = 5;


  //*** Messages routed across the network
  public final static byte TYPE_FINGER = 16;
  public final static byte TYPE_ROUTING_REQUEST = 17;
  public final static byte TYPE_ROUTING_RESPONSE = 18;
  public final static byte TYPE_ROUTING_COMPLETE = 19;

  //*** UDP Connection variables

  /**
   * The time the message was sent, used for determining Connection latency.
   */
  protected long sentTime;

  /**
   * The number of times this Message has been sent.
   */
  protected int sendAttempts;


  //*** Message contents

  /**
   * Message data as a raw byte array.
   */
  protected byte[] data;


  //*** Cached header contents

  /**
   * Message type, see TYPE_PING etc.
   */
  protected byte type;

  /**
   * A counter based message ID which is used for AKs.
   */
  protected Byte id;

  //*** Routing parameters, not included in PING and ACK messages
  protected Identifier source;
  protected Identifier destination;

   /**
   * The actual size of the data buffer (must be less than or equal to
   * data.length). This value is used for keeping track of the buffer
   * size when writing to the buffer.
   */
  int size = 0;

  /**
   * The position we are currently reading the data from.
   */
  int readPosition;


  /**
   * Creates a Message for writing to.
   *
   * @param type byte see TYPE_PING etc.
   * @param id byte Connection specific ACKnowledgement ID
   * @param size int the expected size of the Message
   */
  public Message(byte type, int size) {
    this.type = type;
    this.size = size;
    this.data = new byte[size];
  }

  /**
   * Creates a Message for writing to.
   *
   * @param type byte see TYPE_PING etc.
   * @param id byte Connection specific ACKnowledgement ID
   */
/*
  public Message(byte type) {
    this.type = type;
    this.size = 0;
    this.data = new byte[DEFAULT_BUFFER_SIZE];
  }
*/
  /**
   * Creates a message for writing to.
   * @param source UniqueId
   * @param destination UniqueId
   * @param type byte
   */
  public Message(byte type, Identifier source, Identifier destination) {
    this.source = source;
    this.destination = destination;
    this.type = type;
    this.size = 0;
    this.data = new byte[DEFAULT_SIZE];
  }


  public static String typeToString(int type) {
    switch(type) {
      case TYPE_JOIN_REQUEST:
        return "join_request";
      case TYPE_JOIN_RESPONSE:
        return "join_response";
      case TYPE_CONNECT_REQUEST:
        return "connect_request";
      case TYPE_CONNECT_RESPONSE:
        return "connect_response";

      case TYPE_PING:
        return "ping";
      case TYPE_ACK:
        return "ack";

      case TYPE_FINGER:
        return "finger";

      case TYPE_ROUTING_REQUEST:
        return "routing_request";
      case TYPE_ROUTING_RESPONSE:
        return "routing_response";
      case TYPE_ROUTING_COMPLETE:
        return "routing_complete";
      default:
        return "unknown";
    }
  }

  public byte getId() {
    return id.byteValue();
  }

  public Identifier getSource() {
    return source;
  }

  public byte[] getData() {
    return data;
  }

  public byte getType() {
    return type;
  }

  public long getSentTime() {
    return sentTime;
  }

  public void setSentTime(long time) {
    this.sentTime = time;
  }

  public void setDestination(Identifier destination) {
    this.destination = destination;
  }

  public void setSource(Identifier source) {
    this.source = source;
  }

  public void setData(byte[] data) {
    this.data = data;
  }

  public void setType(byte type) {
    this.type = type;
  }

  public Identifier getDestination() {
    return destination;
  }




  /**
   * Expands the internal buffer to accomodate the new size.
   * @param size the new size of the buffer
   */
  public void expandBuffer(int size) {
    if (size > this.data.length) {
      int newSize = data.length;
      while (size > newSize)
        newSize *= 2;

      // expand the buffer
      byte[] newData = new byte[newSize];
      System.arraycopy(data, 0, newData, 0, this.size);
      this.data = newData;
    }
  }

  /**
   * Sets the internal size variable. This has no effect on
   * the size of the internal buffer. To modify that, call expandBuffer().
   * @param size int
   */
  public void setSize(int size) {
    this.size = size;
  }



  /**
   * Returns the number of bytes at the beginning of the internal buffer
   * that are guaranteed to contain valid data.
   * @return the specified size of this DataBuffer
   */
  public int getSize() {
    return size;
  }

  /**
   * Returns the actual size of the internal buffer.
   * @return the size of the internal buffer
   */
  public int getActualSize() {
    return data.length;
  }

  /**
   * Returns the index of the byte we are currently reading.
   * @return int
   */
  public int getReadPosition() {
    return readPosition;
  }

  /**
   * Returns the byte buffer contained in this object. Please
   * note that the size of the buffer (i.e. buffer.length) is not guaranteed
   * to equal the size of the buffer and the rest of the array may therefore
   * contain undefined data.
   * @return the byte buffer contained in this object
   */
  public byte[] getBuffer() {
    return data;
  }

  /**
   * Resets by setting size to 0.
   */
  public void startWriting() {
    size = 0;
  }

  /**
   * Writes a byte.
   * @param value byte
   */
  public void writeByte(byte value) {
    if(size + 1 > data.length)
      expandBuffer(size + 1);
    data[size ++] = value;
  }

  /**
   * Writes a short.
   * @param value short
   */
  public void writeShort(short value) {
    if(size + 2 > data.length)
      expandBuffer(size + 2);
    data[size ++] = (byte)((value >> 8) & 0xff);
    data[size ++] = (byte)(value & 0xff);
  }

  /**
   * Writes an int.
   * @param value int
   */
  public void writeInt(int value) {
    if(size + 4 > data.length)
      expandBuffer(size + 4);
    data[size ++] = (byte)((value >> 24) & 0xff);
    data[size ++] = (byte)((value >> 16) & 0xff);
    data[size ++] = (byte)((value >> 8) & 0xff);
    data[size ++] = (byte)(value & 0xff);
  }

  /**
   * Writes a String.
   * @param value String
   */
  public void writeString(String value) {
    byte[] bytes = value.getBytes();
    if(size + bytes.length + 4 > data.length)
      expandBuffer(size + bytes.length + 4);

    writeInt(bytes.length);
    System.arraycopy(bytes, 0, data, size, bytes.length);
    size += bytes.length;
  }


  /**
   * Writes a byte array.
   * @param bytes byte[]
   * @param offset int
   * @param length int
   */
  public void write(byte[] bytes, int offset, int length) {

    if(size + length > data.length)
      expandBuffer(size + length);

    System.arraycopy(bytes, offset, data, size, length);
    size += length;
  }

  /**
   * Writes a byte array.
   * @param bytes byte[]
   */
  public void write(byte[] bytes) {
    write(bytes, 0, bytes.length);
  }


  /**
   * Writes the message type and source and destination if appropriate.
   */
  public void writeHeader() {
    startWriting();

    //TODO merge type and ID?
    writeByte(type);
    // the ID is filled only when we send the packet
    // see class Connection.sendMessage for more details
    writeByte((byte)0);

    if(type >= TYPE_FINGER) {
      writeIdentifier(source);
      writeIdentifier(destination);
    }
  }

  /**
   * Resets the DataBuffer by setting readPosition to 0.
   */
  public void startReading() {
    readPosition = 0;
  }


  /**
   * Reads a byte from the DataBuffer.
   * @return byte
   */
  public byte readByte() throws IOException {
    byte ret = 0;
    try {
       ret = data[readPosition++];
     } catch(ArrayIndexOutOfBoundsException e) {
       throw new IOException(READ_PAST_EOF_MESSAGE);
    }

    if(readPosition > size)
      throw new IOException(READ_PAST_EOF_MESSAGE);

    return ret;
  }

  /**
   * Reads a short from the DataBuffer.
   * @return short
   */
  public short readShort() throws IOException {
    short ret = 0;
    try {
      ret |= data[readPosition++] << 8;
      ret |= data[readPosition++];

    } catch(ArrayIndexOutOfBoundsException e) {
       throw new IOException(READ_PAST_EOF_MESSAGE);
    }

    if(readPosition > size)
      throw new IOException(READ_PAST_EOF_MESSAGE);


    return ret;
  }

  /**
   * Reads an int from the DataBuffer.
   * @return int
   */
  public int readInt() throws IOException {

    int ret = 0;
    try {
      ret |= data[readPosition++] << 24;
      ret |= data[readPosition++] << 16;
      ret |= data[readPosition++] << 8;
      ret |= data[readPosition++];
    } catch(ArrayIndexOutOfBoundsException e) {
       throw new IOException(READ_PAST_EOF_MESSAGE);
    }

    if(readPosition > size)
      throw new IOException(READ_PAST_EOF_MESSAGE);

    return ret;
  }

  /**
   * Reads in a String from a DataBuffer. Returns null for String of length 0.
   * @return String
   */
  public String readString() throws IOException {
    try {
      int size = readInt();
      if (size > 0) {
        String ret = new String(data, readPosition, size);
        readPosition += size;
        return ret;
      }
    } catch(ArrayIndexOutOfBoundsException e) {
      throw new IOException(READ_PAST_EOF_MESSAGE);
    }

    if(readPosition > size)
      throw new IOException(READ_PAST_EOF_MESSAGE);

    return null;
  }

  public void read(byte[] bytes, int offset, int length) throws IOException {
    try {
      System.arraycopy(data, readPosition, bytes, offset, length);
      readPosition += length;
    }
    catch (ArrayIndexOutOfBoundsException e) {
      throw new IOException(READ_PAST_EOF_MESSAGE);
    }
    if(readPosition > size)
      throw new IOException(READ_PAST_EOF_MESSAGE);
  }

  public void read(byte[] bytes) throws IOException {
    read(bytes, 0, bytes.length);
  }

  public void skip(int count) {
    readPosition += count;
  }

  public int available() {
    return size - readPosition;
  }

  /**
   * Reads the header and copies the data over to the internal attributes:
   * type, id, source and destination.
   */
  public void readHeader() throws IOException {
    startReading();
    type = readByte();
    id = new Byte(readByte());

    // read source and destination ID only if this is a routing message
    if(type >= TYPE_FINGER) {
      source = readIdentifier();
      destination = readIdentifier();
    }

  }

 /**
   * Reads an Identifier from a DataBuffer.
   * @param buffer DataBuffer
   * @return Identifier
   */
  public Identifier readIdentifier() throws IOException {
    Identifier identifier = new Identifier();
    for(int i = 0; i < Identifier.BYTE_COUNT; i ++)
      identifier.data[i] = readByte();
    return identifier;
  }

  public void writeIdentifier(Identifier id) {
     for(int i = 0; i < Identifier.BYTE_COUNT; i ++)
      writeByte(id.data[i]);
  }

  /**
   * Creates a copy of this message by copying the contents
   * and cached attributes to a new memory location.
   * @return Message
   */
  public Message createCopy() {
    Message message = new Message(type, size);

    // copy the data over
    System.arraycopy(data, 0, message.data, 0, size);

    // set ID to 0 as a precaution
    message.data[1] = 0;

    // copy the cached variables over as well
    message.setSource(source);
    message.setDestination(destination);

    return message;
  }

}
