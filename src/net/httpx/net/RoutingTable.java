package net.httpx.net;

import net.httpx.Identifier;
import java.io.IOException;
import java.io.FileOutputStream;
import java.io.BufferedWriter;
import java.io.OutputStreamWriter;

/**
 * @author Oleg Podsechin
 */
public class RoutingTable {

  /**
   * The number of bits in a single digit used for routing.
   */
  public final static int BITS_PER_DIGIT = 4;

  //*** derived values

  public final static int VALUES_PER_DIGIT = 1 << BITS_PER_DIGIT;
  public final static int DIGITS_PER_ID = Identifier.BIT_COUNT / BITS_PER_DIGIT;
  public final static int DIGITS_PER_BYTE = 8 / BITS_PER_DIGIT;

  public final static int DIGIT_MASK = 0xffffffff >>> (32 - BITS_PER_DIGIT);

  Node node;

  Connection[][] entries = new Connection[DIGITS_PER_ID][VALUES_PER_DIGIT];

  /**
   * The Node this routing table is associated with.
   * @param node Node
   */
  public RoutingTable(Node node) {
    this.node = node;
  }

//*** functions for working on digits stored in an ID

   /**
    * Returns the number of common digits.s
    * @param a UniqueId
    * @param b UniqueId
    * @return int
    */
   public static int getNumberOfCommonDigits(Identifier a, Identifier b) {
    int count = 0;
    while(count < Identifier.BYTE_COUNT) {
      if(a.data[count] != b.data[count]) break;
      count ++;
    }

    // get number of digits
    int digitCount = count * RoutingTable.DIGITS_PER_BYTE;

    if(count < Identifier.BYTE_COUNT) {
      int shiftBy = 8 - RoutingTable.BITS_PER_DIGIT;
      // don't shift by 0, because we already know that the two bytes are different
      while (shiftBy > 0) {
        int newMask = RoutingTable.DIGIT_MASK << shiftBy;
        if ( (a.data[count] & newMask) != (b.data[count] & newMask))break;
        shiftBy -= RoutingTable.BITS_PER_DIGIT;
        digitCount++;
      }
    }

    return digitCount;
  }


 /**
  * Returns the digit at the specified index.
  * @param id UniqueId
  * @param digit int
  * @return int
  */
 public static int getDigit(Identifier id, int digit) {
    return (id.data[digit / RoutingTable.DIGITS_PER_BYTE] >>>
            (8 - (((digit % RoutingTable.DIGITS_PER_BYTE) + 1) *
                  RoutingTable.BITS_PER_DIGIT))) & RoutingTable.DIGIT_MASK;
  }


  /**
   * Routes a message to the nearest node in the table.
   *
   * @param message Message
   */
  public synchronized void routeMessage(Message message) throws IOException
  {
    message.readHeader();
    Identifier destination = message.getDestination();
    int commonDigitCount = getNumberOfCommonDigits(node.id, destination); //node.id.getNumberOfCommonBytes(destination);

    // we are an exact match, this is very unlikely
    if(commonDigitCount == DIGITS_PER_ID) {
      // message delivered
      node.receiveMessage(message);
      return;
    }

    Connection[] routingColumn = entries[commonDigitCount];

    // find the matching entry
    int lastDigit = getDigit(destination, commonDigitCount);
    Connection entry = routingColumn[lastDigit];

    // match found
    if(entry != null) {
      node.forwardMessage(entry, message);
      return;
    }

    int nearestNodeBelow = -1;
    for(int i = lastDigit - 1; i > -1; i --) {
      if(routingColumn[i] != null) {
        nearestNodeBelow = i;
        break;
      }
    }

    int nearestNodeAbove = VALUES_PER_DIGIT;
    for(int i = lastDigit + 1; i < VALUES_PER_DIGIT; i ++) {
      if(routingColumn[i] != null) {
        nearestNodeAbove = i;
        break;
      }
    }

    int nearestNode;
    if(nearestNodeAbove == VALUES_PER_DIGIT) {
      if(nearestNodeBelow == -1) {
        // empty routing column
        // message delivered
        node.receiveMessage(message);
        return;
      } else {
        nearestNode = nearestNodeBelow;
      }
    } else if(nearestNodeBelow == -1) {

      nearestNode = nearestNodeAbove;

    } else {
      // route to numerically closest node
      // NOTE: bias towards lower values, this doesn't really matter
      // as long as we are consistent about it
      if(nearestNodeAbove - lastDigit > lastDigit - nearestNodeBelow)
        nearestNode = nearestNodeBelow;
      else
        nearestNode = nearestNodeAbove;
    }

    int ownLastDigit = getDigit(node.id, commonDigitCount);
    if(Math.abs(ownLastDigit - lastDigit) < Math.abs(nearestNode - lastDigit))
      node.receiveMessage(message);
    else
      node.forwardMessage(routingColumn[nearestNode], message);

  }

  /**
   * Removes the entry from the table.
   * @param id UniqueId
   * @param destroy boolean set to true to destroy the associated connection
   */
  public synchronized void removeEntry(Identifier id, boolean destroy) {
    int commonDigitCount = getNumberOfCommonDigits(node.id, id);
    if(commonDigitCount == DIGITS_PER_ID) {
      // identical id, it doesn't exist
      return;
    }

    Connection[] routingColumn = entries[commonDigitCount];
    int lastDigit = getDigit(id, commonDigitCount);

    if (routingColumn[lastDigit] != null) {
      if (destroy)
        routingColumn[lastDigit].destroy(false);

      routingColumn[lastDigit] = null;
    }
  }

  public synchronized Connection addEntry(Identifier id, Connection connection) {

    int commonDigitCount = getNumberOfCommonDigits(node.id, id);
    if(commonDigitCount == DIGITS_PER_ID) {
      // identical id, generate error
      throw new RuntimeException("Node with identical id connected " + id.toShortString());
    }

    Connection[] routingColumn = entries[commonDigitCount];

    // find the matching entry
    int lastDigit = getDigit(id, commonDigitCount);

    // severe an existing connection
    if(routingColumn[lastDigit] != null) {
      // don't replace anything if we are already in the table
      if(routingColumn[lastDigit].getRemoteId().equals(id))
          return routingColumn[lastDigit];

      node.print("replacing entry " +
                 routingColumn[lastDigit].getRemoteId().toShortString() + " with " +
                 id.toShortString());

      // we don't want to set  this entry in the routing to equal to null
      // this is done so that the action doesn't override the new entry we add below
      routingColumn[lastDigit].destroy(false);
    }

    connection.setRemoteId(id);
    routingColumn[lastDigit] = connection;

    node.print(id.toShortString() + " => routing table");
    return routingColumn[lastDigit];
  }

  /**
   * Exports the addresses of gateway nodes.
   */
  public void exportGatewayNodes(String filename) {
    try {
      FileOutputStream fileOut = new FileOutputStream(filename);
      BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fileOut));
      //[DIGITS_PER_ID][VALUES_PER_DIGIT]
      for(int i = 0; i < DIGITS_PER_ID; i ++) {
        for(int j = 0; j < VALUES_PER_DIGIT; j ++) {
          Connection connection = entries[i][j];
          if(connection != null) {
            // the first is a sanity check
            if(connection.isConnectedToNetwork() && !connection.isRestricted) {
              writer.write(connection.remoteAddress.getHostAddress() + ":" + connection.remotePort);
              writer.newLine();
            }
          }
        }
      }

      writer.close();
      fileOut.close();

    } catch(IOException e) {
      node.print("failed to export gw nodes to: " + filename);
//      e.printStackTrace();
    }
  }

}

