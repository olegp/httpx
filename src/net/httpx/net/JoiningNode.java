package net.httpx.net;

import java.util.Vector;
import java.util.Enumeration;
import net.httpx.Identifier;
import java.io.IOException;


/**
 * Describes the Node which is currently joining the network.
 * JoiningNodes are stored at the gateway Node for the duration
 * of the join process. They are removed from this list when the join process completes
 * successfully or alternatively when it times out.
 *
 * @author Oleg Podsechin
 * @version 1.0
 */
public class JoiningNode {
  Node node;

  Identifier id;
  long timeCreated;

  Connection connection;

  int hopsReceived = 0, hopCount = -1;
  Vector nodeAddresses = new Vector();

  public JoiningNode(Node node, Identifier id, Connection connection) {
    node.print("new joininig_node: " + id.toShortString());
    this.node = node;

    this.id = id;
    this.connection = connection;
    timeCreated = System.currentTimeMillis();
  }

  /**
   * Returns the time when this JoiningNode was created.
   * This is used to timeout JoiningNodes for which we don't
   * receive a response.
   * @return long
   */
  public long getTimeCreated() {
    return timeCreated;
  }

  /**
   * Completes the join procedure by sending the routing table to the Node.
   */
  public void completeJoin() {

    // note: w're using IPv4 header size
    int elementsPerPacket = (ConnectionManager.MAX_OUTGOING_DATAGRAM_SIZE - Message.BARE_HEADER_SIZE) / (4 + 2);

     Message message = null;
     int counter = 0;

    for (Enumeration i = nodeAddresses.elements(); i.hasMoreElements(); ) {
      NodeAddress nodeAddress = (NodeAddress) i.nextElement();
      if((counter % elementsPerPacket) == 0) {
        // send what we've accumulated so far
        if(message != null)
          connection.sendMessage(message);

          // start a new datagram
          message = new Message(Message.TYPE_JOIN_RESPONSE,
                                Message.DEFAULT_SIZE);
          message.writeHeader();
      }

      message.write(nodeAddress.getAddress());
      message.writeShort(nodeAddress.getPort());
      counter ++;
    }

    // send the final Message off
    if(message != null)
      connection.sendMessage(message);

    // remove ourselves from the list of JoiningNodes
    node.joiningNodes.remove(this);
  }

  public void processRoutingResponse(Message message) throws IOException {

    hopsReceived ++;

// the header has already been read, and so has the Identifier which pointed to us
//    message.readHeader();

    while (message.getReadPosition() < message.getSize()) {
      //TODO allow for IPv6
      byte[] address = new byte[4]; // IPv4 address
      message.read(address);
      short port = message.readShort();
      nodeAddresses.add(new NodeAddress(address, port));
    }

    // check if we've received all the hops (or routing table columns)
    if(hopCount != -1 && hopsReceived >= hopCount)
      completeJoin();
  }

  public void processRoutingComplete(Message message) throws IOException {
    // process only the first routing completed message
    if(hopCount == -1) {
      message.readHeader();
      hopCount = message.readByte();
      // ignore the destination id, we already know it matches "id"

      if(hopsReceived >= hopCount)
        completeJoin();
    }
  }
}
