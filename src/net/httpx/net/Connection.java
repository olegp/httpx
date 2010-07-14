package net.httpx.net;

import java.io.*;
import java.net.*;
import java.util.*;

import net.httpx.Identifier;

public class Connection {

  //*** Connection constants
  public final static double CONNECTION_ALPHA = 0.75;
  public final static double CONNECTION_BETA = 2.0;
  public final static int MAX_SEND_ATTEMPTS = 4;
  public final static long MIN_TIMEOUT = 100;

  //***

  Node node;

  //*** remote Node address properties

  Identifier remoteId;

  String name;
  InetAddress remoteAddress;
  int remotePort;

  /**
   * Set to true if the Node at the other end of the Connection is behind a NAT of firewall.
   */
  protected boolean isRestricted = false;

  //*** Connection state properties

   /**
    * Used by ACK responses.
    */
   byte idCounter;

  /**
   * A timestamp storing the time when the Connection was last active. See
   * ConnectionManager.liveConnections for more details.
   */
  long lastActivity = 0;

  /**
   * The current estimated Connection latency. It is constructed from sampled
   * latencies as follows:<br>
   * latency = CONNECTION_ALPHA * latency + (1 - CONNECTION_ALPHA) * sampledLatency<br>
   * Currently the latency we calculate is affected by dropped packets.
   */
  long latency = ConnectionManager.INITIAL_LATENCY;

  /**
   * The period with which we ping the other side.
   */
  long pingPeriod = ConnectionManager.DEFAULT_PING_PERIOD;

  /**
   * Messages that have just been sent on this Connection,
   * but have not been acknowledged yet.
   */
  Hashtable sentMessages = new Hashtable();

  //*** Connection statistics
   /**
    * The time when this Connection was created, used together
    * with the other stats to produce average throughput statistics.
    */
   long timeCreated;

   /**
    * Number of packets sent on this Connection.
    */
   int packetsSent;

   /**
    * Number of packets that have been sent and acknowledged on this Connection.
    */
   int packetsAcknowledged;

   /**
    * Number of packets received.
    */
   int packetsReceived;


   /**
    * Creates a new Connection, associated with the given Node.
    * @param node Node
    * @param name String the name in format "address:port"
    * @param address InetAddress the remote address
    * @param port int the remote port
    */
   public Connection(Node node, String name, InetAddress address, int port) {
    this.node = node;

    this.name = name;
    this.remoteAddress = address;
    this.remotePort = port;
  }

  public String toString() {
    return (isRestricted ? "[-]" : "[+]") + " " + getRemoteIdName() + " " +
        getRemoteAddress().getHostAddress() + ":" +
        getRemotePort();
  }

  public Identifier getRemoteId() {
    return remoteId;
  }

  /**
   * Sets the remote id. This is called after a successful join attempt.
   * @param id Identifierlist
   */
  public void setRemoteId(Identifier id) {
    this.remoteId = id;
  }

  public String getName() {
    return name;
  }

  public InetAddress getRemoteAddress() {
    return remoteAddress;
  }

  public int getRemotePort() {
    return remotePort;
  }

  /**
   * Returns a nicely formatted name of the remote Node. Works even
   * if the remote node is null in which case "xxxx" is returned.
   * @return String
   */
  public String getRemoteIdName() {
    return remoteId != null ? remoteId.toShortString() : "xxxx";
  }


  /**
   * Removes the Message from the list of messages that haven't been
   * acknowledged and also notifies the ConnectionManager
   * @param id Byte
   */
  protected void acknowledgeMessage(Byte id) {
    sentMessages.remove(id);

    ConnectionManager manager = node.getConnectionManager();
    // update the manager if we are no longer active
    if(sentMessages.size() == 0)
      manager.unacknowledgedConnections.remove(this);

  }

  /**
   * Sends a Message on this Connection.
   *
   * @param message Message
   */
  public void sendMessage(Message message) {
    if(node == null) return;

    try {
      // modify the message.counter attribute
      message.id = new Byte(++idCounter);

      // insert the new Message.id into the header
      message.data[1] = message.id.byteValue(); // at index 1, after the packet type
      message.setSentTime(System.currentTimeMillis());
      message.sendAttempts = 1;

      // add the message to the table
      sentMessages.put(message.id, message);

      // add ourselves as an active connection
      if(sentMessages.size() == 1)
        node.getConnectionManager().unacknowledgedConnections.add(this);

      // send the message

      node.getConnectionManager().sendPacket(remoteAddress, remotePort,
                                             message.getBuffer(),
                                             message.getSize());

      //DEBUG
      //node.print("sent to: " + getRemoteIdName() + " [" + name + "], " + message.getSize() + " bytes");

    } catch(IOException e) {

      e.printStackTrace();

      // sending failed, remove the message

    }
  }

  /**
   * Process an ACKnowledgement response.
   * @param message Message
   */
  protected void processAck(Message message) {
    // if this is an ACKnowledgement, update our internal state
    Byte id = message.id;
    Message sentMessage = (Message)sentMessages.get(id);
    //node.print(message.id.toString());

    // make sure this Message exists
    if(sentMessage != null) {
      // update stored Connection latency
      long sampledLatency = System.currentTimeMillis() - sentMessage.sentTime;

      // our latency gradually adjusts to be the actual latency
      latency = (long)(latency * CONNECTION_ALPHA + (1 - CONNECTION_ALPHA) * sampledLatency);

      //DEBUG
      //node.print("sampled/updated latency: " + sampledLatency + "/" + latency + " " + sentMessage.sentTime);

      // PING ACK specific stuff:
      if(sentMessage.type == Message.TYPE_PING)
        this.pingPeriod = ConnectionManager.DEFAULT_PING_PERIOD * 2;

      // the message has been acknowledged, remove it
      acknowledgeMessage(message.id);
    }
  }

  /**
   * Processes CONNECT_REQUEST and CONNECT_RESPONSE Messages.
   * @param message Message
   * @throws IOException
   */
  protected void processConnect(Message message) throws IOException {
    // allow us to reconnect smoothly
    //  if(remoteId == null) {
    // don't try to route connect messages
    remoteId = message.readIdentifier();

    //IPv4
    // read local address
    byte[] address = new byte[4];
    message.read(address);
    int port = message.readShort();

    if (message.getType() == Message.TYPE_CONNECT_REQUEST) {
      // is the remote Node restricted?
      isRestricted = NodeAddress.doAddressesMatch(address, port,
                                      remoteAddress.getAddress(), remotePort);
    }

    if (message.getType() == Message.TYPE_CONNECT_RESPONSE) {
      // read whether the other side is restricted or not from the message
      isRestricted = (message.readByte() != 0);

      // tell the Node to determine is "isRestricted" state
      node.updateIsRestricted(address, port);
    }

    node.print("adding: " + remoteId.toShortString());

    // add ourselves to the routing table
    node.routingTable.addEntry(remoteId, this);

    // send a response back when we get a request
    if (message.getType() == Message.TYPE_CONNECT_REQUEST) {
      Message response = new Message(Message.TYPE_CONNECT_RESPONSE,
                                     Message.BARE_HEADER_SIZE + Identifier.BYTE_COUNT +
                                     6 + 1); // header + id + address:port + 1 byte
      response.writeHeader();
      response.writeIdentifier(node.id);

      // write which port we see the Node connecting to us on
      response.write(node.address.getAddress());
      response.writeShort( (short) node.port);

      // write whether we ourselves are restricted or not
      response.writeByte( (byte) (node.isRestricted ? 1 : 0));

      sendMessage(response);
    }

    //    } else {
    // node.print("Double connection attempt!");
    //    }
  }

  /**
   * Processes messages we receive on a datagram socket.
   * @param message Message
   * @throws IOException
   */
  public void receiveMessage(Message message) throws IOException {

    //NOTE: the header has already been read in

    // process ACKs
    switch(message.getType()) {
      // pings are ignored
      case Message.TYPE_PING:
        pingPeriod = ConnectionManager.DEFAULT_PING_PERIOD;
        break;

      case Message.TYPE_ACK:
        processAck(message);
        break;

      case Message.TYPE_CONNECT_REQUEST:
      case Message.TYPE_CONNECT_RESPONSE:
        if(isConnectedToNetwork()) {
          node.print("connection override, already connected to network");
        }
        processConnect(message);
        break;

      case Message.TYPE_JOIN_REQUEST:
        if(node.isConnected) {
          // don't go via the routing table
          Identifier id = message.readIdentifier();
          node.initiateJoin(id, this);
        } else
          node.print("join_request received, but node disconnected");
        break;

      case Message.TYPE_JOIN_RESPONSE:
        //accept these messages only from the current gateway Node
        if(NodeAddress.doAddressesMatch(node.currentGatewayAddress.getAddress(),
                                        node.currentGatewayAddress.getPort(),
                                        remoteAddress.getAddress(), remotePort)) {
          // don't go via the routing table
          node.processJoinResponse(message);
        } else
          node.print("unauthorized join_response received, ignoring");
        break;

      default:

        // create a local copy of the message and send it along
        node.routingTable.routeMessage(message.createCopy());
        break;
    }
  }

  /**
   * Returns true if this Connection is a valid connection to the
   * peer to peer network. If this returns false, this means this Connection
   * is still in the process of being integrated into the network
   * either as a result of a JOIN_REQUEST or CONNECT_REQUEST
   * @return boolean
   */
  public boolean isConnectedToNetwork() {
    return remoteId != null;
  }

  /**
   * Times out messages for which we haven't received an ACK.
   */
  public void performTimeout() {
    if(node == null) return;

    long timeout = (long)(latency * CONNECTION_BETA);
    long currentTime = System.currentTimeMillis();

    for (Enumeration j = sentMessages.elements(); j.hasMoreElements(); ) {
      Message message = (Message) j.nextElement();

      if (currentTime - message.sentTime > Math.max(timeout, MIN_TIMEOUT)) {
        if (message.sendAttempts < MAX_SEND_ATTEMPTS) {
          // resend the message
          try {
            node.print("message to: " + getRemoteIdName() + " timed out, resending");

            message.setSentTime(currentTime);
            message.sendAttempts++;

            node.getConnectionManager().sendPacket(remoteAddress, remotePort,
                message.getBuffer(),
                message.getSize());

          }
          catch (IOException e) {
            e.printStackTrace();
          }
        }
        else {
          // Connection failed!
          node.print("connection to: " + getRemoteIdName() + " failed");
          destroy();

          // forget about any other messages in the queue
          break;
        }
      }
    }
  }

  /**
   * Destroys this Connection, also removes it from
   * any RoutingTables or other lists it may be part of.
   */
  public void destroy() {
    destroy(true);
  }

  /**
   * Destroys this Connection.
   * @param removeEntryOnDestroy boolean if false, don't remove this Connection
   * RoutingTable
   */
  public void destroy(boolean removeRoutingTableEntry) {
    if(node != null) {
      if(remoteId != null)
        node.print("" + getRemoteIdName() + " disconnected");

      if (removeRoutingTableEntry && remoteId != null) {
        // remove ourselves from the routing table, don't call destroy
        node.routingTable.removeEntry(remoteId, false);
      }

      // tell the ConnectionManager to get rid of us
      node.getConnectionManager().removeConnection(this);
      node = null;
    }
  }

}
