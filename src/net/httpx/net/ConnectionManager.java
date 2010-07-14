package net.httpx.net;

import java.net.DatagramSocket;
import java.net.DatagramPacket;
import java.io.IOException;
import java.util.Hashtable;
import java.net.InetAddress;
import java.util.Vector;
import java.util.Enumeration;
import java.net.BindException;

import net.httpx.Identifier;
import java.net.SocketException;

public class ConnectionManager {

  /**
   * Default port for both UDP and TCP Connections.
   */
  public final static int DEFAULT_PORT = 9310;

  /**
   * The initial latency that we assume a Connection to have.
   */
  public final static long INITIAL_LATENCY = 1000;

  /**
   * The base rate for PINGing neighbor Nodes, in milliseconds.
   */
  public final static long DEFAULT_PING_PERIOD = 5000;

  /**
   * The period with which we refresh unacknowledged connections, in milliseconds.
   */
  public final static long MAINTENANCE_PERIOD = 100;


  /**
   * The maximum size of the UDP receive buffer in bytes.
   */
  public final static int MAX_INCOMING_DATAGRAM_SIZE = 4096;

  /**
  * The maximum size of the UDP send buffer in bytes.
   */
  public final static int MAX_OUTGOING_DATAGRAM_SIZE = 512;


  /**
   * Set to false to stop the manager.
   */
  boolean isRunning = true;

  ConnectionManagerThread receiveThread, maintenanceThread;

  Node node;
  DatagramSocket socket;

  /**
   * A list of all Connections, referenced by connection name ("address:port").
   */
  Hashtable connections = new Hashtable();

  /**
   * A list of  Connections which contain messages
   * for which we haven't yet received an ACK.
   */
  Vector unacknowledgedConnections = new Vector();

  /**
   * A list of all Connections, sorted by their lastActivity property.
   */
  Vector activeConnections = new Vector();

  public ConnectionManager(Node node) {
    this.node = node;
  }

  public DatagramSocket getSocket() {
    return socket;
  }

  public void start() {
    receiveThread = new ConnectionManagerThread(this, ConnectionManagerThread.TYPE_RECEIVE);
    maintenanceThread = new ConnectionManagerThread(this, ConnectionManagerThread.TYPE_MAINTENANCE);

    receiveThread.start();
    maintenanceThread.start();
  }

  //DEBUG
  //Random random = new Random();

  /**
   * Sends a datagram packet on the given Connection. This is a wrapper
   * which allows us to collect additional stats and generate DEBUG info.
   * This also allows us to randomly drop packets for simple testing purposes.
   * This function is also used for opening punching through NATs.
   *
   * @param connection Connection
   * @param data byte[]
   */
  public void sendPacket(InetAddress address, int port, byte[] data, int length) throws IOException {

    //DEBUG drop every other packet
    //if(random.nextInt() % 2 != 0) {
      DatagramPacket packet = new DatagramPacket(data, length, address, port);

      //DEBUG
      if (length > MAX_OUTGOING_DATAGRAM_SIZE) {
        node.print("Packet size exceeds " +  MAX_OUTGOING_DATAGRAM_SIZE + " bytes");
      }

      if(length == 29) {
        //int i = 0;
      }


      if(socket != null)
        socket.send(packet);
    //}
  }


  /**
   * Pings a Node. This serves a number of purposes: it allows us to
   * keep track of the link latency, maintains bindings at NATs and firewalls
   * and allows us to detect when a Node goes offline. Pings are sent only
   * when there's no other traffic between the Nodes. The timeout is Connection
   * specific, but it should be under 60s, since NATs usually remove UDP
   * bindings after this period.
   *
   * @param connection Connection
   */
  public void sendPing(Connection connection) {
     // construct message
     Message message = new Message(Message.TYPE_PING, Message.BARE_HEADER_SIZE);
     message.writeHeader();

     connection.sendMessage(message);
     node.print("ping: " + connection.getRemoteIdName());
  }

  /**
   * Sends an ACKnowledgement response. ACKs are differnt from other messages
   * in that they we do not expect to receive an ACK in response to an ACK message.
   * As a results, ACKs are not sent via the Connection object.
   *
   * @param connection Connection
   * @param id byte
   */
  public void sendAck(InetAddress address, int port, byte id) {
    try {
      byte[] data = new byte[] {Message.TYPE_ACK, id};
      sendPacket(address, port, data, data.length);
    } catch(IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * Creates a new Connection and adds it to the internal lists.
   *
   * @param name String
   * @param address InetAddress
   * @param port int
   * @return Connection
   */
  public Connection createConnection(String name, InetAddress address, int port) {

    // don't let us connect to ourselves
    if (NodeAddress.doAddressesMatch(address.getAddress(), port,
                                    node.address.getAddress(), node.port)) {
      node.print("loopback connection denied");
      return null;
    }

    Connection connection = new Connection(node, name, address, port);
    connection.lastActivity = System.currentTimeMillis();

    connections.put(name, connection);
    activeConnections.add(connection);

    return connection;
  }

  public Connection createConnection(InetAddress address, int port) {
    return createConnection(address.getHostAddress() + ":" + port, address, port);
  }

  /**
   * Connects to the network, should be called after the join process
   * completed successfully.
   *
   * @param address InetAddress
   * @param port int
   */
  public void connect(InetAddress address, int port) {

    if(node.isConnected == false)
      node.print("warning: connecting to the network before join is complete");

    Connection connection = createConnection(address, port);

    if(connection != null) {

      // send the CONNECT_REQUEST message to initiate connection
      Message message = new Message(Message.TYPE_CONNECT_REQUEST,
                                    Message.BARE_HEADER_SIZE +
                                    Identifier.BYTE_COUNT);
      message.writeHeader();
      message.writeIdentifier(node.id);

      // write the local address and port number
      // these are used for NAT detection
      message.write(node.address.getAddress());
      message.writeShort((short)node.port);
      //this is received in: Connection.processConnect

      connection.sendMessage(message);
    }
  }

  /**
   * Attempts to join the network by sending a join request to a gateway Node.
   *
   * @param address InetAddress
   * @param port int
   */
  public void join(InetAddress address, int port) {
    Connection connection = createConnection(address, port);
    if(connection != null) {
      // send the JOIN_REQUEST message to initiate join
      Message message = new Message(Message.TYPE_JOIN_REQUEST,
                                    Message.BARE_HEADER_SIZE +
                                    Identifier.BYTE_COUNT);
      message.writeHeader();
      message.writeIdentifier(node.id);

      connection.sendMessage(message);
    }
  }

  /**
   * Entry point for the Thread which receives UDP messages.
   */
  public void receiveThread() {

    try {
      // construct out input buffer and bind socket
      Message message = new Message((byte)0, MAX_INCOMING_DATAGRAM_SIZE);
      try {
        socket = new DatagramSocket(node.port);
      } catch(BindException e) {
        node.print("port " + node.port + " already in use");
        stop();
        return;
      }

      node.print("started on " + node.port);

      while (isRunning) {
        try {

          // read the datagram straight into the receive buffer
          DatagramPacket packet = new DatagramPacket(message.getBuffer(), message.getActualSize());

          //node.print("before receive");
          socket.receive(packet);

          //DEBUG
          //node.print("received: " + packet.getLength() + " bytes");

          message.setSize(packet.getLength());
          message.readHeader();

          // don't reply to ACKs
          if(message.getType() != Message.TYPE_ACK) {

            // send an ACK back right away, this way the reported latency isn't
            // affected by any calculations we may do as part of the routing process
            sendAck(packet.getAddress(), packet.getPort(), message.getId());

            //DEBUG
            //node.print("received: " + Message.typeToString(message.getType()));
          }


          synchronized(this) {

            // process message
            InetAddress address = packet.getAddress();
            int port = packet.getPort();

            // name is of the form "127.0.0.1:1234",
            // this is guaranteed to be unique unlike the plain host address
            // string without the port number
            //TODO optimize this
            String name = address.getHostAddress() + ":" + port;

            Connection connection = (Connection) connections.get(name);
            if (connection == null) {

              // create a new Connection if one doesn't exist
              // this times out and is removed in a separate Thread
              connection = createConnection(name, address, port);

            } else {

              // update liveConnections queue and lastActivity attribute
              connection.lastActivity = System.currentTimeMillis();
              activeConnections.remove(connection);
              activeConnections.add(connection);
            }

            // process the received packet, use the DataBuffer methods
            connection.receiveMessage(message);

          }

        } catch(SocketException e) {
          // ignore, it's thrown when the Socket is closed from another Thread
        } catch (IOException e) {
          // suppress these exceptions, it's thrown when the Socket is closed
          // or alternatively when the message we received was malformatted
          //node.print(e.toString());
          e.printStackTrace();
        }
      }
    } catch(IOException e) {
      //node.print(e.toString());
      e.printStackTrace();
    }
  }

  /**
   * Sends PING messages to Connections at regular intervals.
   * This ensures that we know the state of the remote Node and also
   * keeps NAT and firewall UDP bindings in place.
   */
  protected void pingConnections() {
     long currentTime = System.currentTimeMillis();
    // send pings
    for (Enumeration i = activeConnections.elements(); i.hasMoreElements(); ) {
      Connection connection = (Connection) i.nextElement();

      // since the Connections are sorted, we don't need to cycle through all of them
      if (currentTime - connection.lastActivity < DEFAULT_PING_PERIOD)
        break;

      // don't ping unacknowledged connections, since we are already expecting a reply
      // also, don't ping connections which aren't part of the network
      // (these include JOIN and CONNECT attempts) since we're already expecting a response
      if (unacknowledgedConnections.indexOf(connection) == -1 &&
          connection.isConnectedToNetwork())
        sendPing(connection);
    }
  }

  /**
   * Times out JoiningNodes for which we haven't received a reply.
   * The timeout used is PING_TIMEOUT.  This is done mostly to ensure
   * that we can still send a UDP message back to the joining Node.
   */
  protected void timeoutJoiningNodes() {
    long currentTime = System.currentTimeMillis();
    // we don't need to sort like in the case of activeConnections,
    // since we expect the number of JoiningNodes to be quite small
    for (Enumeration i = node.joiningNodes.elements(); i.hasMoreElements(); ) {
      JoiningNode joiningNode = (JoiningNode) i.nextElement();
      if(currentTime - joiningNode.getTimeCreated() > DEFAULT_PING_PERIOD)
        node.joiningNodes.remove(joiningNode);
    }
  }

  /**
   * Times out Connections which aren't part of the network.
   */
  protected void timeoutLooseConnections() {
    if(activeConnections.size() == 0 && node.isConnected == false) {
      // note: if the Node thinks it's connected, don't try to reconnect

      // ok, we are completely disconnected, reconnect to the network
      node.print("reconnecting ...");
      node.connectToNetworkViaGateway();

    } else {
      long currentTime = System.currentTimeMillis();
      for (Enumeration i = activeConnections.elements(); i.hasMoreElements(); ) {
        Connection connection = (Connection) i.nextElement();

        // since the Connections are sorted, we don't need to cycle through all of them
        if (currentTime - connection.lastActivity < DEFAULT_PING_PERIOD)
          break;

        // this connection has timed out and it's loose, get rid of it
        if (connection.isConnectedToNetwork() == false) {
          node.print("loose connection to " +
                     connection.getRemoteAddress().getHostAddress() + ":" + connection.getRemotePort() + " timed out");
          connection.destroy();
          // check if we're completely disconnected from the network, if so set the flag
          if(activeConnections.size() == 0)
            node.isConnected = false;
        }
      }


    }

  }


  /**
   * Entry point of the Thread which PINGs and performs Connection timeouts.
   */
  public void maintenanceThread() {
    while (isRunning) {

      long start = System.currentTimeMillis();

      // coarse grained synchronization
      synchronized(this) {

        // time out unacknowledged connections
        for (Enumeration i = unacknowledgedConnections.elements(); i.hasMoreElements(); ) {
          Connection connection = (Connection) i.nextElement();
          connection.performTimeout();
        }

        // perform other timeout based actions
        timeoutLooseConnections();
        timeoutJoiningNodes();
        pingConnections();
      }

      try {
        // modify sleep period depending on work load
        long timeout = MAINTENANCE_PERIOD - (System.currentTimeMillis() - start);
        if(timeout > 0)
          Thread.sleep(timeout);
      } catch (InterruptedException e) {
        // ignore for now
      }

    }

  }

  /**
   * Removes the Connection from internal Connection lists and tables.
   * @param connection Connection
   */
  public void removeConnection(Connection connection) {
    connections.remove(connection.getName());

    unacknowledgedConnections.remove(connection);
    activeConnections.remove(connection);
  }

  /**
   * Destroys this manager.
   */
  public void stop() {
    // this should (eventually) kill both threads
    isRunning = false;

    if(socket != null) {
      socket.close();
      socket = null;
    }
  }

  //*** logging functions

   public void listConnections() {
     for (Enumeration i = activeConnections.elements(); i.hasMoreElements(); ) {
      Connection connection = (Connection) i.nextElement();
      node.print(connection.toString());
    }
   }
}

/**
 * A class for calling ConnectionManager methods in separate Threads.
 */
class ConnectionManagerThread extends Thread {
  public final static int TYPE_RECEIVE = 0;
  public final static int TYPE_MAINTENANCE = 1;

  ConnectionManager manager;
  int type;

  public ConnectionManagerThread(ConnectionManager manager, int type) {
    super(getName(type));
    this.manager = manager;
    this.type = type;
  }

  /**
   * Used to name Threads.
   * @param type int
   * @return String
   */
  public static String getName(int type) {
    switch(type) {
      case TYPE_RECEIVE:
        return "ConnectionManager.Receive";
      case TYPE_MAINTENANCE:
        return "ConnectionManager.Maintenance";
    }
    return "";
  }

  public void run() {
    switch(type) {
      case TYPE_RECEIVE:
        manager.receiveThread();
        break;
      case TYPE_MAINTENANCE:
        manager.maintenanceThread();
        break;
    }
  }
}
