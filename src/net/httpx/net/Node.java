package net.httpx.net;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Vector;
import java.util.Enumeration;
import java.io.IOException;
import java.io.FileInputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Random;

import net.httpx.util.MD5;
import net.httpx.Identifier;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.net.URL;
import java.net.URLConnection;
import java.io.File;

public class Node {

  public final static short VERSION_MAJOR = 0;
  public final static short VERSION_MINOR = 1;

  public final static String GATEWAY_NODES_FILE = "gateways.conf";

  Vector gatewayNodeAddresses = new Vector();

  /**
   * The address of the Node which we used or are using to connect to the network.
   * This is used for filtering incoming JOIN_COMPLETE messages.
   */
  NodeAddress currentGatewayAddress = null;

  /**
   * True if this node is part of the network.
   */
  public boolean isConnected = false;

  protected String name;
  protected Identifier id;

  protected InetAddress address;
  protected int port;

  /**
   * Set to true if this Node is behind a NAT of firewall.
   */
  protected boolean isRestricted = false;

  ConnectionManager connectionManager;
  RoutingTable routingTable;
  Vector joiningNodes = new Vector();

  public Node(int port) throws UnknownHostException {
    this.port = port;
    this.address = InetAddress.getLocalHost();
    this.name = address.getHostAddress() + ":" + port;

    // generate the id from the hashed name
    id = new Identifier(new MD5(name).Final());

    routingTable = new RoutingTable(this);
    connectionManager = new ConnectionManager(this);

    loadGatewayNodes(GATEWAY_NODES_FILE);
  }

  //private SimpleDateFormat dateFormat = new SimpleDateFormat("MMddHHmmss");
  private SimpleDateFormat dateFormat = new SimpleDateFormat("mmss");

  public void print(String message) {
    System.out.println("\r" + dateFormat.format(new Date()) + ": " + id.toShortString() + ": " + message);
  }

  public ConnectionManager getConnectionManager() {
    return connectionManager;
  }

  public RoutingTable getRoutingTable() {
    return routingTable;
  }

  /**
   * Called by a Connection when we receive a new set of data describing our
   * perceived address and port at a remote Node.
   */
  public void updateIsRestricted(byte[] address, int port) {
    // note: we can record our external address at this stage,
    // this is useful later for NAT punch through

    // if we've already determined that we are restricted, there's no point
    // in doing this calculation, since it's a one-way function
    if(!isRestricted) return;

    // get all the addresses of localhost
    InetAddress[] addresses = null;
    try {
      addresses = InetAddress.getAllByName(null);
      for(int i = 0; i < addresses.length; i ++) {
        if(!NodeAddress.doAddressesMatch(addresses[i].getAddress(),
            this.port, address, port)) {
          isRestricted = false;
          break;
        }
      }

    }
    catch (UnknownHostException e) {
      // ignore for now
    }
  }

  /**
   * Called when a Message the is received at this Node, the ID of this
   * Node must be the closest ID in the network to the destination of the message.
   *
   * @param message Message
   */
  public void receiveMessage(Message message) throws IOException {

    // read the header and fill in the cached variables before
    // we pass this message on
    message.readHeader();

    print("received: " + Message.typeToString(message.getType()) + " from: " +
          message.getSource().toShortString());

    // check the message type
    switch (message.type) {
      case Message.TYPE_ROUTING_REQUEST:
        processRoutingRequest(message);
        break;
      case Message.TYPE_ROUTING_RESPONSE:
        processRoutingResponse(message);
        break;
      case Message.TYPE_ROUTING_COMPLETE:
        processRoutingComplete(message);
        break;
        // called directly by the connection
      //case Message.TYPE_JOIN_RESPONSE:
      //  processJoinResponse(message);
      //  break;
      case Message.TYPE_FINGER:
        print("finger!");
        break;
    }
  }

  public void sendMessage(Message message) {
    try {
      routingTable.routeMessage(message);
    } catch(IOException e) {
      // we trap all the IOExceptions which result from problems with reading
      // the message or writing a new one here
      print("couldn't read message from: " + message.source.toShortString());
    }
  }

  public void sendRoutingRequest(Identifier id) {
    Message message = new Message(Message.TYPE_ROUTING_REQUEST, this.id, id);
    message.writeHeader();

    // start with hopcount 0, it's incremented before being sent along
    // note the hopcount is stored as a byte, because we never expect the number of
    // hops or routing table columns to exceed 128, since the Identifier size is 128 bits
    message.writeByte((byte)0);

    sendMessage(message);
  }

  public void modifyRoutingRequest(Message message) throws IOException {
    message.readHeader();
    byte hopCount = message.readByte();

    // write directly to the message buffer at position 0
    message.getData()[Message.FULL_HEADER_SIZE] = (byte)(hopCount + 1);

    //message.startWriting();
    //message.writeInt(hopCount + 1);
  }

  /**
   *
   * @param message Message
   */
  public void sendRoutingResponse(Message message) {
    // the Message header has already been read in

    boolean includeSelf = true;

    //message.startReading();
    //int hopCount = message.readInt();
    //boolean includeSelf = hopCount > 1;

    int commonDigitCount = RoutingTable.getNumberOfCommonDigits(id, message.getDestination());

    if (commonDigitCount == RoutingTable.DIGITS_PER_ID) {
      // we should never have ended up here, we must be in receiveMessage
      return;
    }

    Connection[] routingColumn = routingTable.entries[commonDigitCount];

    Message responseMessage = new Message(Message.TYPE_ROUTING_RESPONSE, id,
                                          message.source);
    responseMessage.writeHeader();

    //TODO include hopcount?

    // write the destination id, this is necessary since
    // the gateway node might be acting as gateway for more than
    // one Node at a time
    responseMessage.writeIdentifier(message.getDestination());

    for (int i = 0; i < routingColumn.length; i++) {
      if (routingColumn[i] != null) {
        Connection connection = routingColumn[i];

        responseMessage.write(connection.remoteAddress.getAddress());
        responseMessage.writeShort( (short) (connection.getRemotePort() &
                                             0xffff));
      }
    }

    // append ourselves to the list
    //TODO don't do this if a valid entry already exists
    if (includeSelf) {
      //TODO a more accurate name can be retrieved from one of the neighbours
      //TODO using IPv4
      byte[] address = null;
      try {
        address = InetAddress.getLocalHost().getAddress();
      } catch (UnknownHostException e) {
      }

      if(address != null) {
        responseMessage.write(address);
        responseMessage.writeShort( (short) (port & 0xffff));
      }
    }

    // since a single column on average contains 16 entries,
    // there's no need to split up the message into multiple parts
    sendMessage(responseMessage);

    //TODO ping the joining node
    // this is done to ensure that Nodes can join successfully from behind a NAT
  }

  public void processRoutingRequest(Message message) throws IOException {

    sendRoutingResponse(message);

    //note: header already read
    byte hopCount = message.readByte();

    // terminate the routing request
    Message completeMessage = new Message(Message.TYPE_ROUTING_COMPLETE, id,
                                          message.source);
    completeMessage.writeHeader();

    completeMessage.writeByte((byte)(hopCount + 1));
    completeMessage.writeIdentifier(message.getDestination());

    sendMessage(completeMessage);
  }

  public JoiningNode findJoiningNode(Identifier id) {
    // cycle through the nodes until we find the one with the matching id
    for (Enumeration i = joiningNodes.elements(); i.hasMoreElements(); ) {
      JoiningNode joiningNode = (JoiningNode) i.nextElement();
      if (joiningNode.id.equals(id))return joiningNode;
    }
    return null;
  }

  public void processRoutingResponse(Message message) throws IOException {
    // header already read
    Identifier id = message.readIdentifier();

    JoiningNode joiningNode = findJoiningNode(id);
    // make sure the joining Node exists
    // if it doesn't it has probably timed out
    if (joiningNode != null) {

      joiningNode.processRoutingResponse(message);
    }

  }

  public void processRoutingComplete(Message message) throws IOException {
    // header already read
    /*byte hopCount =*/ message.readByte(); // skip the hop count
    Identifier id = message.readIdentifier();

    //print("complete for: " + new UniqueId(id).toShortString());

    JoiningNode joiningNode = findJoiningNode(id);
    if (joiningNode != null) {
      joiningNode.processRoutingComplete(message);
    }
  }

  public void processJoinResponse(Message message) throws IOException {

    // once we've received at least one join response,
    // we can say we are connected to the HTTPX network
    this.isConnected = true;

    while (message.getReadPosition() < message.getSize()) {
      // IPv4
      byte[] address = new byte[4];

      message.read(address);
      int port = message.readShort();

      try {
        // connect to the give address and port
        connectionManager.connect(InetAddress.getByAddress(address), port);
      } catch(UnknownHostException e) {
        e.printStackTrace();
      }
    }
  }

  /**
   * Called before a message is actually sent.
   *
   * @param entry RoutingTableEntry
   * @param message Message
   */
  protected void forwardMessage(Connection connection, Message message) throws IOException {

    print("forwarding: " + Message.typeToString(message.type) + " from: " +
          message.getSource().toShortString() + " to: " +
          message.getDestination().toShortString());

    // check the message type
    switch (message.type) {
      case Message.TYPE_ROUTING_REQUEST:

        // we send a response even if we are not the destination Node
        sendRoutingResponse(message);
        modifyRoutingRequest(message);
        break;
    }

    connection.sendMessage(message);
  }

  /**
   * Initiates a join attempt. This function is called at the gateway Node.
   *
   * @param id UniqueId
   * @param connection Connection
   */
  public void initiateJoin(Identifier id, Connection connection) {
    JoiningNode joiningNode = new JoiningNode(this, id, connection);
    joiningNodes.add(joiningNode);

    sendRoutingRequest(id);
  }


  public void joinNetwork(NodeAddress address) {
    try {
      connectionManager.join(InetAddress.getByAddress(address.address), address.port);
      currentGatewayAddress = address;
    }
    catch (UnknownHostException e) {
      e.printStackTrace();
    }
  }

  /**
   * Connects to the distributed network via a gateway Node. The Node
   * is at random selected from the list of gateway Nodes. This list must
   * have been loaded prior to calling this function.
   */
  public void connectToNetworkViaGateway() {
    if(gatewayNodeAddresses.size() > 0) {
      Random random = new Random();
      int n = random.nextInt(gatewayNodeAddresses.size());
      NodeAddress nodeAddress = (NodeAddress) gatewayNodeAddresses.get(n);
      print("connecting via gw: " + nodeAddress);
      joinNetwork(nodeAddress);
    } else {
      // if the routing table is empty, start up in autonomous mode
      this.isConnected = true;
      print("starting in autonomous mode");
    }
  }

  public void start() {
    connectionManager.start();
  }

  public void stop() {
    connectionManager.stop();
  }


  /**
   * Loads a list of nodes and their addresses into the gateway list.
   * This list is then used for joining the network.
   */
  public void loadGatewayNodes(String filename) {
    try {
      FileInputStream fileIn = new FileInputStream(filename);
      BufferedReader reader = new BufferedReader(new InputStreamReader(fileIn));
      String line = null;
      while((line = reader.readLine()) != null) {
        String address;
        int port;
        // parse the line
        int i = line.indexOf(":");
        if (i == -1) {
          address = line;
          port = ConnectionManager.DEFAULT_PORT;
        }
        else {
          address = line.substring(0, i);
          try {
            port = Integer.parseInt(line.substring(i + 1));
          }
          catch (NumberFormatException e) {
            port = ConnectionManager.DEFAULT_PORT;
          }
        }
        // add the address to the list
        NodeAddress nodeAddress = NodeAddress.getAddress(address, (short) port);
        if (nodeAddress != null)
          gatewayNodeAddresses.add(nodeAddress);
      }

      print("loaded " + gatewayNodeAddresses.size() + " gw node(s)");

      reader.close();
      fileIn.close();

    } catch(Exception e) {
      print("couldn't load gw nodes from: " + filename);
    }

  }

  public static void printBanner() {
    System.out.println("httpx: distributed web cache node: v" + VERSION_MAJOR + "." + VERSION_MINOR);
  }

  protected void processCommands() {
    BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
    String line = null;
    try {
      while ( (line = reader.readLine()) != null) {
        if(line.startsWith("exit")) break;
        if(line.startsWith("list"))           // list connections
          connectionManager.listConnections();
        if(line.startsWith("export"))
          routingTable.exportGatewayNodes(line.substring("export".length()).trim());

      }
    } catch(IOException e) {
      // ignore
    }
  }

  /**
   * Checks to see if there's an updated version available.
   * @return boolean
   */
  public static boolean checkForUpdates(String url, String jarname) {
    try {
      URLConnection connection = new URL(url + jarname).openConnection();
      int newLength = connection.getContentLength();
      File file = new File ("./" + jarname);
      if(file.exists()) {

        int currentLength = (int) file.length();

        // this is a bit of a hack, but should still work fairly well
        if (newLength != currentLength)
          return true;
      }

    } catch(Exception e) {
      e.printStackTrace();
      // fall through
    }
    return false;
  }

  /**
   * Launches the system browser and points to the specified URL.
   * @param url String
   */
  public static void launchBrowser(String url) {
    try {
      //note: windows specific
      Runtime.getRuntime().exec
          ("rundll32 url.dll,FileProtocolHandler " + url);
    }
    catch (IOException e) {
      // fail silently
    }
  }


  public static void test() throws Exception {
    Node nodeA = new Node(8000);
    nodeA.isConnected = true;
    nodeA.start();

    Thread.sleep(1000);

    Node nodeB = new Node(8001);

  //  nodeB.getConnectionManager().connect(InetAddress.getLocalHost(), 8000);
    //nodeB.joinNetwork(InetAddress.getLocalHost().getHostAddress(), 8000);
    //nodeB.joinNetwork("127.0.0.1", 8000);
    nodeB.connectToNetworkViaGateway();
    nodeB.start();

    while(true) {
      Thread.sleep(2000);
      if(nodeB.isConnected) {
        Message message = new Message(Message.TYPE_FINGER, nodeB.id, nodeA.id);
        message.writeHeader();
        nodeB.sendMessage(message);
        break;
      }
    }
  }


  public static void main(String[] args) throws Exception {
    
 /* 
   final String link = "http://labs.ionsquare.com/httpx/";  
   if(checkForUpdates(link, "httpx.jar")) {
      System.out.println("new version available! download it from: " + link);
      launchBrowser(link + "update.html");
      return;
    }
*/
    printBanner();

    Node nodeA = new Node(8000);
    nodeA.isConnected = true;
    nodeA.start();

    Thread.sleep(1000);


    Node nodeB = new Node(8001);
    nodeB.connectToNetworkViaGateway();
    nodeB.start();

    int i = 2;
    while(true) {
      Thread.sleep(2000);
      Node nodeC = new Node(8001 + i);
      nodeC.connectToNetworkViaGateway();
      nodeC.start();

      if(nodeB.isConnected) {
        Message message = new Message(Message.TYPE_FINGER, nodeB.id, nodeA.id);
        message.writeHeader();
        nodeB.sendMessage(message);
        break;
      }
    }

    nodeB.processCommands();

    nodeB.stop();
    nodeA.stop();


  //  Thread.sleep(2000);
  //  nodeB.destroy();

  // run a command line
/*


    Node gatewayNode = new Node(8000);
    gatewayNode.isConnected = true;
    gatewayNode.start();

    Vector nodes = new Vector();
    nodes.add(gatewayNode);

    Random r = new Random();
    for(int i = 0; i < 32; i ++) {
      Node node = new Node(8001 + i);
      // select a random node to connect to
      node.joinNetwork(InetAddress.getLocalHost().getHostAddress().getBytes(), 8000 + r.nextInt((i + 1)));
      try {
        Thread.sleep(2000);
      }
      catch (InterruptedException e) {
      }
      nodes.add(node);
    }

    ((Node)nodes.lastElement()).sendMessage(new Message(Message.TYPE_PING, ((Node)nodes.lastElement()).id, gatewayNode.id));
*/
  }

}
