package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;

import android.content.*;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

/*  Main class of the program. Class inherits ContentProvider and overrides some of the methods.
* */
public class SimpleDynamoProvider extends ContentProvider {

    /* Uri */
    public static final Uri uri = Uri.parse("content://edu.buffalo.cse.cse486586.simpledynamo.provider");

    /* For Logger and Debugging purposes. */
    static final String TAG = "TAG";

    /* Gets the status is the entries have been recovered from failure or instance kill */
    private boolean isRecovered;

    /*  Create an Object for SqlHelper class which extends SQLite Helper class. */
    private SqlHelper mydb;

    /*  Creates an instance */
    private SQLiteDatabase database;

    /* Variables for the emulator's port types.*/
    private String portStr, myPort;
    private String[] cols;

    /* Variables with Cursors. */
    private MatrixCursor tCursor, matrixCursor;

    /* HashMaps needed declaration*/
    private HashMap<String, ArrayList<String>> waitInsertMap;
    private HashMap<String, Boolean> waitQueryMap;
    private HashMap<String, String> cursorMap, portKeyMap;

    /*  Contains the list of all the present emulator's ports. */
    private ArrayList<String> nodePeerList;

    /* Variables to maintain the 3 nodes ( coordinator and its two successors. queryCount used to maintain quorum. */
    private int coordinator, successor1, successor2, queryCount;

    /*
    * This is a SimpleDynamoProvider constructor which sets up the variables and datastructures
    * needed.
    * */
    public SimpleDynamoProvider() {
        setInitCursors();
        setInitHashMap();
        this.isRecovered = false;
        this.queryCount = 0;
    }

    /*
    * This is called internally from SimpleDynamoProvider() which sets all the Given node list
    * along with HashMaps needed in the program.
    * */
    public void setInitHashMap() {
        this.waitInsertMap = new HashMap<String, ArrayList<String>>();
        this.waitQueryMap = new HashMap<String, Boolean>();
        this.cursorMap = new HashMap<>();
        this.portKeyMap = setPortKeyMap();
        this.nodePeerList = setNodePeerList();
    }

    /* Number of Android emulator instance are given as fixed (ie 5) with their port Id.
    *  Lexicographically sorts the Peer List using Collection.sort();
    *  returns the final resultant NodePeerList
    * */
    public ArrayList<String> setNodePeerList() {
        ArrayList<String> nodePeerList = new ArrayList<String>(5);
        try {
            nodePeerList.add(genHash("5554"));
            nodePeerList.add(genHash("5556"));
            Collections.sort(nodePeerList);
            nodePeerList.add(genHash("5558"));
            Collections.sort(nodePeerList);
            nodePeerList.add(genHash("5560"));
            Collections.sort(nodePeerList);
            nodePeerList.add(genHash("5562"));
            Collections.sort(nodePeerList);
        } catch (NoSuchAlgorithmException e) {
            e.getMessage();
        } finally {
            return nodePeerList;
        }
    }

    /*  Gets all the ports and its corresponding HashCode(). Stores this in a HashMap<String,String>.
    *   Finally returns the HashMap<>();
    * */
    public HashMap<String, String> setPortKeyMap() {
        HashMap initMap = new HashMap<>();
        try {
            portKeyMap.put(genHash("5554"), "5554");
            portKeyMap.put(genHash("5556"), "5556");
            portKeyMap.put(genHash("5558"), "5558");
            portKeyMap.put(genHash("5560"), "5560");
            portKeyMap.put(genHash("5562"), "5562");
        } catch (NoSuchAlgorithmException e) {
            e.getMessage();
        } finally {
            return initMap;
        }
    }

    /*  Initialize cursors that are needed.
    * */
    public void setInitCursors() {
        this.cols = new String[]{"key", "value"};
        this.matrixCursor = new MatrixCursor(cols);
        this.tCursor = new MatrixCursor(cols);
    }

    /*  Delete operation gets an input as String selection which contains
    *   '@' ( request to delete all entries in the Application's local database)
    *   OR "*" ( request to delete all entries from all the applications connected to the network)
    *   OR Specific key ( which requests to delete that specific key of String type to be deleted from all the application's content providers)
    * */

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        boolean success = requestLocalOrAllDatabaseDelete(selection);
        if (!success) {
            requestSpecificEntryDatabaseDelete(selection);
        }
        return 1;
    }

    /* Requests to application to delete a specific key from all instance's databases. */
    private int requestSpecificEntryDatabaseDelete(String selection) {
        try {
            String keyHash = genHash(selection);
            for (int nodes = 0; nodes < 5; nodes++) {
                if ((keyHash.compareTo(nodePeerList.get(nodes)) < 0)) {
                    makeDeletePacketToSend(nodes, selection);
                    break;
                } else if ((keyHash.compareTo(nodePeerList.get(nodes)) > 0) && (keyHash.compareTo(nodePeerList.get(4))) > 0) {
                    makeDeletePacketToSend(nodes, selection);
                    break;
                }
            }
        } catch (NoSuchAlgorithmException e) {
            e.getMessage();
        } finally {
            return 1;
        }
    }

    /* Check if @ (delete all entries in local database) or * (delete all entries from all database is demanded).
    *  Takes appropriate actions by deleting from that database. Return true on successful deletion from database.
    * */
    private boolean requestLocalOrAllDatabaseDelete(String selection) {

        boolean requestSuccess = false;
        if (selection.equals("\"@\"")) {
            database.delete(SqlHelper.TABLE_NAME, "1", null); //dao layer abstraction
            requestSuccess = true;
        } else if (selection.equals("\"*\"")) {
            deleteStar(selection);
            requestSuccess = true;
        }
        return requestSuccess;
    }

    /*  Invoked on a request to delete a specific key from the content providers.
    *   Creates a messageList packet. Creates a new ClientTask Thread to send that message over the socket.
    * */
    private void makeDeletePacketToSend(int coordinatorId, String selection) {

        FindAndSetDestinationIDs findSetIds = new FindAndSetDestinationIDs();
        int[] idList = findSetIds.setDestinationIds(coordinatorId);
        ArrayList<String> toSend = bundleDeleteKey(selection, idList);
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, toSend);
    }

    /* Creates a packet with fields for Delete_Key kind of a packet.
    *   Returns the packet as an ArrayList.
    * */
    private ArrayList bundleDeleteKey(String selection, int[] idList) {

        ArrayList<String> toSend = new ArrayList<>();
        toSend.add("Delete_Key");
        toSend.add(portKeyMap.get(nodePeerList.get(idList[0])));
        toSend.add(portKeyMap.get(nodePeerList.get(idList[1])));
        toSend.add(portKeyMap.get(nodePeerList.get(idList[2])));
        toSend.add(selection);
        toSend.add("Garbage");
        toSend.add(portStr);
        return toSend;
    }

    /*  Creates a MessageList packet with deleteStar Request.
    *   Starts a new AsyncTask thread with this newly Bundled packet
    * */
    private void deleteStar(String selection) {
        for (int nodeNumber = 0; nodeNumber < 5; nodeNumber++) {
            ArrayList<String> toSend = new ArrayList<String>();
            toSend.add("Delete_Star");
            toSend.add(portKeyMap.get(nodePeerList.get(nodeNumber)));
            toSend.add(selection);
            toSend.add(portStr);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, toSend);
        }
    }

    @Override
    public String getType(Uri uri) {
        return null;
    }

    /*  Processes the request to insert the key into the respective databases.
    *   Finds the destination where the key belongs using SSH hash value.
    *   Bundles the MessageList Packet and sends it through a Serial Executor AsyncTask thread.
    *   Wait until insert quorum successful.
    * */
    @Override
    public Uri insert(Uri uri, ContentValues values) { //Packet Class
        try {
            ArrayList<String> toSend;
            String keyHash = genHash((String) values.get("key"));
            for (int i = 0; i < 5; i++) {
                if ((keyHash.compareTo(nodePeerList.get(i)) < 0)) {
                    toSend = makeInsertPacketToSend(i, values);
                    insertPacketAndWait(toSend);
                    break;
                } else if ((keyHash.compareTo(nodePeerList.get(i)) > 0) && (keyHash.compareTo(nodePeerList.get(4))) > 0) {
                    toSend = makeInsertPacketCornerCase(values);
                    insertPacketAndWait(toSend);
                    break;
                }
            }
            return uri;
        } catch (NoSuchAlgorithmException e) {
            e.getMessage();
        } finally {
            return uri;
        }
    }

    /*  Bundles a messageList packet to send. Called when packet type is Insert a specific key operation.
     * */
    private ArrayList makeInsertPacketToSend(int coordinatorId, ContentValues values) {
        ArrayList<String> toSend = new ArrayList<>();
        FindAndSetDestinationIDs insertListIds = new FindAndSetDestinationIDs();
        int[] list = insertListIds.setDestinationIds(coordinatorId);
        toSend.add("Insert_First");
        toSend.add(portKeyMap.get(nodePeerList.get(list[0])));
        toSend.add(portKeyMap.get(nodePeerList.get(list[1])));
        toSend.add(portKeyMap.get(nodePeerList.get(list[2])));
        toSend.add((String) values.get("key"));
        toSend.add((String) values.get("value"));
        toSend.add(portStr);
        return toSend;
    }

    /* Deals with a Corner case when the keyHash generated is the greatest than any node. Hence compareTo gives
    *  that all the nodePeerList comparison are greater than inserted key.
    * */
    private ArrayList makeInsertPacketCornerCase(ContentValues values) {
        ArrayList<String> toSend = new ArrayList<>();
        coordinator = 0;
        successor1 = 1;
        successor2 = 2;
        toSend.add("Insert_First");
        toSend.add(portKeyMap.get(nodePeerList.get(coordinator)));
        toSend.add(portKeyMap.get(nodePeerList.get(successor1)));
        toSend.add(portKeyMap.get(nodePeerList.get(successor2)));
        toSend.add((String) values.get("key"));
        toSend.add((String) values.get("value"));
        toSend.add(portStr);
        return toSend;
    }

    /*   Calls the insert messageList packet sending it through a new thread.
    *   Creates the Quorum wait entry in to the Hashmap
    *   Waits until the two out of three members reply with insertSuccess notification.
    * */
    private void insertPacketAndWait(ArrayList toSend) {
        String sendKey = toSend.get(4) + "#" + portStr;
        waitInsertMap.put(sendKey, new ArrayList<String>());
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, toSend);
        while (waitInsertMap.get(toSend.get(4) + "#" + portStr).size() < 2) {
        }
    }

    /* Gets the port mapping. An hack to get the emulator's port and then convert to suitable address format.
    * */
    private String getPortMapping() {
        TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
        portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        return myPort;
    }

    /*  Called at the very start of the program. When an instance is newly created at the beggining,
    *   this method sets up the database, gets ports needed, Recovers the data is reviving from a instance kill
    *   and then calls the ServerTask thread which lets the instance receive any requests from connected instances.
    * */
    @Override
    public boolean onCreate() {

        /*  Sets up Database and gets the this instance's port number in the form of string*/
        mydb = new SqlHelper(getContext());
        database = mydb.getWritableDatabase();
        String myPort = getPortMapping();

        /*  Creates a MessageList to request Node recovery and starts a new AsyncTask thread to initiate the Recovery*/
        ArrayList<String> toSend = new ArrayList<>();
        toSend.add("NodeRecovery");
        new RecoverTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, toSend);

        /*  Gives other threads to become live again and gather all the entries to faster recovery */
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            e.getMessage();
        }

        /*  On successful recovery calls ServerTask AsyncTask thread which can now accept
        *   all the incoming requests from other emulator
        **/
        try {
            ServerSocket serverSocket = new ServerSocket(10000);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "onCreate: Can't create a ServerSocket");
        }
        if (database != null)
            return true;
        else
            return false;
    }

    /* Recovery of data is initiated first with the two successor for its own data.
    *   Then the recovery is initiated second with two predecessor which would have the replicas of other emulators.
    * */
    private void recoverAllData(String[] successors, String[] predecessors) {

        int mode = 0;
        isRecovered = true;
        while (mode < 2) {
            Log.d(TAG, "mode :" + mode);
            ArrayList<String> toSend = new ArrayList<>();
            if (mode == 0) {
                toSend.add("SuccessorMode");
                toSend.add(portStr);
            } else if (mode == 1) {
                toSend.add("RecoverMode");
                toSend.add("PredecessorMode");
                toSend.add(portStr);
            }
            for (int i = 0; i <= 1; i++) {
                Socket socket = null;
                try {
                    if (mode == 0) {

                    } else if (mode == 1) {
                        socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(predecessors[i]) * 2);
                    }
                    ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                    out.writeObject(toSend);
                    out.flush();
                    ObjectInputStream input = new ObjectInputStream(socket.getInputStream());
                    List<String> messageList1 = new ArrayList<>();
                    messageList1 = (List<String>) input.readObject();
                    for (int j = 0; j < messageList1.size(); j++) {
                        String pair = messageList1.get(j);
                        String pairs[] = pair.split(";");
                        ContentValues cvalues = new ContentValues();
                        cvalues.put("key", pairs[0]);
                        cvalues.put("value", pairs[1]);
                        database.insert(SqlHelper.TABLE_NAME, null, cvalues);
                    }
                    input.close();
                    out.close();
                    socket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                }
            }
            mode++;
        }
    }

    /*   Overrides the Content Provider method which serves the operation like query for a specific key,
    *    Query for all entries in this local databases and query all the entries from all the databases across the network.
    * */
    @Override
    public Cursor query(Uri uri, String[] projection, String selection,
                        String[] selectionArgs, String sortOrder) {

        String args[] = {selection};
        if (selection.contains("@")) {
            return queryAllLocalEntries(selection);
        } else if (selection.contains("*")) {
            return queryAllEntriesFromAll(sortOrder, selection);
        } else {
            /*Specific query block*/
            return querySpecificEntry(selection);
        }
        /*Cursor cursor = database.query(SqlHelper.TABLE_NAME, projection, SqlHelper.Keyval + "=?", args, null, null, null);
        return cursor;*/
    }

    /*  Finds the destination where the query might belong to from all the databases.
    *   Creates a packet with messageList to query specific key and send to newly found destination.
    *   Waits until the quorum notification is received from the destination port along with the entries inside a cursor.
    * */
    private MatrixCursor querySpecificEntry(String selection) {
        String keyHash;
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        if (isRecovered == false) {
        }
        try {
            keyHash = genHash(selection);
            String queryDestination;
            MatrixCursor tCursor = new MatrixCursor(cols);
            for (int i = 0; i < 5; i++) {
                if ((keyHash.compareTo(nodePeerList.get(i)) < 0)) {
                    String qHashkey = nodePeerList.get(i);
                    queryDestination = portKeyMap.get(qHashkey);
                    tCursor = bundleQuerySpecificThenSendAndWait(selection, queryDestination);
                    break;
                } else if ((keyHash.compareTo(nodePeerList.get(i)) > 0) && (keyHash.compareTo(nodePeerList.get(4))) > 0) {
                    String qHashkey = nodePeerList.get(0);
                    queryDestination = portKeyMap.get(qHashkey);
                    tCursor = bundleQuerySpecificThenSendAndWait(selection, queryDestination);
                    break;
                }
            }
            printMatrixCursor(tCursor);
            return tCursor;
        } catch (NoSuchAlgorithmException e) {
            e.getMessage();
        }
        return null;
    }

    /*  Perform query star "*" operation for retrieving all the from the emulator's databases.
    * */
    private MatrixCursor queryAllEntriesFromAll(String sortOrder, String selection) {
        if (sortOrder == null) {
            matrixCursor = new MatrixCursor(cols);
            sendQueryStarToAll(selection);
            waitUntilQueryStarReturn();
            printMatrixCursor(matrixCursor);
            return matrixCursor;
        } else {
            bundleQueryStarAndSend(sortOrder);
            return null;
        }
    }

    /* Performs the @ operation to retrieve all the key value pair from my current and local emulator's database.
    * */
    private Cursor queryAllLocalEntries(String selection) {
        matrixCursor = new MatrixCursor(cols);
        return database.rawQuery("Select * from " + SqlHelper.TABLE_NAME, null);

    }

    /*  On receiving the query request the ServerTask thread reads the messageList packet to bundle a Query Complete messageList.
    *   Returns the ArrayList.
    *  */
    private ArrayList QueryMyDatabaseAndSendPacket(Uri uri, String[] projection, String selection, String origin) {
        String[] args = {selection};
        Cursor cursor = database.query(SqlHelper.TABLE_NAME, projection, SqlHelper.Keyval + "=?", args, null, null, null);
        ArrayList<String> toSend = new ArrayList<>();
        toSend.add("Query_Complete");
        toSend.add(origin);
        cursor.moveToFirst();
        String sendKey = cursor.getString(cursor.getColumnIndex("key"));
        String sendVal = cursor.getString(cursor.getColumnIndex("value"));
        toSend.add(sendKey);
        toSend.add(sendVal);
        toSend.add(portStr);
        return toSend;
    }

    /*  Creates a QueryKey operation messageList, creates a waiting hashmap to maintain quorum,
    *   calls a new thread for sending for ClientTask.
    *   Returns the cursor on retrieving the entries for that specific key query.
    * */
    private MatrixCursor bundleQuerySpecificThenSendAndWait(String selection, String queryDestination) {
        ArrayList<String> toSend = new ArrayList<>();
        MatrixCursor tempCursor = new MatrixCursor(cols);
        toSend.add("Query_Key");
        toSend.add(queryDestination);
        toSend.add(selection);
        toSend.add(portStr);
        String sendQkey = toSend.get(2) + "#" + queryDestination;
        waitQueryMap.put(sendQkey, false);
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, toSend);
        while (!waitQueryMap.get(sendQkey)) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        String cursorKey = selection;
        String cursorValue = cursorMap.get(selection);
        tempCursor.addRow(new Object[]{cursorKey, cursorValue});
        return tempCursor;
    }


    /* Creates a QueryComplete operation messageList,then a notification, creates a waiting HashMap to maintain quorum,
    *   calls a new thread for sending for ClientTask.
    *   Returns the cursor on retrieving the entries for that Star Complete "*" query.
    * */
    private ArrayList bundleQueryStarAndSend(String sortOrder) {
        ArrayList<String> toSend = new ArrayList<>();
        toSend.add("Query_StarComplete");
        toSend.add(sortOrder);
        Cursor cursor = database.rawQuery("Select * from " + SqlHelper.TABLE_NAME, null);
        cursor.moveToFirst();
        while (cursor.isAfterLast() == false) {
            String sendKey = cursor.getString(cursor.getColumnIndex("key"));
            String sendVal = cursor.getString(cursor.getColumnIndex("value"));
            toSend.add(sendKey + "," + sendVal);
            cursor.moveToNext();
        }
        return toSend;
    }

    /*  requests a query operation for retrieving all the entries from all the emulators in the network.
    * */
    private void sendQueryStarToAll(String selection) {
        for (int i = 0; i < 5; i++) {
            ArrayList<String> toSend = new ArrayList<String>();
            toSend.add("Query_Star");
            toSend.add(portKeyMap.get(nodePeerList.get(i)));
            toSend.add(selection);
            toSend.add(portStr);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, toSend);
        }
    }


    /*  Waits for all the recovery to complete. Successful recovery means retrieving entries
    *   from node's two successors and two predeccessors.
    * */
    private void waitUntilQueryStarReturn() {
        while (queryCount < 4) {
        }
        queryCount = 0;
    }

    /*  Finds the successor of the current node.
    *   Sends the successors with the new messsageList with a Failed Mode marker message.
    *   Purpose is to recover all the data from its both successor.
    * */
    private void queryRecoveryFromSuccessor(ArrayList<String> msgs, String destinationPort) {
        ArrayList<String> msgFailedQuery = new ArrayList<>();
        msgFailedQuery = msgs;
        String[] successor = findSuccessors(destinationPort);
        ArrayList<String> toSend = new ArrayList<>();

        toSend.add("QueryKeyFailedMode");
        toSend.add(successor[0]); //successor
        toSend.add(msgFailedQuery.get(2));//key
        toSend.add(destinationPort);//origin

        initiateQuerySuccessor(toSend, msgFailedQuery);

    }

    /*  Sends the query key operation (failed mode) to send out messagePacket.
    *   Maintains the waiting map. makes the status = true or success.
    * */
    private void initiateQuerySuccessor(ArrayList<String> msgFailedQuery, ArrayList<String> toSend) {
        try {
            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(toSend.get(1)) * 2);
            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
            out.writeObject(toSend);
            out.flush();
            ObjectInputStream input = new ObjectInputStream(socket.getInputStream());
            ArrayList<String> messageList = (ArrayList<String>) input.readObject();
            String keyQuery = messageList.get(2);
            String keyValue = messageList.get(3);
            String senderPort = messageList.get(4); //sender's port
            String coordinator = msgFailedQuery.get(1);
            String Qkey = keyQuery + "#" + coordinator;
            cursorMap.put(keyQuery, keyValue);
            waitQueryMap.put(Qkey, true);
            out.close();
            input.close();
            socket.close();
        } catch (IOException e) {
            e.getMessage();
        } catch (ClassNotFoundException e) {
            e.getMessage();
        }
    }

    /*  InsertPacket sent to all concerned nodes. ( coordinator and two successors).
    *   Maintains the waitInsertMap for quorum of verifying getting the entry complete notification.
    * */
    private void insertToNodes(ArrayList<String> msgs, int k) { //check for duplicate code

        ArrayList<String> msgInsert = new ArrayList<>();
        int m = k;
        msgInsert = msgs;

        Socket socket = null;
        try {
            socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(msgInsert.get(m)) * 2);
            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
            out.writeObject(msgInsert);
            out.flush();
            ObjectInputStream input = new ObjectInputStream(socket.getInputStream());
            ArrayList<String> messageList = (ArrayList<String>) input.readObject();
            String iKey = messageList.get(2) + "#" + messageList.get(1);
            String destinationPort = messageList.get(3);
            waitInsertMap.get(iKey).add(destinationPort);
            input.close();          //finally
            out.close();
            socket.close();
        } catch (IOException e) {
            e.printStackTrace(); //Logger.error
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    /*  Sends the request to perform delete operation. Sends the messageList through the socket
    *   to the emulators.
    * */
    private void deleteToNodes(ArrayList<String> msgs, int k) {

        ArrayList<String> msgInsert = new ArrayList<>();
        int m = k;
        msgInsert = msgs;
        Socket socket = null;
        try {
            socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(msgInsert.get(m)) * 2);
            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());

            out.writeObject(msgInsert);
            out.flush();
            out.close();
            socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /*  On successful reception of query messageList, the receiver reads the packet and sets true boolean
    *   for that specific entry from the source port.
    * */
    private int queryToNode(ArrayList<String> msgs) {
        Socket socket;
        ArrayList<String> msgQuery = new ArrayList<>();
        msgQuery = msgs;
        try {
            socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(msgQuery.get(1)) * 2);
            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
            out.writeObject(msgQuery);
            out.flush();
            ObjectInputStream input = new ObjectInputStream(socket.getInputStream());
            ArrayList<String> messageList = (ArrayList<String>) input.readObject();
            String keyQuery = messageList.get(2);
            String keyValue = messageList.get(3);
            String senderPort = messageList.get(4); //sender's port
            String coordinator = msgQuery.get(1);
            String Qkey = keyQuery + "#" + coordinator;
            waitQueryMap.put(Qkey, true);
            cursorMap.put(keyQuery, keyValue);

        } catch (IOException e) {
            return 0;
        } catch (ClassNotFoundException e) {
            e.getMessage();
        }
        return 1;
    }

    /*  The node List is given and fixed and hence is given hardcoded.
    *   Successors are assigned.
    * */
    private String[] findSuccessors(String port) {
        String[] successor = new String[2];
//        String[] idsOfNodesPresent = {"5562", "5556", "5554", "5558", "5560"};

        if (port.equals("5562")) {
            successor[0] = "5556";
            successor[1] = "5554";
        } else if (port.equals("5556")) {
            successor[0] = "5554";
            successor[1] = "5558";
        } else if (port.equals("5554")) {
            successor[0] = "5558";
            successor[1] = "5560";
        } else if (port.equals("5558")) {
            successor[0] = "5560";
            successor[1] = "5562";
        } else if (port.equals("5560")) {
            successor[0] = "5562";
            successor[1] = "5556";
        }
        return successor;
    }

    /*   The node List is given and fixed and hence is given hardcoded.
    *   Successors are assigned.
    * */
    private String[] findPredecessors(String port) {
        String[] predecessor = new String[2];
        if (port.equals("5562")) {
            predecessor[0] = "5560";
            predecessor[1] = "5558";
        } else if (port.equals("5556")) {
            predecessor[0] = "5562";
            predecessor[1] = "5560";
        } else if (port.equals("5554")) {
            predecessor[0] = "5556";
            predecessor[1] = "5562";
        } else if (port.equals("5558")) {
            predecessor[0] = "5554";
            predecessor[1] = "5556";
        } else if (port.equals("5560")) {
            predecessor[0] = "5558";
            predecessor[1] = "5554";
        }
        return predecessor;
    }


    /*  Works in two modes: Predecessor (Mode 1) and (Mode 2)
    *   Fires a raw query to the database. retrieves the key value pairs.
    *   Bundles them inside the messageList with ";" as delimiter character.
    * */
    private List getRecoveredEntries(ArrayList messageList) {
        List<String> toSend = new ArrayList<>();
        List<String> message = new ArrayList<>();
        String myHash = null;
        message = messageList;
        String[] predecessors = null;
        try {
            if (message.get(1).equals("SuccessorMode")) {
                myHash = genHash(message.get(2));
                predecessors = findPredecessors(message.get(2));
            } else if (message.get(1).equals("PredecessorMode")) {
                myHash = genHash(portStr);
                predecessors = findPredecessors(portStr);
            }

            Cursor cursor = database.rawQuery("Select * from " + SqlHelper.TABLE_NAME, null);
            cursor.moveToFirst();
            while (cursor.isAfterLast() == false) {
                String sendKey = cursor.getString(cursor.getColumnIndex("key"));
                String sendVal = cursor.getString(cursor.getColumnIndex("value"));
                String keyHash = genHash((sendKey));
                String predHash = genHash(predecessors[0]);
                if (keyHash.compareTo(predHash) > 0 && (keyHash.compareTo(myHash) <= 0)) {
                    toSend.add(sendKey + ";" + sendVal);
                } else if (keyHash.compareTo(predHash) <= 0 && keyHash.compareTo(myHash) <= 0 && predHash.compareTo(myHash) > 0) {
                    toSend.add(sendKey + ";" + sendVal);
                } else if (keyHash.compareTo(predHash) > 0 && predHash.compareTo(myHash) > 0) {
                    toSend.add(sendKey + ";" + sendVal);
                }
                cursor.moveToNext();
            }
        } catch (NoSuchAlgorithmException e) {
            e.getMessage();
        }
        return toSend;
    }


    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        return 0;
    }

    /*
    *   For debugging purposes and printing the retrieved entries from matrix cursor.
    * */
    private void printMatrixCursor(MatrixCursor matrixCursor) {
        matrixCursor.moveToFirst();
        while (matrixCursor.isAfterLast() == false) {
            String data = matrixCursor.getString(matrixCursor.getColumnIndex("key"));
            String col = matrixCursor.getString(matrixCursor.getColumnIndex("value"));
            matrixCursor.moveToNext();
        }
    }

    /*
    * SSH hashcode method to generate an hash from the key.
    * */
    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    /*
    *   Thread called in the onCreate() to recover all the entries after revival of failed node.
    * */
    private class RecoverTask extends AsyncTask<ArrayList<String>, Void, Void> {
        @Override
        protected Void doInBackground(ArrayList<String>... msgs) {
            isRecovered = false;
            String[] successors = new String[2];
            String[] predecessors = new String[2];
            //finding the successors and predecessors
            successors = findSuccessors(portStr);
            predecessors = findPredecessors(portStr);
            recoverAllData(successors, predecessors);
            return null;
        }
    }

    /*
    *   ClientTask spins a new thread which sends the MessageList packet for various operation to the other
    *   emulator instance in the network.
    * */
    private class ClientTask extends AsyncTask<ArrayList<String>, Void, Void> {
        @Override
        protected Void doInBackground(ArrayList<String>... msgs) {

            String msgToSend = msgs[0].get(0);

            switch (msgToSend) {
                case "Insert_First":
                    requestInsertKey(msgs[0]);
                    break;
                case "Query_Key":
                    requestQueryKey(msgs[0]);
                    break;
                case "Query_Star":
                    requestQueryStar(msgs[0]);
                    break;
                case "Query_StarComplete":
                    requestQueryComplete(msgs[0]);
                    break;
                case "Delete_Star":
                    requestDeleteStar(msgs[0]);
                    break;
                case "Delete_Key":
                    requestDelete_Key(msgs[0]);
                    break;
            }
            return null;
        }
    }

    /*  Invoked when a request to delete key is needed. Deletes the entry from coordinator as well as two successors.
    * */
    private void requestDelete_Key(ArrayList<String> MessageBean) {
        for (int k = 1; k <= 3; k++) {
            deleteToNodes(MessageBean, k);
        }
    }

    /*  Invoked when a request to delete all entries from all the instances in demanded.
    *   Coordinators and two successor remove all the entries totally.
    * */
    private void requestDeleteStar(ArrayList<String> MessageBean) {
        try {
            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(MessageBean.get(1)) * 2);
            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
            out.writeObject(MessageBean);
            out.close();
        } catch (UnknownHostException e) {
            e.getMessage();
        } catch (IOException e) {
            e.getMessage();
        }
    }

    /*
    *   sends back the notification that a request to query key has been completed successfully.
    * */
    private void requestQueryComplete(ArrayList<String> MessageBean) {
        try {
            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(MessageBean.get(1)) * 2);
            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
            out.writeObject(MessageBean);
            out.close();
        } catch (UnknownHostException e) {
            e.getMessage();
        } catch (IOException e) {
            e.getMessage();
        }

    }

    /*
    *   Notify the successful Query Star * operation on the receiver's side back to the sender.
    * */
    private void requestQueryStar(ArrayList<String> messageBean) {

        try {
            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(messageBean.get(1)) * 2);
            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
            out.writeObject(messageBean);
            out.flush();
            ObjectInputStream input = new ObjectInputStream(socket.getInputStream());
            ArrayList<String> messageList = (ArrayList<String>) input.readObject();
            for (int i = 2; i < messageList.size(); i++) {
                String pair = messageList.get(i);
                String pairs[] = pair.split(",");
                matrixCursor.addRow(pairs);
            }
            queryCount++;
        } catch (IOException e) {
            e.getMessage();
        } catch (ClassNotFoundException e) {
            e.getMessage();
        }
    }

    /* Sends request to query a specific key to the respective coordinators and its successors.
    * */
    private void requestQueryKey(ArrayList<String> msgs) {
        String destinationPort = msgs.get(1);
        int isCoordinatorPresent = queryToNode(msgs);
        if (isCoordinatorPresent == 0) {
            queryRecoveryFromSuccessor(msgs, destinationPort);
        }
    }

    /*Sends request to insert a specific key to the respective coordinators and its successors.
    * */
    private void requestInsertKey(ArrayList<String> msgs) {

        for (int k = 1; k <= 3; k++) {
            if (msgs.get(k).equals(portStr)) {
                ContentValues cvalues = new ContentValues();
                cvalues.put("key", msgs.get(4));
                cvalues.put("value", msgs.get(5));
                database.insert(SqlHelper.TABLE_NAME, null, cvalues);
                String iKey = msgs.get(4) + "#" + portStr;
                String destinationPort = portStr;
                waitInsertMap.get(iKey).add(destinationPort);
            } else {
                insertToNodes(msgs, k);
            }
        }
    }

    /* Spawns a new thread that deals with accepting all the newly incoming requests from the source.
    *   Takes the desired actions and sends back suitable notifications with status.
    * */
    private class ServerTask extends AsyncTask<ServerSocket, Void, Void> {
        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            try {
                Socket inp = null;
                while (true) {
                    inp = serverSocket.accept();
                    while (!isRecovered) {
                    }
                    receiveAndProcessTheInputPacket(inp);
                }
            } catch (IOException e) {
                Log.e(TAG, "error");
            }
            return null;
        }


    }

    /*  Switch case statement that directly requests methods to perform all the Insertion, Query, Recovery and Delete operations.
    * */
    private void receiveAndProcessTheInputPacket(Socket inp) {
        ObjectInputStream input = null;
        try {
            input = new ObjectInputStream(inp.getInputStream());
            ObjectOutputStream out = new ObjectOutputStream(inp.getOutputStream());
            ArrayList<String> messageList = (ArrayList<String>) input.readObject();
            try {
                switch (messageList.get(0)) {

                    case "Insert_First":
                        insertKeyToRespectiveDatabases(messageList);
                        sendInsertCompleteConfirmation(messageList, out);
                        break;
                    case "Query_Key":
                        queryKeyToRespectiveDatabases(messageList, out);
                        break;
                    case "QueryKeyFailedMode":
                        queryKeyFailedMode(messageList, out);
                        break;
                    case "Query_Star":
                        queryStarToRespectiveDatabases(messageList, out);
                        break;
                    case "RecoverMode":
                        break;
                    case "Delete_star":
                        int del = database.delete(SqlHelper.TABLE_NAME, "1", null);
                        break;
                    case "Delete_Key":
                        String selection = messageList.get(4);
                        int garbage = database.delete(SqlHelper.TABLE_NAME, SqlHelper.Keyval + "=?", new String[]{selection});
                        break;
                }
            } catch (Exception e) {
                e.getMessage();
            } finally {
                if (inp != null) {
                    out.flush();
                    input.close();
                    out.close();
                    inp.close();
                }
            }
        } catch (IOException e) {
            e.getMessage();
        } catch (ClassNotFoundException e) {
            e.getMessage();
        }
    }


    /* Appends the RecoveryReceived notification and send out the Object output Stream object.
    * */
    private void queryStarToRespectiveDatabases(ArrayList<String> messageList, ObjectOutputStream out) {

        List<String> toSend = new ArrayList<>();
        toSend.add(portStr + ";RecoveryReceived");
        toSend = getRecoveredEntries(messageList);
        String destinationPort = messageList.get(1);
        try {
            out.writeObject(toSend);
        } catch (IOException e) {
            e.getMessage();
        }
    }

    /*  When the query key in failed mode, bundle the packet for query Star and send the messageList through the socket.
    * */
    private void queryKeyFailedMode(ArrayList<String> messageList, ObjectOutputStream out) {

        ArrayList<String> toSend = new ArrayList<>();
        String queryOrigin = messageList.get(3);
        toSend = bundleQueryStarAndSend(queryOrigin);
        try {
            out.writeObject(toSend);
        } catch (IOException e) {
            e.getMessage();
        }
    }

    /* Creates a messageList packet and Queries the key to all the respective databases.
    * */
    private void queryKeyToRespectiveDatabases(ArrayList<String> messageList, ObjectOutputStream out) throws IOException {
        ArrayList<String> toSend = new ArrayList<>();
        String keyQuery = messageList.get(2);
        String queryOrigin = messageList.get(3);
        toSend = QueryMyDatabaseAndSendPacket(uri, null, keyQuery, queryOrigin);
        out.writeObject(toSend);
    }

    /* On finding the destination emulator insert the key value pairs into the destination database.
    * */
    private void insertKeyToRespectiveDatabases(ArrayList<String> messageList) {

        ContentValues cvalues = new ContentValues();
        cvalues.put("key", messageList.get(4));
        cvalues.put("value", messageList.get(5));
        database.insert(SqlHelper.TABLE_NAME, null, cvalues);
    }

    /*  Bundles a notification packet and send back through the ObjectOutputStream out
    *   Returns toSend message packet back to control flow.
    *  */
    private ArrayList<String> sendInsertCompleteConfirmation(ArrayList<String> messageList, ObjectOutputStream out) {

        /* Message list is created and stored in toSend ArrayList<>*/
        ArrayList<String> toSend = new ArrayList<>();
        toSend.add("Insert_Complete");
        toSend.add(messageList.get(6));
        toSend.add(messageList.get(4));
        toSend.add(portStr);

        /*Message is sent out through ObjectOutputStream object*/
        try {
            out.writeObject(toSend);
        } catch (IOException e) {
            e.getMessage();
        }
        /*return the Message send back to control flow */
        return toSend;
    }

    /*  Insert a specific key value pair into the Database Abstraction Layer.
    *   Then, creates a packet to send back notification through the same socket connection via "toSend" - ArrayList<String>
    * */
    private void insertKey(ArrayList<String> messageList, ObjectOutputStream out) {
        try {
            ArrayList<String> toSend = new ArrayList<>();
            ContentValues cvalues = new ContentValues();
            cvalues.put("key", messageList.get(4));
            cvalues.put("value", messageList.get(5));
            database.insert(SqlHelper.TABLE_NAME, null, cvalues);
            toSend.add("Insert_Complete");
            toSend.add(messageList.get(6));
            toSend.add(messageList.get(4));
            toSend.add(portStr);
            out.writeObject(toSend);
        } catch (IOException e) {
            e.getMessage();
        }
    }
}

/* Clas with a method setDestinationIds() Return the integer list[] that contains the coordinator and its two successors
*  Takes input as the index lexicographically sorted list. This index is assigned as the coordinator
* * */

class FindAndSetDestinationIDs {

    private int coordinator;
    private int successor1;
    private int successor2;

    /*returns the list which contains the successors of the coordinator. The successors configuration has been given already
    * and hence been hardcoded.The structure forms a rings( 0.1,2,3,4,0.....etc)
    * */
    public int[] setDestinationIds(int coordinatorId) {

        this.coordinator = coordinatorId;
        if (coordinatorId < 3) {
            this.successor1 = coordinatorId + 1;
            this.successor2 = coordinatorId + 2;
        } else if (coordinatorId == 3) {
            this.successor1 = coordinatorId + 1;
            this.successor2 = 0;
        } else if (coordinatorId == 4) {
            this.successor1 = 0;
            this.successor2 = 1;
        }
        int[] list = {coordinator, successor1, successor2};
        return list;
    }
}
