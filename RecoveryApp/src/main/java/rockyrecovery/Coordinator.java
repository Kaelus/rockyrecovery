package rockyrecovery;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.LinkedBlockingQueue;

public class Coordinator {

	// storage type related
	public enum BackendStorageType {DynamoDBLocal, DynamoDB_SEOUL, 
		DynamoDB_LONDON, DynamoDB_OHIO, Unknown};
	public static BackendStorageType backendStorage;
	
	// connection related
	private static String ip = "127.0.0.1";
	private static String port = "16679";
	private static ReceiverXMLRPC receiver;
	public static ArrayList<String> recoveryCommittee;
	
	// logging info
	public String loggerID = "Coordinator";
	public String myID;
	public int myInstanceNo;
	
	// debugging related
	public static boolean debugPrintoutFlag = false;
	
	// metadata related (local and cloud)
	// public String pBmTableName = "presenceBitmapTable";
	public static String cloudEpochBitmapsTableName = "cloudEpochBitmapsTable";
	public static String localEpochBitmapsTableName = "localEpochBitmapsTable";
	public static String cloudBlockSnapshotStoreTableName = "cloudBlockSnapshotStoreTable";
	public static String versionMapTableName = "versionMapTable";
	public static String localBlockSnapshotStoreTableName = "localBlockSnapshotStoreTable";

	// public GenericKeyValueStore pBmStore;
	public static GenericKeyValueStore cloudEpochBitmaps;
	public static GenericKeyValueStore localEpochBitmaps;
	public static GenericKeyValueStore cloudBlockSnapshotStore;
	public static GenericKeyValueStore versionMap;
	public static GenericKeyValueStore localBlockSnapshotStore;

	// recovery related (common)
	public static long epochEa = -1;
	public static long epochEp = -1;
	public static boolean hasAllCommitteeNodes = false;
	
	// recovery related (no cloud failure)
	public static boolean hasCloudFailed = false;
	public static NoCloudFailureRecoveryWorker ncfrWorker;
	public static Thread noCloudFailureRecoveryWorkerThread;
	public static boolean noCloudFailureRecoveryFlag = false;
	public static int handleRecoveryNoCloudFailureCnt = 0;
		
	public static LinkedBlockingQueue<EndpointController> endpoints;
	
	public class EndpointController {
		public String endpointID;
		public String ipStr;
		public String portStr;
		
		public LinkedBlockingQueue<Message> reqQueue;
		
		public EndpointController() {
			reqQueue = new LinkedBlockingQueue<Message>();
		}
	}

	
	/**
	 * This constructor is called multiple times. On client's request, WebServer
	 * creates this class' instance on the process of creating a separate worker to
	 * handle the request. (out of our control)
	 * 
	 * Shared variable instantiation should be done in another constructor that is
	 * used for starting up the WebServer
	 * 
	 * NOTE: This should not be called by code that is not WebServer
	 * 
	 * @throws Exception
	 */
	public Coordinator() throws Exception {
		if (loggerID == null) {
			this.loggerID = myID + (myInstanceNo++);
		}
		DebugLog.log("Coordinator constructor each worker thread");
	}

	/**
	 * This constructor is to be used to start up the WebServer. This is meant to be
	 * called only once. Also, this should be the only constructor used by other
	 * than WebServer.
	 * 
	 * @param srvName
	 * @throws Exception
	 */
	public Coordinator(String coordinatorName) {
		myID = coordinatorName;
		myInstanceNo = 0;
		recoveryCommittee = new ArrayList<String>();
		
		if (Coordinator.backendStorage.equals(Coordinator.BackendStorageType.DynamoDBLocal)) {
			//pBmStore = new ValueStorageDynamoDB(pBmTableName, true);
			cloudEpochBitmaps = new ValueStorageDynamoDB(cloudEpochBitmapsTableName, ValueStorageDynamoDB.AWSRegionEnum.LOCAL);
			cloudBlockSnapshotStore = new ValueStorageDynamoDB(cloudBlockSnapshotStoreTableName, ValueStorageDynamoDB.AWSRegionEnum.LOCAL);
		} else if (Coordinator.backendStorage.equals(Coordinator.BackendStorageType.DynamoDB_SEOUL)) {
			//pBmStore = new ValueStorageDynamoDB(pBmTableName, false);
			cloudEpochBitmaps = new ValueStorageDynamoDB(cloudEpochBitmapsTableName, ValueStorageDynamoDB.AWSRegionEnum.SEOUL);
			cloudBlockSnapshotStore = new ValueStorageDynamoDB(cloudBlockSnapshotStoreTableName, ValueStorageDynamoDB.AWSRegionEnum.SEOUL);
		} else if (Coordinator.backendStorage.equals(Coordinator.BackendStorageType.DynamoDB_LONDON)) {
			//pBmStore = new ValueStorageDynamoDB(pBmTableName, false);
			cloudEpochBitmaps = new ValueStorageDynamoDB(cloudEpochBitmapsTableName, ValueStorageDynamoDB.AWSRegionEnum.LONDON);
			cloudBlockSnapshotStore = new ValueStorageDynamoDB(cloudBlockSnapshotStoreTableName, ValueStorageDynamoDB.AWSRegionEnum.LONDON);
		} else if (Coordinator.backendStorage.equals(Coordinator.BackendStorageType.DynamoDB_OHIO)) {
			//pBmStore = new ValueStorageDynamoDB(pBmTableName, false);
			cloudEpochBitmaps = new ValueStorageDynamoDB(cloudEpochBitmapsTableName, ValueStorageDynamoDB.AWSRegionEnum.OHIO);
			cloudBlockSnapshotStore = new ValueStorageDynamoDB(cloudBlockSnapshotStoreTableName, ValueStorageDynamoDB.AWSRegionEnum.OHIO);
		} else {
			System.err.println("Error: Unknown backendStorageType");
 		   	System.exit(1);
		}
		try {
			localEpochBitmaps = new ValueStorageLevelDB(localEpochBitmapsTableName);
			localBlockSnapshotStore = new ValueStorageLevelDB(localBlockSnapshotStoreTableName);
			versionMap = new ValueStorageLevelDB(versionMapTableName);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// get prefetched epoch and load epochEp
		epochEp = getPrefetchedEpoch();
	}

	public void runServer() {
		receiver = new ReceiverXMLRPC(Integer.valueOf(port));
	}

	public void stopServer() {
		receiver.webServer.shutdown();
	}

	/*
	 * This function handles Endpoint's request accordingly
	 */
	public Object handleEndpointRequest(byte[] epReq) throws Exception {

		Message coordiMsg = null;
		Object returnObj = null;
		Message epMsg = (Message) ObjectSerializer.deserialize(epReq);

		DebugLog.log(epMsg.toString());
		DebugLog.log("handleEndpointRequest. Thread ID: " + Thread.currentThread().getId());
		DebugLog.log("Coordinator has received a request in handleEndpointRequest", this.loggerID);

		Message ack;

		switch (epMsg.msgType) {
		case MessageType.MSG_T_REGISTER_ENDPOINT:
			DebugLog.log("Handling A Request: REGISTER_ENDPOINT", this.loggerID);
			returnObj = handleRegisterEndpoint(epMsg);
			ack = new Message();
			if (returnObj == null) {
				ack.msgType = MessageType.MSG_T_NACK;
			} else {
				ack.msgType = MessageType.MSG_T_ACK;
			}
			ack.msgContent = returnObj;
			coordiMsg = (Message) ack;
			break;
		case MessageType.MSG_T_RECOVERY_NO_CLOUD_FAILURE:
			DebugLog.log("Handling A Request: RECOVERY_NO_CLOUD_FAILURE", this.loggerID);
			returnObj = handleRecoveryNoCloudFailure(epMsg);
			ack = new Message();
			ack.msgType = MessageType.MSG_T_ACK;
			ack.msgContent = returnObj;
			coordiMsg = (Message) ack;
			break;
		default:
			break;
		}

		// long beginObjSerialization = System.currentTimeMillis();
		Object retObj = ObjectSerializer.serialize(coordiMsg);
		// long endObjSerialization = System.currentTimeMillis();
		// DebugLog.log("Obj Serialization takes=" + (endObjSerialization -
		// beginObjSerialization) + "ms\n");
		DebugLog.log("Server is responding to a client request", this.loggerID);
		return retObj;
	}

	private Object handleRegisterEndpoint(Message epMsg) {
		Object retObj = null; 
		String[] ipPortPair = (String[]) epMsg.msgContent;
		EndpointController epCtrlr = new EndpointController();
		epCtrlr.ipStr = ipPortPair[0];
		epCtrlr.portStr = ipPortPair[1];
		epCtrlr.endpointID = epCtrlr.ipStr + ":" + epCtrlr.portStr;
		if (!recoveryCommittee.contains(epCtrlr.endpointID)) {
			retObj = null;
		} else {
			synchronized (endpoints) {
				boolean hasEndpointsRegistered = false;
				for (EndpointController cont : endpoints) {
					if (cont.endpointID.equals(epCtrlr.endpointID)) {
						hasEndpointsRegistered = true;
						break;
					}							
				}
				if (!hasEndpointsRegistered) {
					endpoints.add(epCtrlr);
					DebugLog.log("handleRegisterEndpoint registers the endpoint=" 
							+ epCtrlr.endpointID);
					if (endpoints.size() == recoveryCommittee.size()) {
						hasAllCommitteeNodes = true;
						if (!hasCloudFailed) {
							ncfrWorker = new NoCloudFailureRecoveryWorker();
							noCloudFailureRecoveryWorkerThread = new Thread(ncfrWorker);
							noCloudFailureRecoveryWorkerThread.start();
						} else {
							// TODO: implement the setup for the three phase protocol
							
						}
					}
					retObj = (Object) epCtrlr.endpointID;
				} else {
					DebugLog.elog("WARN: Repeatedly registering the endpoint=" 
							+ epCtrlr.endpointID);
					retObj = null;
				}
			}
		}
		return retObj;
	}

	private Object handleRecoveryNoCloudFailure(Message epMsg) {
		Object retObj = null;
		if (hasAllCommitteeNodes) {
			String[] args = new String[2];
			args[0] = "" + hasCloudFailed;
			args[1] = "" + epochEa; 
			retObj = (Object) args;
			if (++handleRecoveryNoCloudFailureCnt == recoveryCommittee.size()) {
				noCloudFailureRecoveryFlag = true;
			}
		} else {
			DebugLog.elog("ERROR: I don't have all committee nodes registered yet!");
			retObj = null;
		}
		
		return (Object) retObj;
	}
	
	public class NoCloudFailureRecoveryWorker implements Runnable {
		public NoCloudFailureRecoveryWorker() { 
		}
		@Override
		public void run() {
			DebugLog.log("[NoCloudFailureRecoveryWorker] run entered");
			while(!noCloudFailureRecoveryFlag) {
				DebugLog.log("[NoCloudFailureRecoveryWorker] noCloudFailureRecoveryFlag is false.. wait");
				try {
					Thread.sleep(50);
				} catch (InterruptedException e) {
					e.printStackTrace();
					DebugLog.elog("[NoCloudFailureRecoveryWorker] waiting for noCloudFailureRecoveryFlag " 
							+ "is interrupted unexpectedly..we stop running this thread");
					return;
				}
			}
			Coordinator.rollbackLocalNode();
			Coordinator.rollbackCloudNode();
			DebugLog.log("[NoCloudFailureRecoveryWorker] Terminating NoCloudFailureRecoveryWorker Thread");
		}
	}
	
	protected static void rollbackLocalNode() {
		
	}
	
	protected static void rollbackCloudNode() {
		
	}
	
	private static void parseRecoveryConfig(String configFile) {
		File confFile = new File(configFile);
		if (!confFile.exists()) {
			if (!confFile.mkdir()) {
				System.err.println("Unable to find " + confFile);
	            System.exit(1);
	        }
		}
		try (BufferedReader br = new BufferedReader(new FileReader(configFile))) {
		    String line;
		    while ((line = br.readLine()) != null) {
		    	if (line.startsWith("ip")) {
		    		String[] tokens = line.split("=");
		    		ip = tokens[1];
		    		System.out.println("ip=" + ip);
		    	} else if (line.startsWith("port")) {
		    		String[] tokens = line.split("=");
		    		port = tokens[1];
		    		System.out.println("port=" + port);
		    	} else if (line.startsWith("recovery_committee")) { 
		    		String[] tokens = line.split("=");
		    		String committeeStr = tokens[1];
		    		String[] committeeNodesStr = committeeStr.split(",");
		    		for (int i = 0; i < committeeNodesStr.length; i++) {
		    			recoveryCommittee.add(committeeNodesStr[i]);
		    		}
		    	} else if (line.startsWith("e_a")) {
		    	   String[] tokens = line.split("=");
		    	   String rollbackEpochStr = tokens[1];
		    	   epochEa = Integer.parseInt(rollbackEpochStr);
		    	   System.out.println("rollbackEpoch=" + epochEa);
		       } else if (line.startsWith("cloud_failure")) {
		    	   String[] tokens = line.split("=");
		    	   hasCloudFailed = Boolean.parseBoolean(tokens[1]);
		    	   System.out.println("hasCloudFailed=" + hasCloudFailed);
		       } else if (line.startsWith("backendStorageType")) {
		    	   String[] tokens = line.split("=");
		    	   String backendStorageTypeStr = tokens[1];
		    	   if (backendStorageTypeStr.equals("dynamoDBLocal")) {
		    		   backendStorage = BackendStorageType.DynamoDBLocal;
		    	   } else if (backendStorageTypeStr.equals("dynamoDBSeoul")) {
		    		   backendStorage = BackendStorageType.DynamoDB_SEOUL;
		    	   } else if (backendStorageTypeStr.equals("dynamoDBLondon")) {
		    		   backendStorage = BackendStorageType.DynamoDB_LONDON;
		    	   } else if (backendStorageTypeStr.equals("dynamoDBOhio")) {
		    		   backendStorage = BackendStorageType.DynamoDB_OHIO;
		    	   } else {
		    		   backendStorage = BackendStorageType.Unknown;
		    	   }
		    	   System.out.println("backendStorageType=" + backendStorage);
		    	   if (backendStorage.equals(BackendStorageType.Unknown)) {
		    		   System.err.println("Error: Cannot support Unknown backendStorageType");
		    		   System.exit(1);
		    	   }
		       } 
		    }
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			System.exit(1);
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
	
	
	private long getPrefetchedEpoch() {
		long retLong = 0;
		byte[] epochBytes;
		try {
			epochBytes = cloudBlockSnapshotStore.get("PrefetchedEpoch-" + myID);
			if (epochBytes == null) {
				System.out.println("PrefetchedEpoch-" + myID 
						+ " key is not allocated yet. Allocate it now");
				try {
					cloudBlockSnapshotStore.put("PrefetchedEpoch-" + myID
							, ByteUtils.longToBytes(retLong));
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			} else {
				//System.out.println("epochBytes=" + epochBytes.hashCode());
				//System.out.println("Long.MAX=" + Long.MAX_VALUE);
				//System.out.println("epochBytes length=" + epochBytes.length);
				retLong = ByteUtils.bytesToLong(epochBytes);
				System.out.println("getPrefetchedEpoch returns=" + retLong);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return retLong;
	}

	public static void main(String[] args) {

		// initialize required variables
		backendStorage = BackendStorageType.DynamoDBLocal;
		
		//update variables using config if given
		if (args.length < 2) {
			System.out.println("no config file is given.");
			System.exit(1);
		} else if (args.length >= 2) {
			System.out.println("given config file=" + args[1]);
			parseRecoveryConfig(args[1]);
		}
		
		// print out variable settings
		System.out.println("ip=" + ip);
		System.out.println("port=" + port);
		System.out.println("backendStorageType=" + backendStorage);
		
		String myID = ip + ":" + port;
		
		Coordinator server = new Coordinator(myID);
		server.runServer();
	}

}
