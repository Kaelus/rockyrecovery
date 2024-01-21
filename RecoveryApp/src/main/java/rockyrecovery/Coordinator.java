package rockyrecovery;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.LinkedBlockingQueue;

public class Coordinator {

	// connection related
	private static ReceiverXMLRPC receiver;
	public static ArrayList<String> recoveryCommittee;
	private String coordinatorPort = "16679";

	// logging info
	public String loggerID = "Coordinator";
	public String coordinatorID;
	public int coordinatorNo;

	// recovery related (common)
	public static int rollbackEpoch = -1;
	public static boolean hasAllCommitteeNodes = false;
	
	// recovery related (no cloud failure)
	public static boolean hasCloudFailed = false;

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
			this.loggerID = coordinatorID + (coordinatorNo++);
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
		coordinatorID = coordinatorName;
		coordinatorNo = 0;
		recoveryCommittee = new ArrayList<String>();
	}

	public void runServer() {
		receiver = new ReceiverXMLRPC(Integer.valueOf(coordinatorPort));
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
			
		} else {
			DebugLog.elog("ERROR: I don't have all committee nodes registered yet!");
			retObj = null;
		}
		
		return (Object) retObj;
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
		    	if (line.startsWith("recovery_committee")) { 
		    		String[] tokens = line.split("=");
		    		String committeeStr = tokens[1];
		    		String[] committeeNodesStr = committeeStr.split(",");
		    		for (int i = 0; i < committeeNodesStr.length; i++) {
		    			recoveryCommittee.add(committeeNodesStr[i]);
		    		}
		    	} else if (line.startsWith("e_a")) {
		    	   String[] tokens = line.split("=");
		    	   String rollbackEpochStr = tokens[1];
		    	   rollbackEpoch = Integer.parseInt(rollbackEpochStr);
		    	   System.out.println("rollbackEpoch=" + rollbackEpoch);
		       } else if (line.startsWith("cloud_failure")) {
		    	   String[] tokens = line.split("=");
		    	   hasCloudFailed = Boolean.parseBoolean(tokens[1]);
		    	   System.out.println("hasCloudFailed=" + hasCloudFailed);
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
	
	public static void main(String[] args) {

		//update variables using config if given
		if (args.length < 2) {
			System.out.println("no config file is given.");
			System.exit(1);
		} else if (args.length >= 2) {
			System.out.println("given config file=" + args[1]);
			parseRecoveryConfig(args[1]);
		}
		
		Coordinator server = new Coordinator("standalone-Coordinator-main");
		server.runServer();
	}

}
