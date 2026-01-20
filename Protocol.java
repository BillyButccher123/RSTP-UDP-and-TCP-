/*
 * Replace the following string of 0s with your student number
 * c4021942
 */
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Protocol {

	static final String  NORMAL_MODE="nm"   ;         // normal transfer mode: (for Part 1 and 2)
	static final String	 TIMEOUT_MODE ="wt"  ;        // timeout transfer mode: (for Part 3)
	static final String	 LOST_MODE ="wl"  ;           // lost Ack transfer mode: (for Part 4)
	static final String	 TCP_MODE ="tcp"  ;           // lost Ack transfer mode: (for Part 4)
	static final String	 UDP_MODE ="udp"  ;           // lost Ack transfer mode: (for Part 4)
	static final int DEFAULT_TIMEOUT =1000  ;         // default timeout in milliseconds (for Part 3)
	static final int DEFAULT_RETRIES =4  ;            // default number of consecutive retries (for Part 3)
	public static final int MAX_Segment_SIZE = 4096;  //the max segment size that can be used when creating the received packet's buffer

	/*
	 * The following attributes control the execution of the transfer protocol and provide access to the 
	 * resources needed for the transfer 
	 * 
	 */ 

	private InetAddress ipAddress;      // the address of the server to transfer to. This should be a well-formed IP address.
	private int portNumber; 		    // the  port the server is listening on
	private DatagramSocket socket;      // the socket that the client binds to

	private File inputFile;            // the client-side CSV file that has the readings to transfer  
	private String outputFileName ;    // the name of the output file to create on the server to store the readings
	private int maxPatchSize;		   // the patch size - no of readings to be sent in the payload of a single Data segment

	private Segment dataSeg   ;        // the protocol Data segment for sending Data segments (with payload read from the csv file) to the server 
	private Segment ackSeg  ;          // the protocol Ack segment for receiving ACK segments from the server

	private int timeout;              // the timeout in milliseconds to use for the protocol with timeout (for Part 3)
	private int maxRetries;           // the maximum number of consecutive retries (retransmissions) to allow before exiting the client (for Part 3)(This is per segment)
	private int currRetry;            // the current number of consecutive retries (retransmissions) following an Ack loss (for Part 3)(This is per segment)

	private int fileTotalReadings;    // number of all readings in the csv file
	private int sentReadings;         // number of readings successfully sent and acknowledged
	private int totalSegments;        // total segments that the client sent to the server

	// Shared Protocol instance so Client and Server access and operate on the same values for the protocol’s attributes (the above attributes).
	public static Protocol instance = new Protocol();

	/**************************************************************************************************************************************
	 **************************************************************************************************************************************
	 * For this assignment, you have to implement the following methods:
	 *		sendMetadata()
	 *      readandSend()
	 *      receiveAck()
	 *      startTimeoutWithRetransmission()
	 *		receiveWithAckLoss()
	 * Do not change any method signatures, and do not change any other methods or code provided.
	 ***************************************************************************************************************************************
	 **************************************************************************************************************************************/
	/* 
	 * This method sends protocol metadata to the server.
	 * See coursework specification for full details.	
	 */
	public void sendMetadata() { 
		//counting the number of readings in the input file
		try (BufferedReader br = new BufferedReader(new java.io.FileReader(inputFile))) {
			String line;
			while ((line = br.readLine()) != null){
				fileTotalReadings++;
			}
		}catch (IOException e){
			checkFile(outputFileName);
		}
		//concatenating the contents of the metadata payload
		String segPayload = (fileTotalReadings +","+ outputFileName +","+ maxPatchSize);
		try{
			Segment metaDataSeg = new Segment();
			metaDataSeg.setType(SegmentType.Meta);
			metaDataSeg.setSeqNum(0);
			metaDataSeg.setPayLoad(segPayload);
			//putting segment into a packet	
			ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
			ObjectOutputStream os = new ObjectOutputStream(outputStream);
			os.writeObject(metaDataSeg);			
			byte[] metaSeg = outputStream.toByteArray();			
			DatagramPacket ackPacket = new DatagramPacket(metaSeg, metaSeg.length, ipAddress, portNumber);
			// send the Meta segment 
			socket.send(ackPacket);
					System.out.println("CLIENT:META [SEQ#"+metaDataSeg.getSeqNum()+"] (Number of readings:"+fileTotalReadings+", file name:"+outputFileName+", patch size:"+maxPatchSize);
		}catch(IOException e){
			System.out.println("CLIENT:Failed to send metadata");
		}
		} 



	/* 
	 * This method read and send the next data segment (dataSeg) to the server. 
	 * See coursework specification for full details.
	 */
	public void readAndSend() {
		List<Reading> payloads = new ArrayList<Reading>();
		//reads each line of data.csv and makes payloads
		try (BufferedReader br = new BufferedReader(new java.io.FileReader(inputFile))) {
			String line;
			
			//goes line by line and creates payloads for each data segment that is about to be sent
			while ((line = br.readLine()) != null){	
				String[] splitiString = line.split(",");
				float[] values = {2.0f,3.0f,4.0f};
				for (int i = 0; i<(splitiString.length - 2);i++){
					values[i] = Float.parseFloat(splitiString[i+2]);
				}
				Reading readingtype = new Reading(splitiString[0],Long.parseLong(splitiString[1]),values);
				payloads.add(readingtype);
			}
		}
		catch(IOException e){
			System.out.println("CLIENT:File "+outputFileName+" is empty.");
		}
		List<String> payloadsString = new ArrayList<String>();
		int segsize = 0;
		int readings = payloads.size();
		String data = "";
		for(int i = 0; i < payloads.size()-1;i++){
			if (segsize < maxPatchSize){
				if (readings > maxPatchSize){
					data = (data +payloads.get(i).toString() +";");
					segsize++;
					if (segsize == maxPatchSize){
						readings = readings - segsize;
						segsize = 0;
						payloadsString.add(data);
						data = "";
					}
					if (readings == 0){
						segsize = 0;
						payloadsString.add(data);
						data = "";
					}
				}
				if (readings <= maxPatchSize){
					data = (data +payloads.get(i+1).toString() +";");
					segsize++;
					readings--;
					if (readings == 0){
						segsize = 0;
						payloadsString.add(data);
						data = "";
					}
				}
			}	
		}
		try{
			if (totalSegments >= payloadsString.size()){
				System.out.println("CLIENT:Total Segments:"+totalSegments);
				System.exit(0);
			}
			String pay = payloadsString.get(totalSegments).substring(0,payloadsString.get(totalSegments).length()-1);
			dataSeg.setSize(pay.length());
			dataSeg.setType(SegmentType.Data);
			//determines the next segment number
			if ((totalSegments%2) == 0){
				dataSeg.setSeqNum(1);
			}else if ((totalSegments%2) != 0){
				dataSeg.setSeqNum(0);
			}
			dataSeg.setPayLoad(pay);
			long x = dataSeg.calculateChecksum();
			dataSeg.setChecksum(x);
			//set up the segment to be sent
			ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
			ObjectOutputStream os = new ObjectOutputStream(outputStream);
			os.writeObject(dataSeg);			
			byte[] dataseg = outputStream.toByteArray();			
			DatagramPacket dataPacket = new DatagramPacket(dataseg, dataseg.length, ipAddress, portNumber);
			// send the data segment 
			socket.send(dataPacket);
			System.out.println("CLIENT:Send: DATA [SEQ#"+dataSeg.getSeqNum()+"] (Size:"+dataSeg.getSize()+", crc:"+dataSeg.getChecksum()+", content:"+dataSeg.getPayLoad().toString()+")");
			totalSegments++;
		}catch(IOException e){
			System.exit(0);
	}
	}
		

	
	/* 
	 * This method receives the current Ack segment (ackSeg) from the server 
	 * See coursework specification for full details.
	 */
	public boolean receiveAck() { 
		try{
			byte[] bufer = new byte[Protocol.MAX_Segment_SIZE]; //prepare the buffer to have the max segment size

			DatagramPacket incomingPacket = new DatagramPacket(bufer, bufer.length);
			socket.receive(incomingPacket);
			byte[] data = incomingPacket.getData();
			ByteArrayInputStream in = new ByteArrayInputStream(data);
			ObjectInputStream is = new ObjectInputStream(in);

			// read the content of the segment
			try {
				ackSeg = (Segment) is.readObject(); 
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}  
			//if the segment is of type Ack
			if (ackSeg.getType() == SegmentType.Ack) {
				if (ackSeg.getSeqNum() == dataSeg.getSeqNum()){
					//print the expected number of segments with the other information
					System.out.println("CLIENT: RECIEVE: ACK [SEQ#"+ackSeg.getSeqNum()+"]");
					int s = dataSeg.getPayLoad().split(",").length;
					sentReadings = sentReadings + s;
					if (sentReadings == totalSegments){
						System.out.println("CLIENT: Readinds sent:"+sentReadings+" , Segments sent:"+totalSegments);
						System.exit(0);
					}
					return true;
				}else if (ackSeg.getSeqNum() == dataSeg.getSeqNum()){
					return false;
				}
			}
		}catch(IOException e){
			System.out.println("CLIENT: The acknowledgemt was not received");

		}
	return false;
}

	/* 
	 * This method starts a timer and does re-transmission of the Data segment 
	 * See coursework specification for full details.
	 */
	public void startTimeoutWithRetransmission(){
		try{
			socket.setSoTimeout(timeout);
			if (!receiveAck()){
				throw new SocketException();
			}
		}catch(SocketException e){
			System.out.println("CLIENT:[ACK#"+dataSeg.getSeqNum()+"] not received");
			System.out.println("CLIENT:Resending segment, retry "+currRetry);
			if (currRetry == maxRetries){
				System.out.println("CLIENT:Maximum tries reached.");
				System.out.println("CLIENT:Exiting...");
				System.exit(0);
			}
			currRetry++;
			totalSegments = totalSegments - 1;
		}
	}
	
	private double efficiency(List<String> usefulData, List<String> totalReceived){
		int using = usefulData.size();
		int got = totalReceived.size();
		double a = using;
		double b = got; 
		return ((a/b)*100);
	}

	/* 
	 * This method is used by the server to receive the Data segment in Lost Ack mode
	 * See coursework specification for full details.
	 */
	public void receiveWithAckLoss(DatagramSocket serverSocket, float loss) {
		byte[] buf = new byte[MAX_Segment_SIZE];
		
		//creat a temporary list to store the readings and segments
		List<String> receivedLines = new ArrayList<>();
		List<String> usefulReceivedLines = new ArrayList<>();
		List<Segment> receivedSeg = new ArrayList<>();
		//track the number of the correctly received readings
		int readingCount= 0;
		try{	
			// while still receiving Data segments  
			while (true) {
				serverSocket.setSoTimeout(2000);
				DatagramPacket incomingPacket = new DatagramPacket(buf, buf.length);
				serverSocket.receive(incomingPacket);// receive from the client    
				
				
				Segment serverDataSeg = new Segment(); 
				byte[] data = incomingPacket.getData();
				ByteArrayInputStream in = new ByteArrayInputStream(data);
				ObjectInputStream is = new ObjectInputStream(in);

				// read and then print the content of the segment
				try {
					serverDataSeg = (Segment) is.readObject();
					if (totalSegments > 1){ 
						receivedSeg.add(serverDataSeg); 
					}
				} catch (ClassNotFoundException e) {
					e.printStackTrace();
				}
				System.out.println("SERVER: Receive: DATA [SEQ#"+ serverDataSeg.getSeqNum()+ "]("+"size:"+serverDataSeg.getSize()+", crc: "+serverDataSeg.getChecksum()+
						", content:"  + serverDataSeg.getPayLoad()+")");
				
				// calculate the checksum
				long x = serverDataSeg.calculateChecksum();

				// if the calculated checksum is same as that of received checksum then send the corresponding ack
				if (serverDataSeg.getType() == SegmentType.Data && x == serverDataSeg.getChecksum()) {
					System.out.println("SERVER: Calculated checksum is " + x + "  VALID");
					
					receivedSeg.add(serverDataSeg);

					// write the payload of the data segment to the temporary list 
					String[] lines = serverDataSeg.getPayLoad().split(";");
					receivedLines.add("Segment ["+ serverDataSeg.getSeqNum() + "] has "+ lines.length + " Readings");
					receivedLines.addAll(Arrays.asList(lines));
					receivedLines.add("");
					
					//update the number of correctly received readings
					readingCount += lines.length;

					// extract the client IP address and port number from the received packet for sending the ack to the client
					InetAddress iPAddress = incomingPacket.getAddress();
					int port = incomingPacket.getPort();
					//the sequence number of the Ack segment is the same as the received Data segment
					if (isLost(loss) == false){
						if (receivedSeg.size() > 1){
							long previousChecksum = receivedSeg.get(receivedSeg.size()-2).calculateChecksum();
							long currentChecksum = receivedSeg.get(receivedSeg.size()-1).calculateChecksum();
							if (currentChecksum == previousChecksum){
								Server.sendAck(serverSocket, iPAddress, port, serverDataSeg.getSeqNum());
								receivedSeg.remove(receivedSeg.size()-1);
							}else if(currentChecksum != previousChecksum){
								usefulReceivedLines.add("Segment ["+ serverDataSeg.getSeqNum() + "] has "+ lines.length + " Readings");
								usefulReceivedLines.addAll(Arrays.asList(lines));
								usefulReceivedLines.add("");
								Server.sendAck(serverSocket, iPAddress, port, serverDataSeg.getSeqNum());
							} 
						}else if (receivedSeg.size() == 1){
							usefulReceivedLines.add("Segment ["+ serverDataSeg.getSeqNum() + "] has "+ lines.length + " Readings");
							usefulReceivedLines.addAll(Arrays.asList(lines));
							usefulReceivedLines.add("");
							Server.sendAck(serverSocket, iPAddress, port, serverDataSeg.getSeqNum());
						}
					}
					else if (isLost(loss) == true){
						if (receivedSeg.size() >= 1){
							receivedSeg.remove(receivedSeg.size()-1);
							}
						readingCount -= lines.length;
						System.out.println("SERVER: Simulating packet loss...");
					}

				// if the calculated checksum is not the same as that of received checksum, then do not send any ack
				} else if (serverDataSeg.getType() == SegmentType.Data&& x != serverDataSeg.getChecksum()) {
					System.out.println("SERVER: Calculated checksum is " + x + "  INVALID");
					System.out.println("SERVER: Not sending any ACK ");
					System.out.println("*************************** "); 
				}
				
				//if all readings are received, then write the readings to the file
				if (Protocol.instance.getOutputFileName() != null && readingCount >= Protocol.instance.getFileTotalReadings()) { 
					Server.writeReadingsToFile(receivedLines, Protocol.instance.getOutputFileName());
					break;
				}
		}}catch(IOException e){
			System.out.println("SERVER:Something went wrong");	
		}
		System.out.println("SERVER: Efficiency:"+efficiency(usefulReceivedLines,receivedLines)+"%");
		serverSocket.close();
	}

	/*************************************************************************************************************************************
	 **************************************************************************************************************************************
	 **************************************************************************************************************************************
	These methods are implemented for you .. Do NOT Change them 
	 **************************************************************************************************************************************
	 **************************************************************************************************************************************
	 **************************************************************************************************************************************/	 
	/* 
	 * This method initialises ALL the 14 attributes needed to allow the Protocol methods to work properly
	 */
	public void initProtocol(String hostName , String portNumber, String fileName, String outputFileName, String batchSize) throws UnknownHostException, SocketException {
		instance.ipAddress = InetAddress.getByName(hostName);
		instance.portNumber = Integer.parseInt(portNumber);
		instance.socket = new DatagramSocket();

		instance.inputFile = checkFile(fileName); //check if the CSV file does exist
		instance.outputFileName =  outputFileName;
		instance.maxPatchSize= Integer.parseInt(batchSize);

		instance.dataSeg = new Segment(); //initialise the data segment for sending readings to the server
		instance.ackSeg = new Segment();  //initialise the ack segment for receiving Acks from the server

		instance.fileTotalReadings = 0; 
		instance.sentReadings=0;
		instance.totalSegments =0;

		instance.timeout = DEFAULT_TIMEOUT;
		instance.maxRetries = DEFAULT_RETRIES;
		instance.currRetry = 0;		 
	}


	/* 
	 * check if the csv file does exist before sending it 
	 */
	private static File checkFile(String fileName)
	{
		File file = new File(fileName);
		if(!file.exists()) {
			System.out.println("CLIENT: File does not exists"); 
			System.out.println("CLIENT: Exit .."); 
			System.exit(0);
		}
		return file;
	}

	/* 
	 * returns true with the given probability to simulate network errors (Ack loss)(for Part 4)
	 */
	private static Boolean isLost(float prob) 
	{ 
		double randomValue = Math.random();  //0.0 to 99.9
		return randomValue <= prob;
	}

	/* 
	 * getter and setter methods	 *
	 */
	public String getOutputFileName() {
		return outputFileName;
	} 

	public void setOutputFileName(String outputFileName) {
		this.outputFileName = outputFileName;
	} 

	public int getMaxPatchSize() {
		return maxPatchSize;
	} 

	public void setMaxPatchSize(int maxPatchSize) {
		this.maxPatchSize = maxPatchSize;
	} 

	public int getFileTotalReadings() {
		return fileTotalReadings;
	} 

	public void setFileTotalReadings(int fileTotalReadings) {
		this.fileTotalReadings = fileTotalReadings;
	}

	public void setDataSeg(Segment dataSeg) {
		this.dataSeg = dataSeg;
	}

	public void setAckSeg(Segment ackSeg) {
		this.ackSeg = ackSeg;
	}

	public void setCurrRetry(int currRetry) {
		this.currRetry = currRetry;
	}

}
