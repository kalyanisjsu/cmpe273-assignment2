package edu.sjsu.cmpe.procurement.domain;

import java.util.ArrayList;


import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.apache.log4j.Logger;
import org.fusesource.stomp.jms.StompJmsConnectionFactory;
import org.fusesource.stomp.jms.StompJmsDestination;
import org.json.JSONArray;
import org.json.JSONObject;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

import edu.sjsu.cmpe.procurement.ProcurementService;

@Path("/")
@Produces(MediaType.APPLICATION_JSON)

public class ProcurementResource implements Job {

	ArrayList<String> booksInQueue = new ArrayList<String>();
	String booksFromPublisher;
	String booksToPublisher;
	ClientResponse response;
	String queue = ProcurementService.queueName;
	String topicB = "/topic/61070.book.computer";
	String topicA = "/topic/61070.book.";
	Message msg =null;
	JSONArray jsonArray=null;
	Session session = null;
	Destination dest=null;
	Connection connection=null;

	public Logger log = Logger.getLogger(ProcurementResource.class);

	public void execute(JobExecutionContext jExeCtx) throws JobExecutionException {

		String user = ProcurementService.apolloUser;
		String password = ProcurementService.apolloPassword;
		String host = ProcurementService.apolloHost;
		int port = ProcurementService.apolloPort;
		log.debug("TestJob run successfully...");
		String destination = arg(0, queue);
		StompJmsConnectionFactory factory = new StompJmsConnectionFactory();
		factory.setBrokerURI("tcp://" + host + ":" + port);

		try {

			connection = factory.createConnection(user, password);
			connection.start();
			session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
			dest = new StompJmsDestination(destination);
			MessageConsumer consumer = session.createConsumer(dest);
			System.out.println("Waiting for messages from " + queue + "...");
			//GetMethod();
			long waitUntil=5000;
			while(true) {
				msg = consumer.receive(waitUntil);
				System.out.println("ArrayList  :: " +booksInQueue);
				if( msg instanceof TextMessage ) {

					System.out.println("****Inside iF****");
					String body = ((TextMessage) msg).getText();
					System.out.println("Received message = " + body);
					String[] bookIsbn = body.split(":");
					booksInQueue.add(bookIsbn[1]);
					System.out.println("ArrayList  :: " +booksInQueue);

				} 
				else if (msg == null) {
					System.out.println("No new messages. Exiting due to timeout - " + waitUntil / 1000 + " sec");
					break;

				} else {

					System.out.println("Unexpected message type: " + msg.getClass());

				}
			}

			//	if(msg!=null)
			PostMethod();
			//else
			//System.out.println("No messages so not calling POST Method");
			GetMethod();
			System.out.println("Done");
			connection.close();


		} catch (JMSException e) {
			// TODO Auto-generated catch block
			System.out.println("Queue is empty");
			e.printStackTrace();

		} 
	}

	public void PostMethod()
	{
		try 
		{
			Client client = Client.create();
			WebResource webResource = client.resource("http://54.219.156.168:9000/orders");
			for (int i = 0; i < booksInQueue.size(); i++) {

				int bookISBN = Integer.parseInt(booksInQueue.get(i));
				booksToPublisher = "{\"id\":\"61070\",\"order_book_isbns\": ".concat("[" + bookISBN + "]" + "}");
				response = webResource.type("application/json").post(ClientResponse.class, booksToPublisher);
				if (response.getStatus() != 200) { 

					throw new RuntimeException("Failed : HTTP error code ***: " + response.getStatus());
				}

			}

			System.out.println("Server Says....");
			String output = response.getEntity(String.class);
			System.out.println(output);

		} catch (Exception e) {
			System.out.println("Queue is empty");
			//e.printStackTrace();
		}
	}


	public void GetMethod()
	{
		try {

			Client client = Client.create();
			WebResource webResource = client.resource("http://54.219.156.168:9000/orders/61070");
			ClientResponse response = webResource.accept("application/json").get(ClientResponse.class);
			if (response.getStatus() != 200) {
				throw new RuntimeException("Failed : HTTP error code : " + response.getStatus());
			}
			booksFromPublisher = response.getEntity(String.class);
			System.out.println("Server Says....Getting books from Publisher");
			System.out.println(booksFromPublisher);
			JSONObject jsonObj1;
			jsonObj1 = new JSONObject(booksFromPublisher);
			jsonArray = jsonObj1.getJSONArray("shipped_books");

			for (int i = 0; i < jsonArray.length(); i++) {
				String category = jsonArray.getJSONObject(i).getString("category");
				System.out.println("Category :: " + category );
				if(category.equalsIgnoreCase("computer"))
				{
					System.out.println("Sending to Lib B topic");
					//	{isbn}:{title}:{category}:{coverimage}
					//123:”Restful Web Services”:”computer”:”http://goo.gl/ZGmzoJ”
					String MsgBookB =  jsonArray.getJSONObject(i).getString("isbn") + ":" + "\"" 
							+ jsonArray.getJSONObject(i).getString("title") + "\"" + ":"+"\"" + category + "\""+":" 
							+"\""+ jsonArray.getJSONObject(i).getString("coverimage")+ "\"" ;
					PublishToLibB(MsgBookB);
				}
				else
				{
					System.out.println("Sending Lib A topic");
					topicA+=category;
					String MsgBookA =  jsonArray.getJSONObject(i).getString("isbn") + ":" + "\"" 
							+ jsonArray.getJSONObject(i).getString("title") + "\"" + ":"+"\"" + category + "\""+":" 
							+"\""+ jsonArray.getJSONObject(i).getString("coverimage")+ "\"" ;
					PublishToLibA(MsgBookA);
				}
			}

		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}


	public void PublishToLibB(String TempMsgBookB) throws JMSException
	{
		String user = ProcurementService.apolloUser;
		String password = ProcurementService.apolloPassword;
		String host = ProcurementService.apolloHost;
		int port = ProcurementService.apolloPort;
		String destination = arg(0, topicB);
		StompJmsConnectionFactory factory = new StompJmsConnectionFactory();
		factory.setBrokerURI("tcp://" + host + ":" + port);
		Connection connection = factory.createConnection(user, password);
		connection.start();
		Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		Destination dest = new StompJmsDestination(destination);
		MessageProducer producer = session.createProducer(dest);
		producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
		TextMessage msg = session.createTextMessage(TempMsgBookB);
		msg.setLongProperty("id", System.currentTimeMillis());
		producer.send(msg);
		System.out.println("Message published to Topic Lib B::  " +msg.getText() );
		//read();
		connection.close();
	}


	public void PublishToLibA(String TempMsgBookA) throws JMSException
	{
		String user = ProcurementService.apolloUser;
		String password = ProcurementService.apolloPassword;
		String host = ProcurementService.apolloHost;
		int port = ProcurementService.apolloPort;
		System.out.println("Topic name send to:" + topicA);
		String destination = arg(0, topicA);
		StompJmsConnectionFactory factory = new StompJmsConnectionFactory();
		factory.setBrokerURI("tcp://" + host + ":" + port);
		Connection connection = factory.createConnection(user, password);
		connection.start();
		Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		Destination dest = new StompJmsDestination(destination);
		MessageProducer producer = session.createProducer(dest);
		producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
		TextMessage msg = session.createTextMessage(TempMsgBookA);
		msg.setLongProperty("id", System.currentTimeMillis());
		producer.send(msg);
		System.out.println("Message published to Topic Lib A:: " + msg.getText() );
		//read();
		connection.close();
	}

	private static String arg(int index, String defaultValue) {
		return defaultValue;
	}


}
