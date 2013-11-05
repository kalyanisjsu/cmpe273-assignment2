package edu.sjsu.cmpe.library;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.TextMessage;
import org.fusesource.stomp.jms.StompJmsConnectionFactory;
import org.fusesource.stomp.jms.StompJmsDestination;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.yammer.dropwizard.Service;
import com.yammer.dropwizard.config.Bootstrap;
import com.yammer.dropwizard.config.Environment;
import com.yammer.dropwizard.views.ViewBundle;
import edu.sjsu.cmpe.library.api.resources.BookResource;
import edu.sjsu.cmpe.library.api.resources.RootResource;
import edu.sjsu.cmpe.library.config.LibraryServiceConfiguration;
import edu.sjsu.cmpe.library.domain.Book;
import edu.sjsu.cmpe.library.domain.Book.Status;
import edu.sjsu.cmpe.library.repository.BookRepository;
import edu.sjsu.cmpe.library.repository.BookRepositoryInterface;
import edu.sjsu.cmpe.library.ui.resources.HomeResource;

public class LibraryService extends Service<LibraryServiceConfiguration> {

	public static String apolloUser;
	public static String queueName;
	public static String topicName;
	public static String apolloPassword ;
	public static int apolloPort;
	public static String apolloHost;
	public static String libraryName;
	public String destination;
	public String topicB = "/topic/61070.book.computer";
	public String topicA = "/topic/61070.book.all";
	public Book book=null;
	public URL url=null;
	Status status = Status.available;

	public static BookRepositoryInterface bookRepository = new BookRepository();
	private final Logger log = LoggerFactory.getLogger(getClass());

	public static void main(String[] args) throws Exception {
		new LibraryService().run(args);
	}


	@Override
	public void initialize(Bootstrap<LibraryServiceConfiguration> bootstrap) {

		bootstrap.setName("library-service");
		bootstrap.addBundle(new ViewBundle());
	}


	@Override
	public void run(LibraryServiceConfiguration configuration, Environment environment) throws Exception {
		// This is how you pull the configurations from library_x_config.yml
		queueName = configuration.getStompQueueName();
		topicName = configuration.getStompTopicName();
		apolloUser = configuration.getApolloUser();
		apolloPassword = configuration.getApolloPassword();
		apolloPort = configuration.getApolloPort();
		apolloHost = configuration.getApolloHost();
		libraryName = configuration.getLibraryName();

		int numThreads = 1;
		ExecutorService executor = Executors.newFixedThreadPool(numThreads);

		Runnable backgroundTask = new Runnable() {

			@Override
			public void run() {

				String user = apolloUser;
				String password =apolloPassword;
				String host = apolloHost;
				int port = apolloPort;


				if(libraryName.equalsIgnoreCase("library-a"))
				{
					System.out.println("Library Name :: " + libraryName );
					destination = arg(0,topicA);
				}
				else if(libraryName.equalsIgnoreCase("library-b"))
				{
					System.out.println("Library Name :: " + libraryName );
					destination = arg(0,topicB);
				}

				StompJmsConnectionFactory factory = new StompJmsConnectionFactory();
				factory.setBrokerURI("tcp://" + host + ":" + port);
				Connection connection;
				try {
					connection = factory.createConnection(user, password);
					connection.start();
					Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
					Destination dest = new StompJmsDestination(destination);
					MessageConsumer consumer = session.createConsumer(dest);
					System.currentTimeMillis();
					System.out.println("Waiting for messages...");
					while(true) {
						Message msg = consumer.receive();
						if( msg instanceof  TextMessage ) {
							String body = ((TextMessage) msg).getText();
							System.out.println("Received message = " + body);
							System.out.println("Size :: " + BookResource.BookMap.size());
							String[] queueBookArray = body.split(":");
							Long queueISBN = Long.parseLong(queueBookArray[0]);
							for ( Long key : BookResource.BookMap.keySet()) {
								System.out.println("Key value :: " +  key.toString() + "  "+queueISBN);
								if(key == queueISBN)
								{
									System.out.println("Updating lost book :: " + BookResource.BookMap.get(key).getTitle());
									Book updateBook = BookResource.BookMap.get(key);
									updateBook.setStatus(status);
									System.out.println("Status updated::: " + status.toString());
								}
								else
								{
									System.out.println("Size :: " + BookResource.BookMap.size()+" " + Long.parseLong(queueBookArray[0]) + queueBookArray[3]+":"+queueBookArray[4]);
									//{isbn}:{title}:{category}:{coverimage}
									book = new Book();
									String coverURL = queueBookArray[3]+":"+queueBookArray[4];
									coverURL = coverURL.replaceAll("\"", "");
									URI uri = new URI(coverURL);
									url =  uri.toURL();
									book.setIsbn(queueISBN);
									book.setTitle(queueBookArray[1].replaceAll("\"", ""));
									book.setCategory(queueBookArray[2].replaceAll("\"", ""));
									book.setCoverimage(url);
									System.out.println("New book :: " +book.getTitle() + " " + book.getIsbn());									
									BookRepository.bookInMemoryMap.put(Long.parseLong(queueBookArray[0]), book);
								}

							}

						} else if (msg == null) {
							System.out.println("No new messages. Exiting due to timeout - " + 5000 / 1000 + " sec");
							break;

						} else {
							System.out.println("Unexpected message type: " + msg.getClass());
						}
					}
					connection.close();
				} catch (JMSException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} 
				catch (URISyntaxException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (MalformedURLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		};

		executor.execute(backgroundTask);
		executor.shutdown();
		/** Root API */
		environment.addResource(RootResource.class);
		/** Books APIs */
		environment.addResource(new BookResource(bookRepository));
		/** UI Resources */
		environment.addResource(new HomeResource(bookRepository));
	}

	private static String arg(int index, String defaultValue) {
		return defaultValue;
	}



}
