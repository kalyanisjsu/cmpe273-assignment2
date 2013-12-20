package edu.sjsu.cmpe.procurement;
import java.util.ArrayList;

import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yammer.dropwizard.Service;
import com.yammer.dropwizard.config.Bootstrap;
import com.yammer.dropwizard.config.Configuration;
import com.yammer.dropwizard.config.Environment;

import edu.sjsu.cmpe.procurement.config.ProcurementServiceConfiguration;
import edu.sjsu.cmpe.procurement.domain.ProcurementResource;

public class ProcurementService extends Service<ProcurementServiceConfiguration> {

	//private final Logger log = LoggerFactory.getLogger(getClass());

	public static String apolloUser;
	public static String queueName;
	public static String topicName;
	public static String apolloPassword ;
	public static int apolloPort;
	public static String apolloHost;
	public static ArrayList<String> booksInQueue = new ArrayList <String>();
	protected Scheduler scheduler;

	public static void main(String[] args) throws Exception {
		new ProcurementService().run(args);
	}

	@Override
	public void initialize(Bootstrap<ProcurementServiceConfiguration> bootstrap) {
		bootstrap.setName("procurement-service");
	}



	@Override
	public void run(ProcurementServiceConfiguration configuration,Environment environment) throws Exception {

		apolloUser = env("APOLLO_USER", "admin");
		apolloPassword = env("APOLLO_PASSWORD", "password");
		apolloHost = env("APOLLO_HOST", "54.219.156.168");
		apolloPort = Integer.parseInt(env("APOLLO_PORT", "61613"));
		queueName = configuration.getStompQueueName();
		
		/*apolloUser = configuration.getApolloUser();
		apolloPassword = configuration.getApolloPassword();
		apolloPort = configuration.getApolloPort();
		apolloHost = configuration.getApolloHost();*/
		
		JobDetail job = JobBuilder.newJob(ProcurementResource.class).build();
		Trigger trigger = TriggerBuilder.newTrigger().withSchedule(SimpleScheduleBuilder.simpleSchedule().withIntervalInMinutes(5).repeatForever()).build();
		Scheduler scheduler = new StdSchedulerFactory().getScheduler();
		scheduler.start();
		scheduler.scheduleJob(job, trigger);
		environment.addResource(ProcurementResource.class);

	}
	
	 private static String env(String key, String defaultValue) {
			String rc = System.getenv(key);
			if( rc== null ) {
			    return defaultValue;
			}
			return rc;
		    }
}

