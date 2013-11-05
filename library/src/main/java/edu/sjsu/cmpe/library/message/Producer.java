/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.sjsu.cmpe.library.message;

import javax.jms.Connection;

import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.fusesource.stomp.jms.StompJmsConnectionFactory;
import org.fusesource.stomp.jms.StompJmsDestination;

public class Producer {

    public static void main(String []args) throws JMSException {

	String user = env("APOLLO_USER", "admin");
	String password = env("APOLLO_PASSWORD", "password");
	String host = env("APOLLO_HOST", "54.215.210.214");
	int port = Integer.parseInt(env("APOLLO_PORT", "61613"));
	String queue = "/queue/test";
	String destination = arg(0, queue);

	StompJmsConnectionFactory factory = new StompJmsConnectionFactory();
	factory.setBrokerURI("tcp://" + host + ":" + port);

	Connection connection = factory.createConnection(user, password);
	
	connection.start();
	Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
	Destination dest = new StompJmsDestination(destination);
	MessageProducer producer = session.createProducer(dest);
	producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
	System.out.println("*** " + connection.getClientID());
	System.out.println("Sending messages to " + queue + "..." + connection.getClientID());
	String data = "Hello World";
	TextMessage msg = session.createTextMessage(data);
	msg.setLongProperty("id", System.currentTimeMillis());
	producer.send(msg);
	System.out.println(msg.getText());
	producer.send(session.createTextMessage("SHUTDOWN"));
	connection.close();

    }

    private static String env(String key, String defaultValue) {
	String rc = System.getenv(key);
	if( rc== null ) {
	    return defaultValue;
	}
	return rc;
    }

    private static String arg(int index, String defaultValue) {
	
	    return defaultValue;
	
    }

}
