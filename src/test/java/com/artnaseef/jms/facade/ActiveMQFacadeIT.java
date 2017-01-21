package com.artnaseef.jms.facade;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.store.memory.MemoryPersistenceAdapter;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by art on 1/20/17.
 */
public class ActiveMQFacadeIT {

  private static final Logger LOGGER = LoggerFactory.getLogger(ActiveMQFacadeIT.class);

  private BrokerService brokerService;

  // Timeout after 2 minutes
  @Test(timeout = 120000L)
  public void testActiveMQConnectionViaFacade() throws Exception {
    ActiveMQConnectionFactory
        activeMQConnectionFactory =
        new ActiveMQConnectionFactory("tcp://localhost:61616");

    FacadeJmsConnectionFactory
        facadeJmsConnectionFactory =
        new FacadeJmsConnectionFactory(activeMQConnectionFactory);


    LOGGER.info("About to connect via facade");
    Connection connection = facadeJmsConnectionFactory.createConnection();
    LOGGER.info("Finished connection via facade");

    connection.start();

    // Now start the broker
    startBroker();

    //
    // The rest of this is all synchronous, meaning a connection to the broker must already
    //  exist.
    //
    Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    Queue testQueue = session.createQueue("facade.test.queue");
    MessageProducer producer = session.createProducer(session.createQueue("facade.test.queue"));
    producer.send(session.createTextMessage("x-facade-test-message-x"));

    MessageConsumer consumer = session.createConsumer(testQueue);
    Message msg = consumer.receive();
    assertTrue(msg instanceof TextMessage);

    LOGGER.info("have a message returned: " + ((TextMessage) msg).getText());
    assertEquals("x-facade-test-message-x", ((TextMessage) msg).getText());
  }

  private void startBroker() throws Exception {
    this.brokerService = new BrokerService();
    this.brokerService.setPersistent(false);
    this.brokerService.setUseJmx(false);

    this.brokerService.setPersistenceAdapter(new MemoryPersistenceAdapter());
    this.brokerService.addConnector("tcp://localhost:61616");

    this.brokerService.start();
    this.brokerService.waitUntilStarted();
  }
}
