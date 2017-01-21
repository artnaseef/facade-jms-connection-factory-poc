package com.artnaseef.jms.facade;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;

/**
 * Created by art on 1/20/17.
 */
public class FacadeJmsConnectionFactory implements ConnectionFactory {

    private final ConnectionFactory targetFactory;

    public FacadeJmsConnectionFactory(ConnectionFactory targetFactory) {
        this.targetFactory = targetFactory;
    }

    @Override
    public Connection createConnection() throws JMSException {
        AsyncFacadeJmsJConnection asyncFacadeJmsJConnection = new AsyncFacadeJmsJConnection();
        Thread getConnectedThread = new Thread(() -> getConnected(asyncFacadeJmsJConnection, () -> targetFactory.createConnection()));
        getConnectedThread.start();

        return asyncFacadeJmsJConnection;
    }

    @Override
    public Connection createConnection(String userName, String password) throws JMSException {
        AsyncFacadeJmsJConnection asyncFacadeJmsJConnection = new AsyncFacadeJmsJConnection();
        Thread getConnectedThread = new Thread(() -> getConnected(asyncFacadeJmsJConnection, () -> targetFactory.createConnection(userName, password)));
        getConnectedThread.start();

        return asyncFacadeJmsJConnection;
    }

    private void getConnected(AsyncFacadeJmsJConnection asyncFacadeJmsJConnection, ConnectionAction connectionAction) {
        // TODO: exceptions here need better handling
        try {
            Connection targetConnection = connectionAction.connect();
            asyncFacadeJmsJConnection.onTargetConnectionCreated(targetConnection);
        } catch (JMSException jmsExc) {
            jmsExc.printStackTrace();
        }
    }

    private interface ConnectionAction {
        public Connection connect() throws JMSException;
    }
}
