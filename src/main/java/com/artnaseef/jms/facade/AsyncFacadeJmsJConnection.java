package com.artnaseef.jms.facade;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;

import javax.jms.*;

/**
 * Created by art on 1/20/17.
 */
public class AsyncFacadeJmsJConnection implements javax.jms.Connection {

    private final CountDownLatch aquiredConnectionLatch = new CountDownLatch(1);
    private Connection targetConnection;
    private ExceptionListener exceptionListener;
    private boolean abort;
    private ConcurrentLinkedQueue<AsyncFacadeCommand> onConnectedCommandQueue = new ConcurrentLinkedQueue<>();

    public Connection getTargetConnection() {
        return targetConnection;
    }

    public void onTargetConnectionCreated(Connection targetConnection) throws JMSException {
        // Performing any of the onConnectedCommands twice is a non-problem, so remember the new connection first,
        //  then replay the commands, and finally count down the latch.  This way, we are guaranteed not to miss any
        //  command.
        this.targetConnection = targetConnection;
        playOnConnectedCommands(targetConnection);

        this.aquiredConnectionLatch.countDown();
    }

    private void playOnConnectedCommands(Connection targetConnection) throws JMSException {
        for (AsyncFacadeCommand command : onConnectedCommandQueue) {
            switch (command) {
                case SET_EXCEPTION_LISTENER:
                    targetConnection.setExceptionListener(this.exceptionListener);
                    break;

                case START:
                    targetConnection.start();
                    break;
            }
        }
    }

    @Override
    public Session createSession(boolean transacted, int acknowledgeMode) throws JMSException {
        this.waitUntilConnectionAcquired();
        return targetConnection.createSession(transacted, acknowledgeMode);
    }

    @Override
    public String getClientID() throws JMSException {
        this.waitUntilConnectionAcquired();
        return this.targetConnection.getClientID();
    }

    @Override
    public void setClientID(String clientID) throws JMSException {
        this.waitUntilConnectionAcquired();
        this.targetConnection.setClientID(clientID);
    }

    @Override
    public ConnectionMetaData getMetaData() throws JMSException {
        this.waitUntilConnectionAcquired();
        return this.targetConnection.getMetaData();
    }

    @Override
    public ExceptionListener getExceptionListener() throws JMSException {
        return this.exceptionListener;
    }

    @Override
    public void setExceptionListener(ExceptionListener listener) throws JMSException {
        this.exceptionListener = listener;

        this.onConnectedCommandQueue.add(AsyncFacadeCommand.SET_EXCEPTION_LISTENER);
        if (this.targetConnection != null) {
            this.targetConnection.setExceptionListener(listener);
        }
    }

    @Override
    public void start() throws JMSException {
        this.onConnectedCommandQueue.add(AsyncFacadeCommand.START);

        if (this.targetConnection != null) {
            this.targetConnection.start();
        }
    }

    @Override
    public void stop() throws JMSException {
        this.abort = true;
        if (this.aquiredConnectionLatch.getCount() == 0) {
            this.aquiredConnectionLatch.countDown();
        }
        this.waitUntilConnectionAcquired();

    }

    @Override
    public void close() throws JMSException {
        this.waitUntilConnectionAcquired();

    }

    @Override
    public ConnectionConsumer createConnectionConsumer(Destination destination, String messageSelector, ServerSessionPool sessionPool, int maxMessages) throws JMSException {
        this.waitUntilConnectionAcquired();
        return null;
    }

    @Override
    public ConnectionConsumer createDurableConnectionConsumer(Topic topic, String subscriptionName, String messageSelector, ServerSessionPool sessionPool, int maxMessages) throws JMSException {
        this.waitUntilConnectionAcquired();
        return null;
    }

    private void waitUntilConnectionAcquired() throws JMSException {
        try {
            this.aquiredConnectionLatch.await();
            if (this.abort) {
                throw new JMSException("aborted due to stop before fully starting the underlying connection");
            }
        } catch (InterruptedException intExc) {
            JMSException jmsException = new JMSException("interrupted waiting to acquire the underlying connection");
            jmsException.initCause(intExc);

            throw jmsException;
        }
    }
}
