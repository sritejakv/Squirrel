package com.squirrel.rabbit;

import com.SquirrelWebObject;
import com.SquirrelWebObjectHelper;
import com.graph.VisualisationGraph;
import com.graph.VisualisationHelper;
import com.rabbitmq.client.*;
import org.aksw.simba.squirrel.data.uri.CrawleableUriFactoryImpl;
import org.aksw.simba.squirrel.data.uri.serialize.Serializer;
import org.aksw.simba.squirrel.data.uri.serialize.java.GzipJavaUriSerializer;
import org.aksw.simba.squirrel.rabbit.msgs.UriSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotNull;
import java.awt.*;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeoutException;

/**
 * The interface between the RabbitMQ and the Web-Service
 * Or better to say: Listener for the RabbitMQ - receives and organize the {@link SquirrelWebObject}s and the {@link com.graph.VisualisationGraph}s
 *
 * @author Philipp Heinisch
 */
public class RabbitMQList implements Runnable {

    private List<SquirrelWebObject> dataQueue = new ArrayList<>();
    private List<VisualisationGraph> graphQueue = new ArrayList<>();
    private final static String QUEUE_INPUT_GENERAL_NAME = "squirrel.web.in";
    private final static String QUEUE_INPUT_GRAPH_NAME = "squirrel.web.in.graph";
    //reuse an already existing queue
    private final static String QUEUE_OUTPUT_URI_NAME = "squirrel.frontier"; // "squirrel.web.out.uri";
    private Connection connection;
    private Channel channel;

    private Serializer serializer = new GzipJavaUriSerializer();

    private Logger logger = LoggerFactory.getLogger(RabbitMQList.class);

    private final static int MAXLENGTHOFHISTORY = 2000;

    @Override
    public void run() {
        if (!rabbitConnect(6) || !queueDeclare(QUEUE_INPUT_GENERAL_NAME)) {
            return;
        }

        // IN

        boolean listenToVisualizationGraphs = queueDeclare(QUEUE_INPUT_GRAPH_NAME);

        Consumer generalConsumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) {
                SquirrelWebObject o = SquirrelWebObjectHelper.convertToObject(body);
                logger.debug("The consumer " + consumerTag + "received an SquirrelWebObject from the Frontier!");
                addElementToLimitedList(dataQueue, o);
                logger.trace("Added the new SquirrelWebObject to the dataQueue, contains " + dataQueue.size() + " SquirrelWebObjects now!");
            }
        };
        Consumer graphConsumer = null;
        if (listenToVisualizationGraphs) {
            graphConsumer = new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) {
                    VisualisationGraph graph = VisualisationHelper.convertToObject(body);
                    logger.debug("The consumer " + consumerTag + "received an VisualisationGraph from the Frontier!");
                    addElementToLimitedList(graphQueue, graph);
                    logger.trace("Added the new VisualisationGraph to the graphQueue, contains " + graphQueue.size() + " VisualisationGraphs now!");
                }
            };
        }
        try {
            channel.basicConsume(QUEUE_INPUT_GENERAL_NAME, true, generalConsumer);
            if (listenToVisualizationGraphs) {
                channel.basicConsume(QUEUE_INPUT_GRAPH_NAME, true, graphConsumer);
            }
        } catch (IOException e) {
            logger.warn(e.getMessage(), e);
            try {
                Thread.sleep(5000);
            } catch (InterruptedException ei) {
                ei.printStackTrace();
                return;
            }
            run();
        }

        //OUT
        //queueDeclare is already done by the frontier. No need to do it here. And without the Frontier... this feature would make no sense...
        //queueDeclare(QUEUE_OUTPUT_URI_NAME);
    }

    private boolean queueDeclare(String queueName) {
        try {
            channel.queueDeclare(queueName, false, false, false, null);
        } catch (IOException e) {
            logger.error("I have a connection to " + connection.getClientProvidedName() + " with the channel number " + channel.getChannelNumber() + ", but I was not able to declare a queue :(", e);
            return false;
        }
        logger.info("Queue declaration succeeded with the name " + queueName + " [" + channel.getChannelNumber() + "]");

        return true;
    }

    /**
     * recursive function, that tries to connect with the rabbit container
     *
     * @param triesLeft the number of left tries. 5s are in between 2 tries
     * @return {@code true}, if the connection to the RabbitMQ was established, otherwise {@code false}
     */
    private boolean rabbitConnect(int triesLeft) {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("rabbit");
        factory.setUsername("guest");
        factory.setPassword("guest");
        connection = null;
        try {
            connection = factory.newConnection();
            channel = connection.createChannel();
        } catch (IOException e) {
            if (triesLeft > 0) {
                logger.warn(triesLeft + " tries left: Could not established a connection to the rabbit: no communication to rabbit :( [" + e.getMessage() + "]", e);
                try {
                    //wait until the rabbit is started in Docker
                    Thread.sleep(5000);
                } catch (InterruptedException ei) {
                    logger.info("The waiting time for the rabbit was interrupted. Steo forward with trying to get a connection!");
                }
                return rabbitConnect(triesLeft - 1);
            } else {
                logger.error("0 tries left: Could not established a connection to the rabbit: no communication to rabbit :( [" + e.getMessage() + "]", e);
                return false;
            }
        } catch (TimeoutException e) {
            if (triesLeft > 0) {
                logger.warn(triesLeft + " tries left: Could not established a connection to the rabbit - TIMEOUT: no communication to rabbit :( [" + e.getMessage() + "]", e);
                try {
                    //wait until the rabbit is started in Docker
                    Thread.sleep(5000 * (6 - Math.min(5, triesLeft)));
                } catch (InterruptedException ei) {
                    logger.info("The waiting time for the rabbit was interrupted. Steo forward with trying to get a connection!");
                }
                return rabbitConnect(triesLeft - 1);
            } else {
                logger.error("0 tries left: Could not established a connection to the rabbit - TIMEOUT: no communication to rabbit :( [" + e.getMessage() + "]", e);
                return false;
            }
        }

        logger.info("Connection to rabbit succeeded: " + factory.getHost() + " - " + connection.getClientProvidedName() + " [" + connection.getId() + "]");
        return true;
    }

    public void close() throws IOException, TimeoutException {
        if (channel == null)
            return;
        logger.debug("THREAD: CLOSE BEGIN");
        channel.close();
        logger.debug("THREAD: CLOSE END");
    }

    /**
     * Gets the fetched data from the Frontier. Contains many information about the current crawling status and so on
     *
     * @return the latest {@link SquirrelWebObject}
     */
    public SquirrelWebObject getSquirrel() {
        return getSquirrel(dataQueue.size() - 1);
    }

    /**
     * Gets the fetched data from the Frontier. Contains many information about the current crawling status and so on
     *
     * @param index All received {@link SquirrelWebObject} are stored in a list. Index {@code 0} is the oldest entry, Index {@code size-1} is the latest one
     * @return the {@link SquirrelWebObject}
     */
    SquirrelWebObject getSquirrel(int index) {
        SquirrelWebObject ret = getObject(dataQueue, index);
        return (ret == null) ? new SquirrelWebObject() : ret;
    }

    /**
     * Gets the fected crawled graph from Frontier.
     *
     * @return the latest {@link VisualisationGraph}
     */
    VisualisationGraph getCrawledGraph() {
        return getCrawledGraph(graphQueue.size() - 1);
    }

    /**
     * Gets the fected crawled graph from Frontier.
     *
     * @param index All received {@link VisualisationGraph} are stored in a list. Index {@code 0} is the oldest entry, Index {@code size-1} is the latest one
     * @return the {@link VisualisationGraph}
     */
    VisualisationGraph getCrawledGraph(int index) {
        VisualisationGraph ret = getObject(graphQueue, index);
        if (ret == null) {
            ret = new VisualisationGraph();
            ret.addNode("No Graph available").setColor(Color.RED);
        }

        return ret;
    }

    private <T> T getObject(List<T> list, int index) {
        if (list.isEmpty()) {
            return null;
        }
        try {
            return list.get(index);
        } catch (IndexOutOfBoundsException e) {
            return list.get(dataQueue.size() - 1);
        }
    }

    /**
     * Counts the number of {@link SquirrelWebObject}
     *
     * @return the number of {@link SquirrelWebObject}-objects, that were received from the WebService
     */
    int countSquirrelWebObjects() {
        return dataQueue.size();
    }

    /**
     * Adds a element to a {@link List}, but checks in addition before, if a certain size limit (MAXLENGTHOFHISTORY) is reached. If yes, the method truncates every second element from the list.
     * <b>Attention:</b> if the list is bigger than MAXLENGTHOFHISTORY*2, then the truncate procedure will execute multiple times.
     *
     * @param list            the {@link List}, in that the element should be added. Must not be null.
     * @param insertedElement the element, that should be added Must not be null.
     * @param <T>             this method is generic
     */
    private <T> void addElementToLimitedList(@NotNull List<T> list, @NotNull T insertedElement) {
        while (list.size() >= MAXLENGTHOFHISTORY) {
            List<T> toBeDeleted = new ArrayList<>(MAXLENGTHOFHISTORY >> 1);
            for (int i = 0; i < list.size(); i++) {
                if (i % 2 == 1) {
                    toBeDeleted.add(list.get(i));
                }
            }
            list.removeAll(toBeDeleted);
        }

        list.add(insertedElement);
    }

    /**
     * Just publishes a URI to the queue (connected with the Frontier): for serialising, a {@link Serializer} is used.
     * Until yet, it's common to use as the {@link Serializer} implementation the {@link GzipJavaUriSerializer}. If it changes, you should adpat this method!
     *
     * @param uri the {@link java.net.URI}
     * @return {@code true} if and only if the publish to the {@link Channel} succeeded!
     */
    boolean publishURI(String uri) {
        logger.trace("Received a request to publish " + uri);
        try {
            channel.basicPublish("", QUEUE_OUTPUT_URI_NAME, null, serializer.serialize(new UriSet(Collections.singletonList(new CrawleableUriFactoryImpl().create(uri)))));
            logger.info("Successful pushed the URI " + uri + " to the rabbit queue " + QUEUE_OUTPUT_URI_NAME);
            return true;
        } catch (IOException e) {
            logger.error("ERROR during serializing/ publishing the " + uri, e);
        }
        return false;
    }
}
