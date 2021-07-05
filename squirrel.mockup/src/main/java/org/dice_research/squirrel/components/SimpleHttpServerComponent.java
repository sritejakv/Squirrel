package org.dice_research.squirrel.components;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.ResIterator;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.riot.Lang;
import org.apache.jena.util.FileUtils;
import org.dice_research.squirrel.configurator.SimpleHTTPServerConfiguration;
import org.dice_research.squirrel.simulation.CrawleableResource;
import org.dice_research.squirrel.simulation.CrawleableResourceContainer;
import org.dice_research.squirrel.simulation.DumpResource;
import org.dice_research.squirrel.simulation.SimpleStringResource;
import org.dice_research.squirrel.simulation.StringResource;
import org.dice_research.squirrel.utils.Closer;
import org.hobbit.core.components.Component;
import org.simpleframework.http.core.Container;
import org.simpleframework.http.core.ContainerServer;
import org.simpleframework.transport.Server;
import org.simpleframework.transport.connect.Connection;
import org.simpleframework.transport.connect.SocketConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleHttpServerComponent implements Component {

    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleHttpServerComponent.class);

    protected Container container;
    protected Server server;
    protected Connection connection;

    @Override
    public void init() throws Exception {
        SimpleHTTPServerConfiguration conf = SimpleHTTPServerConfiguration.getSimpleHTTPServerConfiguration();

        Model model = readModel(conf.getModelFile(), conf.getModelLang());
        if (model == null) {
            throw new IllegalArgumentException("Couldn't read model file.");
        }

        List<CrawleableResource> resources = new ArrayList<>();
        if (conf.getDumpFileName() != null) {
            resources.add(new DumpResource(model, conf.getDumpFileName(), Lang.N3));
        }

        if (conf.isUseDeref()) {
            addDeref(resources, model);
        }

        if (conf.getRobotsTxt() != null) {
            try {
                resources.add(
                        new SimpleStringResource("/robots.txt", FileUtils.readWholeFileAsUTF8(conf.getRobotsTxt())));
            } catch (Exception e) {
                LOGGER.error("Error while reading robots.txt file.", e);
            }
        }

        container = new CrawleableResourceContainer(resources.toArray(new CrawleableResource[resources.size()]));
        server = new ContainerServer(container);
        connection = new SocketConnection(server);
        SocketAddress address = new InetSocketAddress(conf.getServerPort());
        connection.connect(address);

        LOGGER.info("HTTP server initialized.");
    }

    protected Model readModel(String modelFile, String modelLang) {
        Model model = ModelFactory.createDefaultModel();
        try (FileInputStream fin = new FileInputStream(modelFile)) {
            model.read(fin, "", modelLang);
        } catch (Exception e) {
            LOGGER.error("Couldn't read model file. Returning null.", e);
            return null;
        }
        return model;
    }

    /**
     * Adds a dereferencing resource for every subject that is available in the
     * given model.
     *
     * @param resources
     *            the list of crawleable resources this server is using
     * @param model
     *            the model from which the subjects and the data that will be
     *            returned when they are dereferenced is collected
     */
    protected void addDeref(List<CrawleableResource> resources, Model model) {
        ResIterator iterator = model.listSubjects();
        Resource subject;
        Model resourceModel;
        while (iterator.hasNext()) {
            subject = iterator.next();
            resourceModel = ModelFactory.createDefaultModel();
            resourceModel.add(model.listStatements(subject, null, (RDFNode) null));
            resources.add(new StringResource(resourceModel, subject.getURI(), Lang.N3));
        }
    }

    @Override
    public void run() throws Exception {
        synchronized (this) {
            this.wait();
        }
    }

    @Override
    public void close() throws IOException {
        Closer.closeQuietly(connection);
        if (server != null) {
            server.stop();
        }
    }

}
