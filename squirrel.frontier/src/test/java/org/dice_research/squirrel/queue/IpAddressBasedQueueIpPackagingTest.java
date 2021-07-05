package org.dice_research.squirrel.queue;

import java.net.InetAddress;
import java.net.URI;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.dice_research.squirrel.data.uri.CrawleableUri;
import org.dice_research.squirrel.data.uri.CrawleableUriFactory4Tests;
import org.dice_research.squirrel.data.uri.UriType;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * <p>
 * this class tests the packaging of URIs into chunks. URIs inside a single
 * chunks should have the same IP address and the same {@link UriType}. For
 * every IP UriType combination there shouldn't be more than one single chunk.
 * </p>
 * 
 * @author Michael R&ouml;der (roeder@informatik.uni-leipzig.de)
 *
 */
@RunWith(Parameterized.class)
public class IpAddressBasedQueueIpPackagingTest {

    private CrawleableUri uris[];
    private InetAddress expectedChunks[];
    private String expectedChunkUris[][];

    @SuppressWarnings("deprecation")
    @Parameters
    public static Collection<Object[]> data() throws Exception {
        CrawleableUriFactory4Tests factory = new CrawleableUriFactory4Tests();
        return Arrays.asList(new Object[][] {
                // the same UriType but different IPs
                { new CrawleableUri[] {
                        factory.create(new URI("http://example.org/dump_1"), InetAddress.getByName("192.168.100.1"),
                                UriType.DUMP),
                        factory.create(new URI("http://example.org/dump_2"), InetAddress.getByName("192.168.100.1"),
                                UriType.DUMP),
                        factory.create(new URI("http://example.org/dump_3"), InetAddress.getByName("192.168.100.2"),
                                UriType.DUMP),
                        factory.create(new URI("http://example.org/dump_4"), InetAddress.getByName("192.168.100.3"),
                                UriType.DUMP),
                        factory.create(new URI("http://example.org/dump_5"), InetAddress.getByName("192.168.100.3"),
                                UriType.DUMP),
                        factory.create(new URI("http://example.org/dump_6"), InetAddress.getByName("192.168.100.3"),
                                UriType.DUMP) },
                        new InetAddress[] { InetAddress.getByName("192.168.100.1"),
                                InetAddress.getByName("192.168.100.2"), InetAddress.getByName("192.168.100.3") },
                        new String[][] { { "http://example.org/dump_1", "http://example.org/dump_2" },
                                { "http://example.org/dump_3" },
                                { "http://example.org/dump_4", "http://example.org/dump_5",
                                        "http://example.org/dump_6" } } },
                // the same IPs but different UriTypes
                { new CrawleableUri[] {
                        factory.create(new URI("http://example.org/dump_1"), InetAddress.getByName("192.168.100.1"),
                                UriType.DUMP),
                        factory.create(new URI("http://example.org/sparql_1"), InetAddress.getByName("192.168.100.1"),
                                UriType.SPARQL),
                        factory.create(new URI("http://example.org/resource_1"), InetAddress.getByName("192.168.100.1"),
                                UriType.DEREFERENCEABLE),
                        factory.create(new URI("http://example.org/resource_2"), InetAddress.getByName("192.168.100.1"),
                                UriType.DEREFERENCEABLE),
                        factory.create(new URI("http://example.org/dump_2"), InetAddress.getByName("192.168.100.1"),
                                UriType.DUMP),
                        factory.create(new URI("http://example.org/resource_3"), InetAddress.getByName("192.168.100.1"),
                                UriType.DEREFERENCEABLE) },
                        new InetAddress[] { InetAddress.getByName("192.168.100.1") },
                        new String[][] { { "http://example.org/dump_1", "http://example.org/dump_2",
                                "http://example.org/sparql_1", "http://example.org/resource_1",
                                "http://example.org/resource_2", "http://example.org/resource_3" } } },
                // different IPs and different UriTypes
                { new CrawleableUri[] {
                        factory.create(new URI("http://example.org/dump_1"), InetAddress.getByName("192.168.100.1"),
                                UriType.DUMP),
                        factory.create(new URI("http://example.org/resource_1"), InetAddress.getByName("192.168.100.1"),
                                UriType.DEREFERENCEABLE),
                        factory.create(new URI("http://example.org/dump_2"), InetAddress.getByName("192.168.100.2"),
                                UriType.DUMP),
                        factory.create(new URI("http://example.org/dump_3"), InetAddress.getByName("192.168.100.3"),
                                UriType.DUMP),
                        factory.create(new URI("http://example.org/dump_4"), InetAddress.getByName("192.168.100.3"),
                                UriType.DUMP),
                        factory.create(new URI("http://example.org/sparql_1"), InetAddress.getByName("192.168.100.3"),
                                UriType.SPARQL) },
                        new InetAddress[] { InetAddress.getByName("192.168.100.1"),
                                InetAddress.getByName("192.168.100.2"), InetAddress.getByName("192.168.100.3") },
                        new String[][] { { "http://example.org/dump_1" ,  "http://example.org/resource_1" }, { "http://example.org/dump_2" },
                                { "http://example.org/dump_3", "http://example.org/dump_4",
                                        "http://example.org/sparql_1" } } } });
    }

    public IpAddressBasedQueueIpPackagingTest(CrawleableUri[] uris, InetAddress[] expectedChunks,
            String[][] expectedChunkUris) {
        this.uris = uris;
        this.expectedChunks = expectedChunks;
        this.expectedChunkUris = expectedChunkUris;
    }

    @Test
    public void test() throws Exception {
        BlockingQueue<InetAddress> queue = new InMemoryQueue();

        for (int i = 0; i < uris.length; ++i) {
            queue.addUri(uris[i]);
        }

        int chunkId;
        BitSet chunksFound = new BitSet(expectedChunks.length);
        List<CrawleableUri> chunk = queue.getNextUris();
        while ((chunk != null) && (chunk.size() > 0)) {
            chunkId = 0;
            while ((chunkId < expectedChunks.length)
                    && (!expectedChunks[chunkId].equals(chunk.get(0).getIpAddress()))) {
                ++chunkId;
            }
            Assert.assertTrue(
                    "Couldn't find a matching chunk with the IP " + chunk.get(0).getIpAddress().toString() + ".",
                    chunkId < expectedChunks.length);
            Assert.assertEquals("Expected another number of URIs in this set of URIs.",
                    expectedChunkUris[chunkId].length, chunk.size());
            Set<String> expectedUris = new HashSet<String>();
            expectedUris.addAll(Arrays.asList(expectedChunkUris[chunkId]));
            for (CrawleableUri uri : chunk) {
                Assert.assertTrue("Couldn't find the URI " + uri.getUri().toString()
                        + " inside the set of expected Uris (" + expectedUris.toString() + ").",
                        expectedUris.contains(uri.getUri().toString()));
            }
            chunksFound.set(chunkId);
            // mark the ip as accessible
            queue.markUrisAsAccessible(chunk);

            chunk = queue.getNextUris();
        }
        Assert.assertEquals("The number of different chunks does not equal the expected number.", expectedChunks.length,
                chunksFound.cardinality());
    }
}
