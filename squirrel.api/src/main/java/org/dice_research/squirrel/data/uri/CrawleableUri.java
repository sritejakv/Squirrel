package org.dice_research.squirrel.data.uri;

import java.io.Serializable;
import java.net.InetAddress;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.TreeMap;

/**
 * This class represents a URI and additional meta data that is helpful for
 * crawling it.
 *
 * <p>
 * <b>Serialization</b> - objects of this class can be serialized to byte
 * arrays. These arrays are organized as follows.<br>
 * <code>bytes[0] = </code>ordinal number of the {@link #type}, we use
 * {@link UriType#UNKNOWN} if the attribute is null.<br>
 * <code>bytes[1 to 4] = </code>length <code>uLength</code>of the URI in bytes.
 * <br>
 * <code>bytes[{@value #URI_START_INDEX} to (uLength+4)] = </code> URI in bytes
 * with {@link #CHARSET_NAME}={@value #CHARSET_NAME} as charset.<br>
 * If <code>(bytes.length > (uLength + {@value #URI_START_INDEX}))</code> then
 * the remaining bytes are the {@link #ipAddress}.
 * </p>
 *
 * @author Michael R&ouml;der (roeder@informatik.uni-leipzig.de)
 *
 */
public class CrawleableUri implements Serializable {

    public static final String UUID_KEY = "UUID";
    private static final long serialVersionUID = 1L;

    private static final String CHARSET_NAME = "UTF-8";
    private static final Charset ENCODING_CHARSET = Charset.forName(CHARSET_NAME);

    private long timestampNextCrawl;

    private final URI uri;

    private InetAddress ipAddress;
    private Map<String, Object> data = new TreeMap<>();

    public CrawleableUri(URI uri) {
        this(uri, null);
    }

    public CrawleableUri(URI uri, InetAddress ipAddress) {
        this.uri = uri;
        this.ipAddress = ipAddress;
    }

    public InetAddress getIpAddress() {
        return ipAddress;
    }

    public void setIpAddress(InetAddress ipAddress) {
        this.ipAddress = ipAddress;
    }

    public URI getUri() {
        return uri;
    }

    public void addData(String key, Object data) {
        this.data.put(key, data);
    }

    public Object getData(String key) {
        if(data.containsKey(key)) {
            return data.get(key);
        } else {
            return null;
        }
    }

    public Map<String, Object> getData() {
        return data;
    }

    public void setData(Map<String, Object> data) {
        this.data = data;
    }

    public void putData(String key, Object value) {
        data.put(key, value);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((uri == null) ? 0 : uri.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        CrawleableUri other = (CrawleableUri) obj;
        if (uri == null) {
            if (other.uri != null)
                return false;
        } else if (!uri.equals(other.uri))
            return false;
        return true;
    }

    /**
     *
     * @return
     * @deprecated Use the JSON serialization instead.
     */
    public ByteBuffer toByteBuffer() {
        byte uriBytes[] = uri.toString().getBytes(ENCODING_CHARSET);
        int bytesLength = 6 + uriBytes.length;
        byte ipAddressBytes[] = null;
        if (ipAddress != null) {
            ipAddressBytes = ipAddress.getAddress();
            bytesLength += ipAddressBytes.length;
        }
        ByteBuffer buffer = ByteBuffer.allocate(bytesLength);
        buffer.putInt(uriBytes.length);
        buffer.put(uriBytes);
        if (ipAddressBytes != null) {
            buffer.put((byte) ipAddressBytes.length);
            buffer.put(ipAddressBytes);
        } else {
            buffer.put((byte) 0);
        }
        return buffer;
    }

    /**
     *
     * @return
     * @deprecated Use the JSON serialization instead.
     */
    public byte[] toByteArray() {
        return toByteBuffer().array();
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("CrawleableUri(\"");
        builder.append(uri.toString());
        builder.append("\",");
        if (ipAddress != null) {
            builder.append(ipAddress.toString());
        }
        builder.append(')');
        return builder.toString();
    }

    public long getTimestampNextCrawl() {
        return timestampNextCrawl;
    }

    public void setTimestampNextCrawl(long timestampNextCrawl) {
        this.timestampNextCrawl = timestampNextCrawl;
    }
}