package org.dice_research.squirrel.robots;

import org.dice_research.squirrel.data.uri.CrawleableUri;

/**
 * Interface of a class that can be used to access the robots.txt files. This
 * interface hides the whole retrieving and processing of the robots.txt files
 * an provides only those methods, that are needed by a worker of a crawler.
 * 
 * @author Michael R&ouml;der (roeder@informatik.uni-leipzig.de)
 *
 */
public interface RobotsManager {

    /**
     * Returns true, if the robots.txt file does not forbid the crawling of that
     * URI.
     * 
     * @param curi
     *            the URI that should be crawled
     * @return false, if the robots.txt forbids the crawling of this URI, else
     *         true.
     */
    public boolean isUriCrawlable(CrawleableUri curi);

    /**
     * Returns the minimum time a crawler should wait before sending a new
     * request to the given domain. If the robots.txt file can not be retrieved
     * or does not define a minimum waiting time, 0 is returned.
     * 
     * @param curi
     *            a URI containing the domain to which two or more requests
     *            should be send.
     * @return the minimum time to wait in milliseconds.
     */
    public long getMinWaitingTime(CrawleableUri curi);
}
