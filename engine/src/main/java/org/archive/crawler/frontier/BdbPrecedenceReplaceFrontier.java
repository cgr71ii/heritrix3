/*
 *  This file is part of the Heritrix web crawler (crawler.archive.org).
 *
 *  Licensed to the Internet Archive (IA) by one or more individual 
 *  contributors. 
 *
 *  The IA licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.archive.crawler.frontier;

import java.io.File;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.collections.Closure;
import org.archive.modules.CrawlURI;
import org.archive.net.UURI;

import com.sleepycat.bind.EntryBinding;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.OperationStatus;

/**
 * A Frontier tracking duplicated URIs and replacing them in case that the
 * precedence value is better. If a URI with better precedence is detected,
 * it is checked that the enqueued URI has not been downloaded yet, and if
 * that is the case, it is removed from the queue and the new URI is enqueued
 * 
 * It should be used together with NoopUriUniqFilter or similar for detecting
 * duplicated URIs
 *
 * @author cgr71ii
 */
public class BdbPrecedenceReplaceFrontier extends BdbFrontier {
    @SuppressWarnings("unused")
    private static final long serialVersionUID = 1L;

    private static final Logger logger =
        Logger.getLogger(BdbPrecedenceReplaceFrontier.class.getName());

    private Map<String, CrawlURI> alreadySeen = new HashMap<>();

    public BdbPrecedenceReplaceFrontier() {
        super();
    }

    public Boolean getLoggerFine() {
        return (Boolean) kp.get("loggerFine");
    }

    public void setLoggerFine(Boolean logger_fine) {
        kp.put("loggerFine", logger_fine);

        if (getLoggerFine()) {
            logger.setLevel(Level.FINE);
        }
        else {
            logger.setLevel(Level.INFO);
        }
    }

    @Override
    public void terminate() {
        // Method called when the current job is finished
        // https://github.com/internetarchive/heritrix3/wiki/Frontier : "There is only one Frontier per crawl job."

        File reportsPath = getCrawlController().getStatisticsTracker().getReportsDir().getFile();
        String[] reportsPathElements = reportsPath.list();
        String jobName = reportsPath.toString();

        if (reportsPathElements.length >= 3) {
            jobName = reportsPathElements[reportsPathElements.length - 3];
        }

        logger.info("Job terminated: " + jobName);

        alreadySeen.clear(); // More memory efficient
        System.gc();

        super.terminate();
    }

    @Override
    public void receive(CrawlURI curi) {
        String curiURI = curi.getCanonicalString();
        Boolean forceDownload = curi.forceFetch();
        CrawlURI seenURI = alreadySeen.get(curiURI);
        Database pendingUrisDB = pendingUris.getPendingUrisDB();
        EntryBinding<CrawlURI> crawlUriBinding = pendingUris.getBinding();
        WorkQueue wq = getQueueFor(curi.getClassKey());

        //logger.info("URI: " + curiURI);

        if (wq != null) {
            synchronized (wq) {
                /*
                Closure logAllUris = new Closure() {
                    public void execute(Object curi2) {
                        DatabaseEntry de = (DatabaseEntry)((CrawlURI) curi2).getHolderKey();
        
                        if (de == null) {
                            logger.info("Pending URI: " + ((CrawlURI) curi2).getCanonicalString() + " : is null");
                        }
                        else {
                            logger.info("Pending URI: " + ((CrawlURI) curi2).getCanonicalString() + " : " + de.toString());
                        }
                    }
                };

                forAllPendingDo(logAllUris);
                */

                CrawlURI peekUri = wq.peek(this);

                if (peekUri != null) {
                    // Is the current CURI being already processed or will be really soon?
                    UURI via = peekUri.getVia();
                    CrawlURI via_curi = null;

                    if (via != null) {
                        via_curi = peekUri.getFullVia();
                    }

                    if (peekUri.getCanonicalString().equals(curiURI) || (via_curi != null && via_curi.getCanonicalString().equals(curiURI))) {
                        if (logger.isLoggable(Level.FINE)) {
                            logger.fine("URI being processed right now or is next: " + curiURI);
                        }
                        return;
                    }
                }
                if (seenURI != null && !forceDownload) {
                    // URI has been seen before
                    int currentPrecedence = curi.getPrecedence();
                    int seenPrecedence = seenURI.getPrecedence();

                    if (currentPrecedence < seenPrecedence) {
                        // Try to remove seen URI
                        if (logger.isLoggable(Level.FINE)) {
                            logger.fine("URI with better precedence: " + curiURI + " : " + currentPrecedence + " < " + seenPrecedence);
                        }

                        try {
                            DatabaseEntry key = new DatabaseEntry();
                            DatabaseEntry value = new DatabaseEntry();
                            Cursor cursor = pendingUrisDB.openCursor(null, null);
                            CrawlURI doesExist = null;

                            // Iterate until find the current URI
                            while (cursor.getNext(key, value, null) == OperationStatus.SUCCESS) {
                                if (value.getData().length == 0) {
                                    continue;
                                }

                                CrawlURI item = (CrawlURI) crawlUriBinding.entryToObject(value);

                                if (item.getCanonicalString().equals(curiURI)) {
                                    doesExist = item;
                                    //logger.info("URI found!!!!!!!!!!!!!!!! : " + curiURI + " : " + key.toString());
                                    break;
                                }
                            }
                            cursor.close();

                            //CrawlURI doesExist = pendingUris.get(de);

                            if (doesExist == null) {
                                // URI is not in the "pending URIs" queue anymore -> we don't want to download again
                                if (logger.isLoggable(Level.FINE)) {
                                    logger.fine("URI already downloaded: " + curiURI);
                                }
                                return;
                            }

                            doesExist.setHolderKey(key);
                            //pendingUris.delete(doesExist);
                            wq.dequeueSpecificURI(this, doesExist);
                            decrementQueuedCount(1);

                            if (logger.isLoggable(Level.FINE)) {
                                logger.fine("URI updated: " + curiURI);
                            }
                        }
                        catch (Exception e) {
                            logger.warning("URI couldn't be updated and won't be downloaded again: " + curiURI + " : " + e.toString());
                            return;
                        }
                    }
                    else {
                        // Worse precedence -> not a better option -> do not enqueue
                        if (logger.isLoggable(Level.FINE)) {
                            logger.fine("URI with worse precedence: " + curiURI + " -> " + currentPrecedence + " >= " + seenPrecedence);
                        }
                        return;
                    }
                }
                //else {
                //    if (seenURI == null) {
                //        logger.info("New URI: " + curiURI);
                //    }
                //    if (forceDownload) {
                //        logger.info("URI forceFetch: " + curiURI);
                //    }
                //}
            }
        }
        else {
            if (logger.isLoggable(Level.FINE)) {
                logger.fine("New queue for URI: " + curiURI);
            }
        }

        alreadySeen.put(curiURI, curi);

        //logger.info("URI being enqueued: " + curiURI);

        super.receive(curi); // Enqueue
    }

}
