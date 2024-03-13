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

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.Base64;
import java.util.ArrayList;
import java.util.List;
import java.util.Collection;

import org.archive.crawler.util.PUC;
import org.archive.modules.CrawlURI;
import org.archive.net.UURI;

import org.json.JSONObject;
import org.json.JSONArray;
import org.json.JSONException;

import com.sleepycat.bind.EntryBinding;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.OperationStatus;

/**
 * A Frontier tracking duplicated URIs and prioritizing those which according
 * to relearning2crawl should be prioritized. If a URI has to be prioritized,
 * it is checked that the URI has not been downloaded yet, and if that is the
 * case, it is removed from the queue, if needed, and the URI is enqueued and
 * prioritized
 * 
 * It should be used together with NoopUriUniqFilter or similar for detecting
 * duplicated URIs since relearning2crawl will handle duplicated, but should
 * not harm either
 * 
 * A constant precedence policy is assumed with value equal to 1, which is the
 * default configuration: CostUriPrecedencePolicy + UnitCostAssignmentPolicy
 *
 * @author cgr71ii
 */
public class BdbRelearning2CrawlFrontier extends BdbFrontier {
    @SuppressWarnings("unused")
    private static final long serialVersionUID = 1L;

    private static final Logger logger =
        Logger.getLogger(BdbRelearning2CrawlFrontier.class.getName());

    private Map<String, CrawlURI> alreadySeen = new HashMap<>();

    private String lastSeenURI = null;

    //private List<CrawlURI> lastSeenURIOutlinks = new ArrayList<>();
    private List<String> lastSeenURIOutlinks = new ArrayList<>();

    public BdbRelearning2CrawlFrontier() {
        super();
    }

    {
        setRelearning2CrawlServerUrl("http://localhost:5000");
        setUserAgent(String.format("heritrix: %s", BdbRelearning2CrawlFrontier.class.getName()));
        setLangPreference("");
        setUrlsBase64(true);
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

    public String getRelearning2CrawlServerUrl() {
        return (String) kp.get("relearning2crawlServerUrl");
    }

    public void setRelearning2CrawlServerUrl(String url) {
        kp.put("relearning2crawlServerUrl", url);
    }

    public String getUserAgent() {
        return (String) kp.get("userAgent");
    }

    public void setUserAgent(String user_agent) {
        kp.put("userAgent", user_agent);
    }

    public String getLangPreference() {
        return (String) kp.get("langPreference");
    }

    public void setLangPreference(String lang_preference) {
        kp.put("langPreference", lang_preference);
    }

    public Boolean getUrlsBase64() {
        return (Boolean) kp.get("urlsBase64");
    }

    public void setUrlsBase64(Boolean urls_base64) {
        kp.put("urlsBase64", urls_base64);
    }

    private synchronized boolean removeURI(WorkQueue wq, String curiURI) {
        try {
            EntryBinding<CrawlURI> crawlUriBinding = pendingUris.getBinding();
            DatabaseEntry key = new DatabaseEntry();
            DatabaseEntry value = new DatabaseEntry();
            Database pendingUrisDB = pendingUris.getPendingUrisDB();
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
                logger.warning(String.format("URI not found: %s", curiURI));

                return false;
            }

            doesExist.setHolderKey(key);
            //pendingUris.delete(doesExist);
            wq.dequeueSpecificURI(this, doesExist);
            decrementQueuedCount(1);

            if (logger.isLoggable(Level.FINE)) {
                logger.fine("URI removed: " + curiURI);
            }
        }
        catch (Exception e) {
            logger.warning(String.format("URI could not be removed: %s. Exception: %s", curiURI, e.toString()));

            return false;
        }

        return true;
    }

    @Override
    public void terminate() {
        // Method called when the current job is finished
        // https://github.com/internetarchive/heritrix3/wiki/Frontier : "There is only one Frontier per crawl job."

        String reportsPath = getCrawlController().getStatisticsTracker().getReportsDir().getFile().toString();

        logger.info("Job terminated: reports directory: " + reportsPath);

        alreadySeen.clear(); // More memory efficient
        System.gc();

        super.terminate();
    }

    @Override
    public void receive(CrawlURI curi) {
        String curiURI = curi.getCanonicalString();
        //Boolean forceDownload = curi.forceFetch();
        WorkQueue wq = getQueueFor(curi.getClassKey());
        boolean curiSeed = curi.isSeed();
        String langPreference = getLangPreference();

        if (langPreference.length() != 2) {
            throw new RuntimeException(String.format("Unexpected langPreference length: got %d, but 2 was expected: %s", langPreference.length(), langPreference));
        }

        //logger.info("URI: " + curiURI);

        if (wq != null && !curiSeed) {
            if (this.lastSeenURI == null) {
                throw new RuntimeException("this.lastSeenURI is null and should not");
            }

            synchronized (wq) {
                // Is the last URL of the last downloaded file?
                CrawlURI viacuriURI = curi.getFullVia();
                String viaURI = viacuriURI.getCanonicalString();

                this.lastSeenURIOutlinks.add(curiURI);
                alreadySeen.put(curiURI, curi);

                int lastSeenURIOutlinksCount = this.lastSeenURIOutlinks.size();

                if (lastSeenURIOutlinksCount == 1) {
                    // Via URI should be new

                    if (viaURI.equals(this.lastSeenURI)) {
                        throw new RuntimeException(String.format("viaURI.equals(this.lastSeenURI): %s == %s", viaURI, this.lastSeenURI));
                    }

                    this.lastSeenURI = viaURI;
                }

                if (!viaURI.equals(this.lastSeenURI)) {
                    throw new RuntimeException(String.format("!viaURI.equals(this.lastSeenURI): %s vs %s", viaURI, this.lastSeenURI));
                }

                CrawlURI previousURI = alreadySeen.get(viaURI);

                if (previousURI == null) {
                    throw new RuntimeException(String.format("URI is null and should not (previousURI): %s", viaURI));
                }

                Collection<CrawlURI> previousURIOutlinks = previousURI.getOutLinks();
                int previousURIOutlinksCount = previousURIOutlinks.size();

                if (lastSeenURIOutlinksCount > previousURIOutlinksCount) {
                    throw new RuntimeException(String.format("lastSeenURIOutlinksCount > previousURIOutlinksCount: %d vs %d", lastSeenURIOutlinksCount, previousURIOutlinksCount));
                }

                if (lastSeenURIOutlinksCount == previousURIOutlinksCount) {
                    // This should be the last child URL of previousURI -> process stored URLs

                    String[] langResults = PUC.isLangOk(viacuriURI, langPreference, "placeholder", true, logger);
                    String langResult = langResults[1];
                    String requestParam = null;

                    if (getUrlsBase64()) {
                        requestParam = String.format("parent_url=%s&parent_lang=%s",
                                                            Base64.getEncoder().encodeToString(viaURI.getBytes()).replace('+', '_'),
                                                            langResult);
                    }
                    else {
                        requestParam = String.format("parent_url=%s&parent_lang=%s", viaURI, langResult);
                    }

                    for (String lastSeenURIOutlink : this.lastSeenURIOutlinks) {
                        if (getUrlsBase64()) {
                            requestParam = String.format("%s&child_url=%s",
                                                         requestParam,
                                                         Base64.getEncoder().encodeToString(lastSeenURIOutlink.getBytes()).replace('+', '_'));
                        }
                        else {
                            requestParam = String.format("%s&child_url=%s", requestParam, lastSeenURIOutlink);
                        }
                    }

                    boolean requestOK = false;
                    String requestResult = null;

                    for (int tries = 0; tries < 3; tries += 1) {
                        try {
                            requestResult = PUC.sendPOST(getRelearning2CrawlServerUrl(), requestParam, getUserAgent());

                            requestOK = true;
                            break;
                        } catch (Exception e) {
                            logger.warning(String.format("Try #%d: exception: %s", tries, e.toString()));
                        }
                    }

                    if (!requestOK) {
                        throw new RuntimeException(String.format("Coult not process the request: %s", curiURI));
                    }
                    if (requestResult == null) {
                        throw new RuntimeException(String.format("Unexpected null value in the request: %s", curiURI));
                    }

                    String nextURI2DownloadURI = null;
                    List<String> relearning2crawlFilteredURIs = new ArrayList<String>();

                    // Process request response
                    JSONObject obj = new JSONObject(requestResult);

                    try {
                        JSONObject obj2 = obj.getJSONObject("ok");
                        nextURI2DownloadURI = obj2.getString("action_url");
                        JSONArray filteredURIs = obj2.getJSONArray("filtered_urls");

                        for (int i = 0; i < filteredURIs.length(); i += 1) {
                            String filteredURI = filteredURIs.getString(i);

                            relearning2crawlFilteredURIs.add(filteredURI);
                        }
                    } catch (JSONException e) {
                        try {
                            String error = obj.getString("err");

                            logger.warning(String.format("Request 'err' field: %s. Exception: %s", error, e.toString()));
                        } catch (JSONException e2) {
                            logger.warning(String.format("Could not process 'err' field from the request. Exception: %s. Exception 2: %s", e.toString(), e2.toString()));
                        }

                        throw new RuntimeException(String.format("Coult not process the request: %s. JSON exception: %s", curiURI, e.toString()));
                    } catch (Exception e) {
                        throw new RuntimeException(String.format("Coult not process the request: %s. Exception: %s", curiURI, e.toString()));
                    }

                    // Filter URIs according to relearning2crawl

                    for (String filteredURI : relearning2crawlFilteredURIs) {
                        if (!this.lastSeenURIOutlinks.contains(filteredURI)) {
                            throw new RuntimeException(String.format("URI should be in this.lastSeenURIOutlinks (size: %d): %s (got %d URIs from relearning2crawl)",
                                                                     lastSeenURIOutlinksCount, filteredURI, relearning2crawlFilteredURIs.size()));
                        }

                        CrawlURI outlinkuri = alreadySeen.get(filteredURI);

                        if (outlinkuri == null) {
                            throw new RuntimeException(String.format("URI is null and should not (outlinkuri): %s", filteredURI));
                        }

                        super.receive(outlinkuri); // Enqueue all URIs from the current via (there should not be more than the current stored URIs)
                    }

                    CrawlURI nextURI2Download = alreadySeen.get(nextURI2DownloadURI);

                    if (nextURI2Download == null) {
                        throw new RuntimeException(String.format("URI is null and should not (nextURI2Download): %s", nextURI2DownloadURI));
                    }

                    Boolean removedURI = null;

                    if (curi.getClassKey().equals(nextURI2Download.getClassKey())) {
                        removedURI = this.removeURI(wq, nextURI2DownloadURI);
                    }
                    else {
                        WorkQueue wq2 = getQueueFor(nextURI2Download.getClassKey());

                        synchronized (wq2) {
                            removedURI = this.removeURI(wq2, nextURI2DownloadURI);
                        }
                    }

                    if (removedURI) {
                        nextURI2Download.setPrecedence(nextURI2Download.getPrecedence() + 1);

                        super.receive(nextURI2Download);
                    }

                    this.lastSeenURIOutlinks.clear();
                }

                return;
            }
        }
        else {
            if (wq == null && !curiSeed) {
                logger.warning(String.format("wq is not initialized and URI is not seed? %s", curiURI));
            }

            if (logger.isLoggable(Level.FINE)) {
                if (wq == null && curiSeed) {
                    logger.fine("New queue for URI (seed): " + curiURI);
                }
                else {
                    if (wq == null) {
                        logger.fine("New queue for URI: " + curiURI);
                    }
                    if (curiSeed) {
                        logger.fine("New seed URI: " + curiURI);
                    }
                }
            }

            if (curiSeed && this.lastSeenURI != null) {
                // TODO multiple seeds can be provided for the same website, and that would not be a bug
                //      This class is being currently designed for 1 seed per website
                logger.warning(String.format("URI is seed but has been previous executions of this method: bug? Last seen URI: %s ; current URI: %s", this.lastSeenURI, curiURI));
            }
        }

        alreadySeen.put(curiURI, curi);

        //logger.info("URI being enqueued: " + curiURI);
        this.lastSeenURI = curiURI;

        super.receive(curi); // Enqueue
    }

}
