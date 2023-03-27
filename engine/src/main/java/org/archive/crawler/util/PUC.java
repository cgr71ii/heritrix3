package org.archive.crawler.util;

import org.archive.modules.CrawlURI;

import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;

import java.nio.charset.StandardCharsets;

import org.commoncrawl.langdetect.cld2.Cld2;
import org.commoncrawl.langdetect.cld2.Result;

import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URI;

import com.google.common.net.InternetDomainName;

public class PUC {

    public static String getContent(CrawlURI curi, Logger logger) {
        String str_content = "";

        try (InputStream stream = curi.getFullVia().getRecorder().getContentReplayInputStream()) {
            str_content = new BufferedReader(
                new InputStreamReader(stream, StandardCharsets.UTF_8))
                    .lines()
                    .collect(Collectors.joining("\n"));
        }
        catch (Exception e) {
            if (logger.isLoggable(Level.WARNING)) {
                logger.warning("stream exception: " + e.toString());
            }
        }

        return str_content;
    }

    public static String[] isLangOk(CrawlURI curi, String lang1, String lang2, Boolean only_reliable, Logger logger) {
        if (lang1.equals("") || lang2.equals("")) {
            return new String[]{"", ""};
        }

        String file_content = PUC.getContent(curi, logger);
        Result lang_result = Cld2.detect(file_content, false);
        Boolean is_reliable = lang_result.isReliable();
        String best_lang_code = lang_result.getLanguageCode();

        if (logger.isLoggable(Level.FINE)) {
            logger.fine(String.format("lang<tab>reliable<tab>via: %s\t%s\t%s", best_lang_code, is_reliable, curi.getVia().toCustomString()));
        }

        if ((only_reliable && is_reliable) || !only_reliable) {
            if (best_lang_code.equals(lang1) || best_lang_code.equals(lang2)) {
                return new String[]{best_lang_code, best_lang_code};
            }
        }

        return new String[]{"", best_lang_code};
    }

    public static String sendPOST(String url, String params, String user_agent) throws IOException {
		URL obj = new URL(url);
		HttpURLConnection con = (HttpURLConnection) obj.openConnection();
		con.setRequestMethod("POST");
		con.setRequestProperty("User-Agent", user_agent);

		// For POST only - START
		con.setDoOutput(true);
		OutputStream os = con.getOutputStream();
		os.write(params.getBytes());
		os.flush();
		os.close();
		// For POST only - END

		int responseCode = con.getResponseCode();

		if (responseCode == HttpURLConnection.HTTP_OK) { //success
			BufferedReader in = new BufferedReader(new InputStreamReader(
					con.getInputStream()));
			String inputLine;
			StringBuffer response = new StringBuffer();

			while ((inputLine = in.readLine()) != null) {
				response.append(inputLine);
			}
			in.close();

            return response.toString();
		}

        return null;
	}

    public static String getDomainFromAuthority(String authority) {
        InternetDomainName idn = InternetDomainName.from(authority);

        if (idn.isPublicSuffix()) {
            return authority; // Special case: some public suffixes like s3.amazonaws.com can be both a website and a public suffix itself (https://github.com/google/guava/issues/1829)
        }

        String tld = idn.publicSuffix().toString();
        String domain_and_tld = idn.topPrivateDomain().toString();
        String domain = domain_and_tld.substring(0, domain_and_tld.length() - tld.length() - 1);

        return domain;
    }

    public static String getAuthority(String uri, Logger logger) {
        try {
            String authority = (new URI(uri)).getHost();

            if (authority == null) {
                logger.warning("host is null. uri="+uri);

                return "";
            }

            return authority;
        }
        catch (Exception e) {
            // Try to recover using regex
            if (uri.startsWith("http://") || uri.startsWith("https://")) {
                String[] authority_parts = uri.split("/");

                if (authority_parts.length > 2) {
                    return authority_parts[2];
                }
            }

            logger.log(Level.WARNING,"uri="+uri, e);
        }

        return "";
    }

    public static String getDomain(String uri, Logger logger) {
        String authority = PUC.getAuthority(uri, logger);

        if (!authority.equals("")) {
            return PUC.getDomainFromAuthority(authority);
        }

        return "";
    }

    public static String removeTrailingSlashes(String uri) {
        if (uri == null) {
            return uri;
        }

        while (uri.endsWith("/")) {
            uri = uri.substring(0, uri.length() - 1);
        }

        return uri;
    }

}
