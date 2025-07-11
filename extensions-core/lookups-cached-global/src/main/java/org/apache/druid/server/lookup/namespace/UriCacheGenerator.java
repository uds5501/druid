/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.server.lookup.namespace;

import com.google.common.io.ByteSource;
import com.google.inject.Inject;
import org.apache.druid.data.SearchableVersionedDataFinder;
import org.apache.druid.data.input.MapPopulator;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.RetryUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.lookup.namespace.CacheGenerator;
import org.apache.druid.query.lookup.namespace.UriExtractionNamespace;
import org.apache.druid.segment.loading.URIDataPuller;
import org.apache.druid.server.lookup.namespace.cache.CacheHandler;
import org.apache.druid.server.lookup.namespace.cache.CacheScheduler;
import org.apache.druid.utils.CompressionUtils;
import org.apache.druid.utils.RuntimeInfo;

import javax.annotation.Nullable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Map;
import java.util.regex.Pattern;

/**
 *
 */
public final class UriCacheGenerator implements CacheGenerator<UriExtractionNamespace>
{
  private static final int DEFAULT_NUM_RETRIES = 3;
  private static final Logger log = new Logger(UriCacheGenerator.class);
  private final Map<String, SearchableVersionedDataFinder> pullers;
  private final long maxMemory;

  @Inject
  public UriCacheGenerator(
      Map<String, SearchableVersionedDataFinder> pullers,
      RuntimeInfo runtimeInfo
  )
  {
    this.pullers = pullers;
    this.maxMemory = runtimeInfo.getMaxHeapSizeBytes();
  }

  @Override
  @Nullable
  public String generateCache(
      final UriExtractionNamespace extractionNamespace,
      final CacheScheduler.EntryImpl<UriExtractionNamespace> entryId,
      @Nullable final String lastVersion,
      final CacheHandler cache
  ) throws Exception
  {
    final boolean doSearch = extractionNamespace.getUriPrefix() != null;
    final URI originalUri = doSearch ? extractionNamespace.getUriPrefix() : extractionNamespace.getUri();
    final SearchableVersionedDataFinder<URI> pullerRaw = pullers.get(originalUri.getScheme());
    if (pullerRaw == null) {
      throw new IAE(
          "Unknown loader type[%s].  Known types are %s",
          originalUri.getScheme(),
          pullers.keySet()
      );
    }
    if (!(pullerRaw instanceof URIDataPuller)) {
      throw new IAE(
          "Cannot load data from location [%s]. Data pulling from [%s] not supported",
          originalUri,
          originalUri.getScheme()
      );
    }
    final URIDataPuller puller = (URIDataPuller) pullerRaw;
    final URI uri;
    if (doSearch) {
      final Pattern versionRegex;

      if (extractionNamespace.getFileRegex() != null) {
        versionRegex = Pattern.compile(extractionNamespace.getFileRegex());
      } else {
        versionRegex = null;
      }
      uri = pullerRaw.getLatestVersion(
          extractionNamespace.getUriPrefix(),
          versionRegex
      );

      if (uri == null) {
        throw new FileNotFoundException(
            StringUtils.format(
                "Could not find match for pattern `%s` in [%s] for %s",
                versionRegex,
                originalUri,
                extractionNamespace
            )
        );
      }
    } else {
      uri = extractionNamespace.getUri();
    }

    return RetryUtils.retry(
        () -> {
          final String version = puller.getVersion(uri);
          try {
            // Important to call equals() against version because lastVersion could be null
            if (version.equals(lastVersion)) {
              log.debug(
                  "URI [%s] for [%s] has the same last modified time [%s] as the last cached. " +
                  "Skipping ",
                  uri.toString(),
                  entryId,
                  version
              );
              return null;
            }
          }
          catch (NumberFormatException ex) {
            log.debug(ex, "Failed to get last modified timestamp. Assuming no timestamp");
          }
          final ByteSource source = new ByteSource()
          {
            @Override
            public InputStream openStream() throws IOException
            {
              return CompressionUtils.decompress(puller.getInputStream(uri), uri.getPath());
            }
          };

          try {
            final long startNs = System.nanoTime();
            final MapPopulator.PopulateResult populateResult = new MapPopulator<>(
                extractionNamespace.getNamespaceParseSpec().getParser()
            ).populateAndWarnAtByteLimit(
                source,
                cache.getCache(),
                (long) (maxMemory * extractionNamespace.getMaxHeapPercentage() / 100.0),
                null == entryId ? null : entryId.toString()
            );
            final long duration = System.nanoTime() - startNs;
            log.info(
                "Finished loading %d values (%d bytes) from %d lines for [%s] in %d ns",
                populateResult.getEntries(),
                populateResult.getBytes(),
                populateResult.getLines(),
                entryId,
                duration
            );
            return version;
          }
          catch (Throwable t) {
            try {
              cache.close();
            }
            catch (Exception e) {
              t.addSuppressed(e);
            }
            throw t;
          }
        },
        puller.shouldRetryPredicate(),
        DEFAULT_NUM_RETRIES
    );
  }
}
