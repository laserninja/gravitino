/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.iceberg.service.rest;

import com.google.common.base.Preconditions;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.responses.ListNamespacesResponse;
import org.apache.iceberg.rest.responses.ListTablesResponse;

/**
 * Utility for applying cursor-based in-memory pagination to Iceberg list responses.
 *
 * <p>Items are sorted deterministically by name and the page token is the name of the last item on
 * the current page. On the next request, items strictly after the cursor (by string comparison) are
 * returned. This is more resilient than offset-based pagination when the underlying catalog is
 * modified between page requests.
 *
 * <p><b>Known limitations:</b>
 *
 * <ul>
 *   <li>This is in-memory pagination: the full item list is materialized from the underlying
 *       catalog on every page request, so server-side memory usage is unchanged. Paging through N
 *       items is O(N²) total work. True server-push-down pagination would require catalog
 *       implementations to add native support.
 *   <li>Items created after the cursor but alphabetically before the last item on the current page
 *       will be missed on subsequent pages. This is the same behavior as most keyset pagination
 *       implementations and is generally acceptable.
 *   <li>When {@code pageToken} is provided without {@code pageSize}, the response returns all items
 *       after the cursor with no {@code nextPageToken}.
 * </ul>
 */
class IcebergPaginationHelper {

  private IcebergPaginationHelper() {}

  /**
   * Paginate a {@link ListNamespacesResponse}.
   *
   * @param response the full (unpaginated) response
   * @param pageToken cursor-based page token (name of last item on previous page), or {@code null}
   *     for the first page
   * @param pageSize maximum items per page, or {@code null} for no limit
   * @return a new response containing only the requested page
   */
  static ListNamespacesResponse paginateNamespaces(
      ListNamespacesResponse response, @Nullable String pageToken, @Nullable Integer pageSize) {
    PaginatedPage<Namespace> page =
        paginate(response.namespaces(), pageToken, pageSize, Namespace::toString);
    ListNamespacesResponse.Builder builder = ListNamespacesResponse.builder().addAll(page.items);
    if (page.nextPageToken != null) {
      builder.nextPageToken(page.nextPageToken);
    }
    return builder.build();
  }

  /**
   * Paginate a {@link ListTablesResponse}. Works for both table and view list responses.
   *
   * @param response the full (unpaginated) response
   * @param pageToken cursor-based page token (name of last item on previous page), or {@code null}
   *     for the first page
   * @param pageSize maximum items per page, or {@code null} for no limit
   * @return a new response containing only the requested page
   */
  static ListTablesResponse paginateTables(
      ListTablesResponse response, @Nullable String pageToken, @Nullable Integer pageSize) {
    PaginatedPage<TableIdentifier> page =
        paginate(response.identifiers(), pageToken, pageSize, TableIdentifier::toString);
    ListTablesResponse.Builder builder = ListTablesResponse.builder().addAll(page.items);
    if (page.nextPageToken != null) {
      builder.nextPageToken(page.nextPageToken);
    }
    return builder.build();
  }

  /**
   * Core pagination logic shared by all list endpoints.
   *
   * <p>Items are sorted by {@code keyExtractor} to produce a stable ordering, then sliced using the
   * cursor token. The next-page token is the key of the last item on the returned page.
   *
   * @param items the complete list of items to paginate
   * @param pageToken cursor string (key of last item on previous page), or {@code null} for the
   *     first page
   * @param pageSize maximum items per page, or {@code null} for no limit
   * @param keyExtractor function to extract a comparable cursor key from each item
   * @return a {@link PaginatedPage} containing the requested page of items and optional next token
   */
  static <T> PaginatedPage<T> paginate(
      List<T> items,
      @Nullable String pageToken,
      @Nullable Integer pageSize,
      Function<T, String> keyExtractor) {
    if (pageSize == null && (pageToken == null || pageToken.isEmpty())) {
      return new PaginatedPage<>(items, null);
    }

    if (pageSize != null) {
      Preconditions.checkArgument(pageSize > 0, "pageSize must be positive, got: %s", pageSize);
    }

    List<T> sorted =
        items.stream().sorted(Comparator.comparing(keyExtractor)).collect(Collectors.toList());

    int startIdx = 0;
    if (pageToken != null && !pageToken.isEmpty()) {
      startIdx = sorted.size();
      for (int i = 0; i < sorted.size(); i++) {
        if (keyExtractor.apply(sorted.get(i)).compareTo(pageToken) > 0) {
          startIdx = i;
          break;
        }
      }
    }

    int limit = pageSize != null ? pageSize : sorted.size();
    int end = Math.min(startIdx + limit, sorted.size());
    List<T> page = sorted.subList(startIdx, end);

    String nextToken = null;
    if (end < sorted.size() && !page.isEmpty()) {
      nextToken = keyExtractor.apply(page.get(page.size() - 1));
    }

    return new PaginatedPage<>(page, nextToken);
  }

  /** Holds a page of items and an optional token for the next page. */
  static class PaginatedPage<T> {
    final List<T> items;
    @Nullable final String nextPageToken;

    PaginatedPage(List<T> items, @Nullable String nextPageToken) {
      this.items = items;
      this.nextPageToken = nextPageToken;
    }
  }
}
