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
package org.apache.pulsar.broker.service.trace;

import javax.servlet.http.HttpServletRequest;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.pulsar.broker.service.ServerCnx;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TopicTraceContext {
    private TopicTraceSourceType sourceType;
    private String principal;
    private String originalPrincipal;
    private String clientAddress;
    private Long requestId;
    private String requestPath;
    private String requestMethod;
    private String listenerName;

    public static TopicTraceContext system() {
        return TopicTraceContext.builder().sourceType(TopicTraceSourceType.SYSTEM).build();
    }

    public static TopicTraceContext internal() {
        return TopicTraceContext.builder().sourceType(TopicTraceSourceType.INTERNAL).build();
    }

    public static TopicTraceContext fromServerCnx(ServerCnx cnx, long requestId) {
        return TopicTraceContext.builder()
                .sourceType(TopicTraceSourceType.BINARY)
                .principal(cnx.getPrincipal())
                .clientAddress(cnx.clientSourceAddress())
                .requestId(requestId)
                .build();
    }

    public static TopicTraceContext fromHttp(HttpServletRequest request, String principal, String originalPrincipal) {
        return TopicTraceContext.builder()
                .sourceType(TopicTraceSourceType.HTTP)
                .principal(principal)
                .originalPrincipal(originalPrincipal)
                .clientAddress(request == null ? null : request.getRemoteAddr())
                .requestPath(request == null ? null : request.getRequestURI())
                .requestMethod(request == null ? null : request.getMethod())
                .build();
    }
}
