/*
 * Copyright 2017 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License")
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.spinnaker.clouddriver.elasticsearch.converters;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.spinnaker.clouddriver.core.services.Front50Service;
import com.netflix.spinnaker.clouddriver.elasticsearch.descriptions.BulkDeleteEntityTagsDescription;
import com.netflix.spinnaker.clouddriver.elasticsearch.model.ElasticSearchEntityTagsProvider;
import com.netflix.spinnaker.clouddriver.elasticsearch.ops.BulkDeleteEntityTagsAtomicOperation;
import com.netflix.spinnaker.clouddriver.orchestration.AtomicOperation;
import com.netflix.spinnaker.clouddriver.orchestration.AtomicOperationConverter;
import com.netflix.spinnaker.clouddriver.security.AccountCredentialsProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component("bulkDeleteEntityTagsDescription")
public class BulkDeleteEntityTagsAtomicOperationConverter implements AtomicOperationConverter {

  private final ObjectMapper objectMapper;
  private final Front50Service front50Service;
  private final AccountCredentialsProvider accountCredentialsProvider;
  private final ElasticSearchEntityTagsProvider entityTagsProvider;

  @Autowired
  public BulkDeleteEntityTagsAtomicOperationConverter(ObjectMapper objectMapper,
                                                      Front50Service front50Service,
                                                      AccountCredentialsProvider accountCredentialsProvider,
                                                      ElasticSearchEntityTagsProvider entityTagsProvider) {
    this.objectMapper = objectMapper;
    this.front50Service = front50Service;
    this.accountCredentialsProvider = accountCredentialsProvider;
    this.entityTagsProvider = entityTagsProvider;
  }

  @Override
  public AtomicOperation convertOperation(Map input) {
    return new BulkDeleteEntityTagsAtomicOperation(
      front50Service, accountCredentialsProvider, entityTagsProvider, convertDescription(input)
    );
  }

  public BulkDeleteEntityTagsDescription convertDescription(Map input) {
    return objectMapper.convertValue(input, BulkDeleteEntityTagsDescription.class);
  }
}
