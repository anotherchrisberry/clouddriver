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

package com.netflix.spinnaker.clouddriver.elasticsearch.ops;

import com.netflix.spinnaker.clouddriver.core.services.Front50Service;
import com.netflix.spinnaker.clouddriver.data.task.Task;
import com.netflix.spinnaker.clouddriver.data.task.TaskRepository;
import com.netflix.spinnaker.clouddriver.elasticsearch.descriptions.BulkDeleteEntityTagsDescription;
import com.netflix.spinnaker.clouddriver.elasticsearch.model.ElasticSearchEntityTagsProvider;
import com.netflix.spinnaker.clouddriver.model.EntityTags;
import com.netflix.spinnaker.clouddriver.orchestration.AtomicOperation;
import com.netflix.spinnaker.clouddriver.security.AccountCredentialsProvider;
import groovy.util.logging.Slf4j;

import java.util.*;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static java.lang.String.format;

@Slf4j
public class BulkDeleteEntityTagsAtomicOperation implements AtomicOperation<Void> {

  private static final String BASE_PHASE = "ENTITY_TAGS";
  public static final int MAX_RESULTS = 10000;

  private final Front50Service front50Service;
  private final AccountCredentialsProvider accountCredentialsProvider;
  private final ElasticSearchEntityTagsProvider entityTagsProvider;
  private final BulkDeleteEntityTagsDescription bulkDeleteEntityTagsDescription;

  public BulkDeleteEntityTagsAtomicOperation(Front50Service front50Service,
                                             AccountCredentialsProvider accountCredentialsProvider,
                                             ElasticSearchEntityTagsProvider entityTagsProvider,
                                             BulkDeleteEntityTagsDescription bulkDeleteEntityTagsDescription) {
    this.front50Service = front50Service;
    this.accountCredentialsProvider = accountCredentialsProvider;
    this.entityTagsProvider = entityTagsProvider;
    this.bulkDeleteEntityTagsDescription = bulkDeleteEntityTagsDescription;
  }

  public Void operate(List priorOutputs) {
    boolean done = false;
    int currentTagIndex = 0;

    while (!done) {
      Collection<EntityTags> existingTags = getMatchingTags(currentTagIndex);
      getTask().updateStatus(BASE_PHASE, "Retrieving current entity tags");
//      List<EntityTags> durableTags = front50Service.bulkFetch(existingTags.stream().map(EntityTags::getId).collect(Collectors.toList())).stream()
//        .filter(t -> t.getTags().stream().anyMatch(t2 -> bulkDeleteEntityTagsDescription.tags.contains(t2.getName())))
//        .collect(Collectors.toList());
      if (bulkDeleteEntityTagsDescription.tags != null) {
        existingTags.forEach(tag ->
          bulkDeleteEntityTagsDescription.tags.forEach(tag::removeEntityTag)
        );
      }
      if (bulkDeleteEntityTagsDescription.namespace != null) {
        existingTags.forEach(tag ->
          tag.getTags().stream()
            .filter(t -> t.getNamespace().equals(bulkDeleteEntityTagsDescription.namespace))
            .forEach(t -> tag.removeEntityTag(t.getName()))
        );
      }

      // delete any that have no other tags
      List<EntityTags> toRemove = existingTags.stream().filter(t -> t.getTags().isEmpty()).collect(Collectors.toList());
      if (!toRemove.isEmpty()) {
        getTask().updateStatus(BASE_PHASE, format("Deleting %s entity tags", toRemove.size()));
        toRemove.forEach(t -> {
          Logger.getLogger("tagger").info("Deleting tag: " + t.getId());
          entityTagsProvider.delete(t.getId());
//          if (durableTags.stream().anyMatch(t2 -> t2.getId().equals(t.getId()))) {
//            Logger.getLogger("tagger").info("Deleting tag in Front50: " + t.getId());
          front50Service.deleteEntityTags(t.getId());
//          }
        });
        getTask().updateStatus(BASE_PHASE, format("Deleted %s entity tags", toRemove.size()));
      }

      // bulk update the others
      List<EntityTags> toUpdate = existingTags.stream().filter(t -> !t.getTags().isEmpty()).collect(Collectors.toList());
      if (!toUpdate.isEmpty()) {
        Logger.getLogger("tagger").info("updating tags: " + toUpdate.size());
        getTask().updateStatus(BASE_PHASE, format("Updating %s entity tags in Front50", toUpdate.size()));
        toUpdate.forEach(t -> {
          Logger.getLogger("tagger").info("Updating tag: " + t.getId());
          front50Service.saveEntityTags(t);
        });
//        Collection<EntityTags> updatedDurableTags = front50Service.batchUpdate(toUpdate);

        getTask().updateStatus(BASE_PHASE, format("Updating %s entity tags in ElasticSearch", toUpdate.size()));
        entityTagsProvider.bulkIndex(toUpdate);

        getTask().updateStatus(BASE_PHASE, format("Updated %s entity tags in ElasticSearch", toUpdate.size()));
      }
      if (bulkDeleteEntityTagsDescription.entityType == null) {
        done = true;
      } else {
        if (existingTags.size() < MAX_RESULTS) {
          currentTagIndex++;
        }
        if (bulkDeleteEntityTagsDescription.namespace != null) {
          done = existingTags.size() < MAX_RESULTS;
        } else {
          done = currentTagIndex == bulkDeleteEntityTagsDescription.tags.size();
        }
      }
    }
    return null;
  }

  private Collection<EntityTags> getMatchingTags(int currentTagIndex) {
    ArrayList<EntityTags> matchingTags = new ArrayList<>();
    if (!bulkDeleteEntityTagsDescription.ids.isEmpty()) {
      matchingTags.addAll(entityTagsProvider.getAll(null, bulkDeleteEntityTagsDescription.entityType, bulkDeleteEntityTagsDescription.ids, null, null, null, null, null, MAX_RESULTS));
    }
    if (!bulkDeleteEntityTagsDescription.entityRefs.isEmpty()) {
      List<String> tagIds = bulkDeleteEntityTagsDescription.entityRefs.stream().map(r -> {
        EntityTags tag = new EntityTags();
        tag.setEntityRef(r);
        return BulkUpsertEntityTagsAtomicOperation.entityRefId(accountCredentialsProvider, tag).id;
      }).collect(Collectors.toList());
      matchingTags.addAll(entityTagsProvider.getAll(null, bulkDeleteEntityTagsDescription.entityType, tagIds, null, null, null, null, null, MAX_RESULTS));
    }
    if (bulkDeleteEntityTagsDescription.entityType != null) {
      // When searching, we will look at one tag at a time, but delete all matched tags on that particular entity,
      // so it should still be reasonably efficient
      Map<String, Object> tags = null;
      if (bulkDeleteEntityTagsDescription.tags != null) {
        tags = new HashMap<>();
        tags.put(bulkDeleteEntityTagsDescription.tags.get(currentTagIndex), "*");
      }
      matchingTags.addAll(entityTagsProvider.getAll(null, bulkDeleteEntityTagsDescription.entityType, null, null, null, null, bulkDeleteEntityTagsDescription.namespace, tags, MAX_RESULTS));
    }
    if (matchingTags.isEmpty()) {
      return Collections.emptyList();
    }
    matchingTags.sort(Comparator.comparing(EntityTags::getId));
    return matchingTags;
  }

  private static Task getTask() {
    return TaskRepository.threadLocalTask.get();
  }
}
