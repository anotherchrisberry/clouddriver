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

package com.netflix.spinnaker.clouddriver.elasticsearch.validators;

import com.netflix.spinnaker.clouddriver.deploy.DescriptionValidator;
import com.netflix.spinnaker.clouddriver.elasticsearch.descriptions.BulkDeleteEntityTagsDescription;
import org.springframework.stereotype.Component;
import org.springframework.validation.Errors;

import java.util.List;

@Component("bulkDeleteEntityTagsDescriptionValidator")
public class BulkDeleteEntityTagsDescriptionValidator extends DescriptionValidator<BulkDeleteEntityTagsDescription> {

  @Override
  public void validate(List priorDescriptions, BulkDeleteEntityTagsDescription description, Errors errors) {
    int ids = description.ids.isEmpty() ? 0 : 1;
    int entityRefs = description.entityRefs.isEmpty() ? 0 : 1;
    int entityType = description.entityType == null ? 0 : 1;
    if (ids + entityRefs + entityType != 1) {
      errors.rejectValue("entityTags.selection",
        "Exactly one of 'ids', 'entityRefs', 'namespace', or 'entityType' can be specified in a bulk delete operation");
    }
    if (description.namespace == null && (description.tags == null || description.tags.isEmpty())) {
      errors.rejectValue("tags", "At least one tag or namespace must be provided");
    }
  }
}
