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

package com.netflix.spinnaker.clouddriver.elasticsearch.ops

import com.netflix.spinnaker.clouddriver.core.services.Front50Service
import com.netflix.spinnaker.clouddriver.data.task.Task
import com.netflix.spinnaker.clouddriver.data.task.TaskRepository
import com.netflix.spinnaker.clouddriver.elasticsearch.descriptions.BulkDeleteEntityTagsDescription
import com.netflix.spinnaker.clouddriver.elasticsearch.model.ElasticSearchEntityTagsProvider
import com.netflix.spinnaker.clouddriver.model.EntityTags
import com.netflix.spinnaker.clouddriver.model.EntityTags.EntityRef
import com.netflix.spinnaker.clouddriver.model.EntityTags.EntityTag
import com.netflix.spinnaker.clouddriver.security.AccountCredentials
import com.netflix.spinnaker.clouddriver.security.AccountCredentialsProvider
import spock.lang.Specification

class BulkDeleteEntityTagsAtomicOperationSpec extends Specification {

  def testCredentials = Mock(AccountCredentials) {
    getAccountId() >> { return "100" }
    getName() >> { return "test" }
  }

  def front50Service = Mock(Front50Service)
  def accountCredentialsProvider = Stub(AccountCredentialsProvider) {
    getCredentials("test") >> testCredentials
  }
  def entityTagsProvider = Mock(ElasticSearchEntityTagsProvider)

  BulkDeleteEntityTagsDescription description
  BulkDeleteEntityTagsAtomicOperation operation

  def setupSpec() {
    TaskRepository.threadLocalTask.set(Mock(Task))
  }

  def setup() {
    description = new BulkDeleteEntityTagsDescription()
    operation = new BulkDeleteEntityTagsAtomicOperation(front50Service, accountCredentialsProvider, entityTagsProvider, description)
  }

  void "should get matches from elastic search if entityType provided"() {
    given:
    description.entityType = "servergroup"
    description.tags = ["a"]

    when:
    operation.operate([])

    then:
    1 * entityTagsProvider.getAll(null, "servergroup", null, null, null, null, null, [a: "*"], BulkDeleteEntityTagsAtomicOperation.MAX_RESULTS) >> []
    1 * front50Service.bulkFetch([]) >> []
    0 * _
  }

  void "should build ids when entityRefs provided"() {
    given:
    description.entityRefs = [
      new EntityRef(entityId: "myapp-v001", entityType: "servergroup", region: "us-east-1", account: "test", cloudProvider: "aws"),
      new EntityRef(entityId: "myapp-v002", entityType: "servergroup", region: "us-east-1", account: "test", cloudProvider: "aws")
    ]
    description.tags = ["a"]

    when:
    operation.operate([])

    then:
    1 * front50Service.bulkFetch(
      ["aws:servergroup:myapp-v001:100:us-east-1", "aws:servergroup:myapp-v002:100:us-east-1"]
    ) >> []
  }

  void "should only remove declared tags and update existing tag if other tags exist"() {
    given:
    EntityTags v001Tags = new EntityTags(id: "v001", tags: [ new EntityTag(name: "a", value: 1)])
    EntityTags v002Tags = new EntityTags(id: "v002", tags: [ new EntityTag(name: "a", value: 1), new EntityTag(name: "b", value: 2)])
    EntityTags v003Tags = new EntityTags(id: "v003", tags: [ new EntityTag(name: "b", value: 2)])
    description.ids = ["v001", "v002", "v003"]
    description.tags = ["a"]

    when:
    operation.operate([])

    then:
    1 * front50Service.bulkFetch(description.ids) >> [v001Tags, v002Tags, v003Tags]
    1 * entityTagsProvider.delete("v001")
    1 * front50Service.deleteEntityTags("v001")
    1 * front50Service.batchUpdate([v002Tags]) >> [v002Tags]
    1 * entityTagsProvider.bulkIndex([v002Tags])
    0 * _

  }

  void "should iterate through all tags when only entityType is provided"() {
    given:
    EntityTags v001Tags = new EntityTags(id: "v001", tags: [ new EntityTag(name: "a", value: 1)])
    EntityTags v002Tags = new EntityTags(id: "v002", tags: [ new EntityTag(name: "a", value: 1), new EntityTag(name: "b", value: 2)])
    EntityTags v003Tags = new EntityTags(id: "v003", tags: [ new EntityTag(name: "b", value: 2)])
    description.entityType = "servergroup"
    description.tags = ["a", "b"]

    when:
    operation.operate([])

    then:
    1 * entityTagsProvider.getAll(null, "servergroup", null, null, null, null, null, [a: "*"], BulkDeleteEntityTagsAtomicOperation.MAX_RESULTS) >> [v001Tags, v002Tags]
    1 * front50Service.bulkFetch(["v001", "v002"]) >> [v001Tags, v002Tags]
    1 * entityTagsProvider.delete("v001")
    1 * front50Service.deleteEntityTags("v001")
    1 * entityTagsProvider.delete("v002")
    1 * front50Service.deleteEntityTags("v002")

    1 * entityTagsProvider.getAll(null, "servergroup", null, null, null, null, null, [b: "*"], BulkDeleteEntityTagsAtomicOperation.MAX_RESULTS) >> [v002Tags, v003Tags]
    1 * front50Service.bulkFetch(["v002", "v003"]) >> [v002Tags, v003Tags]
    1 * entityTagsProvider.delete("v003")
    1 * front50Service.deleteEntityTags("v003")
    0 * _
  }

  void "should loop through when more than 10000 matches are found when only entityType is provided"() {
    given:
    List<EntityTags> firstBatch = (1..10000).collect { new EntityTags(id: "$it", tags: [new EntityTag(name: "a"), new EntityTag(name: "b")])}
    List<EntityTags> secondBatch = (1..10).collect { new EntityTags(id: "$it", tags: [new EntityTag(name: "a")])}
    description.entityType = "servergroup"
    description.tags = ["a", "b"]

    when:
    operation.operate([])

    then:
    entityTagsProvider.getAll(null, "servergroup", null, null, null, null, null, [a: "*"], BulkDeleteEntityTagsAtomicOperation.MAX_RESULTS) >>> [firstBatch, secondBatch]
    entityTagsProvider.getAll(null, "servergroup", null, null, null, null, null, [b: "*"], BulkDeleteEntityTagsAtomicOperation.MAX_RESULTS) >>> [firstBatch, secondBatch]
    front50Service.bulkFetch(_) >>> [firstBatch, secondBatch]
  }
}
