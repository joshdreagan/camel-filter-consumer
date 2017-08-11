/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.camel.examples;

import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.model.dataformat.JsonLibrary;
import org.apache.camel.spring.spi.SpringTransactionPolicy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;

@Component
public class CamelRouteConfiguration extends RouteBuilder {

  @Autowired
  private ApplicationProperties properties;

  @Autowired
  private ApplicationContext applicationContext;
  
  @Override
  public void configure() {
    fromF("file:%s?moveFailed=.failed", properties.getFile().getMessagesDirectory())
      .routeId("fileConsumerRoute")
      .unmarshal().json(JsonLibrary.Jackson)
      .multicast().parallelProcessing(true)
        .to("direct:dbInsert")
        .to("direct:fileWrite")
      .end()
    ;
    
    from("direct:dbInsert")
      .routeId("dbInserterRoute")
      .transacted("requiredTransactionPolicy")
      .filter(method(applicationContext.getBean("producerTemplate", ProducerTemplate.class), "requestBody('direct:checkDb',${body},${type:java.lang.Boolean})"))
        .log("Inserting into DB...")
        .toF("sql:insert into %s values (:#${body['id']},:#${body['message']},:#${date:now})?dataSource=#dataSource", properties.getSql().getMessagesTable())
        .log("Successfully inserted into DB.")
      .end()
    ;
    
    from("direct:checkDb")
      .routeId("dbCheckerRoute")
      .log("Checking DB...")
      .toF("sql:select count(*) from %s where ID=:#${body['id']}?dataSource=#dataSource&outputType=SelectOne", properties.getSql().getMessagesTable())
      .setBody(simple("${body} <= 0", Boolean.class))
      .log("DB record not exists: [${body}]")
    ;
    
    from("direct:fileWrite")
      .routeId("fileWriterRoute")
      .filter(method(applicationContext.getBean("producerTemplate", ProducerTemplate.class), "requestBody('direct:checkFile',${body},${type:java.lang.Boolean})"))
        .log("Writing file...")
        .setHeader(Exchange.FILE_NAME, simple("${body['id']}.txt"))
        .setBody(simple("${body['message']}"))
        .toF("file:%s?fileExist=Fail", properties.getFile().getProcessedDirectory())
        .log("Successfully wrote file.")
      .end()
    ;
    
    from("direct:checkFile")
      .routeId("fileCheckerRoute")
      .log("Checking file...")
      .setBody().groovy(String.format("import java.nio.file.*; !Files.exists(Paths.get('%s', request.body['id']+'.txt'));", properties.getFile().getProcessedDirectory()))
      .log("File not exists: [${body}]")
    ;
  }
  
  @Bean
  ProducerTemplate producerTemplate(CamelContext camelContext) {
    return camelContext.createProducerTemplate();
  }
  
  @Bean
  SpringTransactionPolicy requiredTransactionPolicy(PlatformTransactionManager transactionManager) {
    SpringTransactionPolicy policy = new SpringTransactionPolicy(transactionManager);
    policy.setPropagationBehaviorName("PROPAGATION_REQUIRED");
    return policy;
  }
}
