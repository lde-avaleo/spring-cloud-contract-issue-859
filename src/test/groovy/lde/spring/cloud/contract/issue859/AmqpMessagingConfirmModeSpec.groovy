/*
 * Copyright 2013-2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package lde.spring.cloud.contract.issue859

import static com.toomuchcoding.jsonassert.JsonAssertion.assertThatJson
import static org.assertj.core.api.Assertions.assertThat
import static org.springframework.cloud.contract.verifier.messaging.util.ContractVerifierMessagingUtil.headers

import com.jayway.jsonpath.DocumentContext
import com.jayway.jsonpath.JsonPath
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import javax.inject.Inject
import org.springframework.amqp.core.AnonymousQueue
import org.springframework.amqp.core.Binding
import org.springframework.amqp.core.BindingBuilder
import org.springframework.amqp.core.Message
import org.springframework.amqp.core.Queue
import org.springframework.amqp.core.TopicExchange
import org.springframework.amqp.rabbit.annotation.RabbitListener
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.cloud.contract.verifier.messaging.boot.AutoConfigureMessageVerifier
import org.springframework.cloud.contract.verifier.messaging.internal.ContractVerifierMessage
import org.springframework.cloud.contract.verifier.messaging.internal.ContractVerifierMessaging
import org.springframework.cloud.contract.verifier.messaging.internal.ContractVerifierObjectMapper
import org.springframework.context.annotation.Bean
import spock.lang.Issue
import spock.lang.Specification

@AutoConfigureMessageVerifier
@SpringBootTest(properties = ["stubrunner.amqp.enabled=true",
		"stubrunner.amqp.mockConnection=false",
		"spring.rabbitmq.port=5672",
		"spring.rabbitmq.username=user",
		"spring.rabbitmq.password=password",
		"spring.rabbitmq.publisher-confirms=true"])
class AmqpMessagingConfirmModeSpec extends Specification {

	// ALL CASES
	private static final String DESTINATION = "input"

	@Inject
	ContractVerifierMessaging contractVerifierMessaging
	@Inject
	ContractVerifierObjectMapper contractVerifierObjectMapper

	@Inject
	TestListener testListener

	@Issue("859")
	def "should send message using non transacted channel into real rabbitmq instance"() {
		given:
			ContractVerifierMessage inputMessage = contractVerifierMessaging.create(
					"{\"name\":\"foo2\"}"
					, headers()
					.header("contentType", "application/json")
			)
		when:
			contractVerifierMessaging.send(inputMessage, DESTINATION)
		then:
			testListener.waitForMessage(10, TimeUnit.SECONDS)
			DocumentContext parsedJson = JsonPath.parse(contractVerifierObjectMapper.writeValueAsString(testListener.messages.first().getBody()))
			assertThatJson(parsedJson).field("['name']").isEqualTo("foo2")
	}


	@TestConfiguration
	static class TestRabbitConfiguration {
		@Bean
		TopicExchange exchange() {
			return new TopicExchange(DESTINATION)
		}

		@Bean
		Queue testQueue() {
			return new AnonymousQueue()
		}

		@Bean
		Binding declareBinding() {
			return BindingBuilder.bind(testQueue())
				.to(exchange()).with("")
		}

		@Bean
		TestListener testListener() {
			return new TestListener()
		}

	}

	static class TestListener {

		CountDownLatch latch = new CountDownLatch(1)

		Collection<Message> messages = Collections.synchronizedList(new ArrayList<>())

		@RabbitListener(queues = "#{testQueue.name}")
		void receive(Message message) {
			messages.add(message)
			latch.countDown()
		}

		void waitForMessage(long timeout, TimeUnit timeUnit) {
			latch.await(timeout, timeUnit)
		}
	}
}
