package ru.xask.ordermicroservice.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.annotation.Transactional;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import ru.xask.ordermicroservice.dto.OrderDto;
import ru.xask.ordermicroservice.dto.OrderResponse;
import ru.xask.ordermicroservice.entity.Order;
import ru.xask.ordermicroservice.repository.OrderRepository;
import ru.xask.ordermicroservice.util.TestDataFactory;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@ActiveProfiles("test")
@Testcontainers
public class OrderServiceIntegrationTest {

    @Container
    static KafkaContainer kafka = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.5.0"));

    @Autowired
    private OrderService orderService;

    @Autowired
    private OrderRepository orderRepository;

    @Autowired
    private TestKafkaListener testKafkaListener;

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
    }

    /** Встроенный listener, который слушает топик всё время жизни контекста */
    @TestConfiguration
    static class TestConfig {
        @Bean
        TestKafkaListener testKafkaListener() {
            return new TestKafkaListener();
        }
    }

    public static class TestKafkaListener {
        private final ConcurrentLinkedQueue<String> messages = new ConcurrentLinkedQueue<>();
        private volatile CountDownLatch latch = new CountDownLatch(1);

        @KafkaListener(topics = "order_events", groupId = "test-group")
        public void listen(String message) {
            messages.offer(message);
            CountDownLatch current = latch;
            if (current != null) {
                current.countDown();
            }
        }

        public void reset() {
            messages.clear();
            latch = new CountDownLatch(1);
        }

        public boolean waitForMessage(String expected, Duration timeout) {
            long deadline = System.currentTimeMillis() + timeout.toMillis();
            while (System.currentTimeMillis() < deadline) {
                if (messages.stream().anyMatch(m -> m.contains(expected))) {
                    return true;
                }
                CountDownLatch current = latch;
                if (current != null) {
                    long remaining = deadline - System.currentTimeMillis();
                    if (remaining <= 0) break;
                    try {
                        current.await(remaining, TimeUnit.MILLISECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return false;
                    }
                }
            }
            return messages.stream().anyMatch(m -> m.contains(expected));
        }
    }

    @BeforeEach
    void setUp() {
        testKafkaListener.reset();
    }

    @Test
    void shouldSendOrderCreatedEventToKafka() {
        OrderDto orderDto = TestDataFactory.createDefaultOrderDto();

        OrderResponse response = orderService.createOrder(orderDto);
        Long orderId = response.id();

        boolean messageFound = testKafkaListener.waitForMessage(
                "OrderCreated:" + orderId,
                Duration.ofSeconds(5)
        );

        assertThat(messageFound)
                .withFailMessage("Не найдено сообщение OrderCreated:%d в топике order_events", orderId)
                .isTrue();
    }

    @Test
    @Transactional
    void shouldCreateOrderAndSendOrderCreatedEventToKafka() {
        OrderDto orderDto = TestDataFactory.createDefaultOrderDto();

        OrderResponse response = orderService.createOrder(orderDto);
        Long orderId = response.id();

        assertThat(response.id()).isNotNull();
        assertThat(response.customerName()).isEqualTo(orderDto.customerName());
        assertThat(response.status()).isEqualTo(orderDto.status());
        assertThat(response.items()).hasSize(1);
        assertThat(response.items().get(0).productName()).isEqualTo("Laptop");

        Optional<Order> savedOrderOpt = orderRepository.findById(orderId);
        assertThat(savedOrderOpt).isPresent();
        Order savedOrder = savedOrderOpt.get();
        assertThat(savedOrder.getCustomerName()).isEqualTo(orderDto.customerName());
        assertThat(savedOrder.getStatus()).isEqualTo(orderDto.status());
        assertThat(savedOrder.getItems()).hasSize(1);
        assertThat(savedOrder.getItems().get(0).getProductName()).isEqualTo("Laptop");

        boolean messageFound = testKafkaListener.waitForMessage(
                "OrderCreated:" + orderId,
                Duration.ofSeconds(5)
        );
        assertThat(messageFound)
                .withFailMessage("Сообщение OrderCreated:%d не найдено в топике order_events", orderId)
                .isTrue();
    }
    //коммент для гита чтоб закомитить
}