package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.util.UUID
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors
import java.util.concurrent.Semaphore
import java.util.concurrent.atomic.AtomicInteger

class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>
) : PaymentExternalSystemAdapter {

    companion object {
        val mapper = ObjectMapper().registerKotlinModule()
        const val MIN_TIMEOUT_MS = 15000L
        const val SAFETY_MARGIN_MS = 1000L
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAvgTime = properties.averageProcessingTime
    private val maxRequestsPerSec = properties.rateLimitPerSec
    private val maxConcurrentRequests = properties.parallelRequests
    private val maxRetryCount = 2

    private val client: HttpClient = HttpClient.newBuilder()
        .version(HttpClient.Version.HTTP_2)
        .connectTimeout(Duration.ofSeconds(5))
        .build()

    private val semaphore = Semaphore(maxConcurrentRequests)
    private val rateLimiter = SimpleRateLimiter(maxRequestsPerSec)

    private val executorService = Executors.newVirtualThreadPerTaskExecutor()

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        val transactionId = UUID.randomUUID()

        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        executorService.submit {
            attemptPayment(paymentId, transactionId, amount, deadline, retryCount = 0)
                .exceptionally { ex ->
                    cancelRequest(paymentId, transactionId, ex.message ?: "Unknown error")
                    null
                }
            Unit
        }
    }

    private fun attemptPayment(
        paymentId: UUID,
        transactionId: UUID,
        amount: Int,
        deadline: Long,
        retryCount: Int
    ): CompletableFuture<java.lang.Void> {
        if (shouldCancelDueToDeadline(deadline)) {
            cancelRequest(paymentId, transactionId, "Estimated completion beyond deadline.")
            rateLimiter.release()
            semaphore.release()
            return CompletableFuture.completedFuture(null)
        }

        if (!rateLimiter.tryAcquire()) {
            cancelRequest(paymentId, transactionId, "Rate limit exceeded.")
            rateLimiter.release()
            semaphore.release()
            return CompletableFuture.completedFuture(null)
        }

        if (!semaphore.tryAcquire()) {
            cancelRequest(paymentId, transactionId, "Too many concurrent requests.")
            rateLimiter.release()
            semaphore.release()
            return CompletableFuture.completedFuture(null)
        }

        val remainingMillis = (deadline - now()) - SAFETY_MARGIN_MS
        val computedTimeout = if (remainingMillis > MIN_TIMEOUT_MS) Duration.ofMillis(remainingMillis) else Duration.ofMillis(MIN_TIMEOUT_MS)

        val url = "http://localhost:1234/external/process?serviceName=$serviceName&accountName=$accountName" +
                "&transactionId=$transactionId&paymentId=$paymentId&amount=$amount&timeout=$computedTimeout"
        val request = HttpRequest.newBuilder()
            .uri(URI.create(url))
            .timeout(computedTimeout)
            .POST(HttpRequest.BodyPublishers.noBody())
            .build()

        return client.sendAsync(request, HttpResponse.BodyHandlers.ofString())
            .thenApply { response ->
                try {
                    mapper.readValue<ExternalSysResponse>(response.body())
                } catch (e: Exception) {
                    ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                }
            }
            .thenCompose { externalResponse ->

                paymentESService.update(paymentId) {
                    it.logProcessing(externalResponse.result, now(), transactionId, reason = externalResponse.message)
                }

                if (externalResponse.result) {
                    CompletableFuture.completedFuture(null)
                } else {
                    if (retryCount < maxRetryCount) {
                        attemptPayment(paymentId, transactionId, amount, deadline, retryCount + 1)
                    } else {
                        CompletableFuture.completedFuture(null)
                    }
                }
            }
            .whenComplete { _, _ ->
                rateLimiter.release()
                semaphore.release()
            }
    }

    private fun shouldCancelDueToDeadline(deadline: Long): Boolean {
        return (now() + requestAvgTime.toMillis() * 1.5) >= deadline
    }

    private fun cancelRequest(paymentId: UUID, transactionId: UUID, reason: String) {
        paymentESService.update(paymentId) {
            it.logProcessing(false, now(), transactionId, reason = reason)
        }
    }

    override fun price() = properties.price
    override fun isEnabled() = properties.enabled
    override fun name() = properties.accountName
}

fun now() = System.currentTimeMillis()

class SimpleRateLimiter(private val maxRequestsPerSecond: Int) {
    private val availableTokens = AtomicInteger(maxRequestsPerSecond)
    @Volatile private var lastRefillTime = now()

    fun tryAcquire(): Boolean {
        refillTokens()
        return availableTokens.getAndDecrement() > 0
    }

    fun release() {
        availableTokens.incrementAndGet()
    }

    private fun refillTokens() {
        val currentTime = now()
        if (currentTime - lastRefillTime >= 1000) {
            availableTokens.set(maxRequestsPerSecond)
            lastRefillTime = currentTime
        }
    }
}
