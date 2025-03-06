package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.Semaphore
import java.util.concurrent.atomic.AtomicInteger

class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAvgTime = properties.averageProcessingTime
    private val maxRequestsPerSec = properties.rateLimitPerSec
    private val maxConcurrentRequests = properties.parallelRequests

    private val client = OkHttpClient.Builder().build()

    private val semaphore = Semaphore(maxConcurrentRequests)
    private val rateLimiter = SimpleRateLimiter(maxRequestsPerSec)

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Initiating payment process for $paymentId")

        val transactionId = UUID.randomUUID()
        logger.info("[$accountName] Starting transaction $transactionId for payment $paymentId")

        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        if (!semaphore.tryAcquire()) {
            cancelRequest(paymentId, transactionId, "Too many concurrent requests.")
            return
        }

        try {
            if (shouldCancelDueToDeadline(deadline)) {
                cancelRequest(paymentId, transactionId, "Estimated completion beyond deadline.")
                semaphore.release()
                return
            }

            if (!rateLimiter.tryAcquire()) {
                cancelRequest(paymentId, transactionId, "Rate limit exceeded.")
                semaphore.release()
                return
            }

            if (shouldCancelDueToDeadline(deadline)) {
                cancelRequest(paymentId, transactionId, "Estimated completion beyond deadline after acquiring rate limit.")
                rateLimiter.release()
                semaphore.release()
                return
            }

            val request = Request.Builder().url(
                "http://localhost:1234/external/process?serviceName=$serviceName&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount"
            ).post(emptyBody).build()

            client.newCall(request).execute().use { response ->
                val body = try {
                    mapper.readValue(response.body?.string() ?: "")
                } catch (e: Exception) {
                    logger.error("[$accountName] [ERROR] Processing txId: $transactionId, payment: $paymentId failed, HTTP code: ${response.code}")
                    ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                }

                logger.warn("[$accountName] Completed txId: $transactionId, payment: $paymentId, success: ${body.result}, message: ${body.message}")

                paymentESService.update(paymentId) {
                    it.logProcessing(body.result, now(), transactionId, reason = body.message)
                }
            }
        } catch (e: SocketTimeoutException) {
            logger.error("[$accountName] Timeout for txId: $transactionId, payment: $paymentId", e)
            cancelRequest(paymentId, transactionId, "Request timeout.")
        } catch (e: Exception) {
            logger.error("[$accountName] Unexpected error for txId: $transactionId, payment: $paymentId", e)
            cancelRequest(paymentId, transactionId, e.message ?: "Unknown error.")
        } finally {
            rateLimiter.release()
            semaphore.release()
        }
    }

    private fun shouldCancelDueToDeadline(deadline: Long): Boolean {
        return (now() + requestAvgTime.toMillis() * 1.5) >= deadline
    }

    private fun cancelRequest(paymentId: UUID, transactionId: UUID, reason: String) {
        logger.error("[$accountName] Payment $paymentId aborted (txId: $transactionId) - $reason")
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
