package io.ktor.experimental.client.redis.utils

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.*

/**
 * Patternized the channel receiving a request and generating a result in queue.
 * The main problem here is that the actual execution is follow when reading the code since happens in the service.
 * Also you have to close the channel.
 */
class QueueService<I, O>(
    handler: I.() -> O
) {
    class Request<I, O>(val input: I, val complete: CompletableDeferred<O>)

    val channel = actor<Request<I, O>> {
        channel.consumeEach { request ->
            val result = handler(request.input)
            request.complete.complete(result)
        }
    }

    suspend fun request(input: I): O {
        val deferred = CompletableDeferred<O>()
        channel.send(Request(input, deferred))
        return deferred.await()
    }

    fun close() {
        channel.close()
    }
}
