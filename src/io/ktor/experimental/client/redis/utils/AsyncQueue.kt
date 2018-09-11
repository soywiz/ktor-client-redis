package io.ktor.experimental.client.redis.utils

import kotlinx.coroutines.*
import kotlin.coroutines.*

class AsyncQueue(val context: CoroutineContext) {
    private var lastDeferred: Deferred<*> = CompletableDeferred(Unit)

    operator fun invoke(callback: suspend () -> Unit) {
        synchronized(this) {
            val oldDeferred = lastDeferred
            lastDeferred = async(context) {
                oldDeferred.await()
                callback()
            }
        }
    }

    suspend fun <T> sync(callback: suspend () -> T): T {
        lateinit var newDeferred: Deferred<T>
        synchronized(this) {
            val oldDeferred = lastDeferred
            newDeferred = async(context) {
                oldDeferred.await()
                callback()
            }
            lastDeferred = newDeferred
        }
        return newDeferred.await()
    }
}