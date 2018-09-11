package io.ktor.experimental.client.redis

import io.ktor.experimental.client.redis.protocol.*
import io.ktor.experimental.client.redis.utils.*
import io.ktor.network.selector.*
import io.ktor.network.sockets.*
import io.ktor.network.sockets.Socket
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.*
import kotlinx.coroutines.experimental.io.* // This is fine!
import java.io.*
import java.lang.RuntimeException
import java.net.*
import java.nio.charset.*
import kotlin.coroutines.*

/**
 * A Redis basic interface exposing emiting commands receiving their responses.
 *
 * Specific commands are exposed as extension methods.
 */
interface Redis : Closeable {
    companion object {
        val DEFAULT_PORT = 6379
    }

    /**
     * Use [context] to await client close or terminate
     */
    val context: Job

    /**
     * Chatset that
     */
    val charset: Charset get() = Charsets.UTF_8

    /**
     * Executes a raw command. Each [args] will be sent as a String.
     *
     * It returns a type depending on the command.
     * The returned value can be of type [String], [Long] or [List].
     *
     * It may throw a [RedisResponseException]
     */
    suspend fun execute(vararg args: Any?): Any?

    /**
     * Gets the channel used for receiving messages.
     */
    fun getMessageChannel(): ReceiveChannel<Any?> = Channel<Any?>(0).apply { close() }
}

enum class RedisClientReplyMode { ON, OFF, SKIP }

class RedisInvalidAuthException(message: String) : RuntimeException(message)

/**
 * TODO
 * 1. add pipeline timeouts
 */

//class RedisClusteredClient(
//    private val addresses: List<SocketAddress> = listOf(InetSocketAddress("127.0.0.1", 6379)),
//    val maxConnections: Int = 50,
//    private val password: String? = null,
//    private val charset: Charset = Charsets.UTF_8,
//    private val dispatcher: CoroutineDispatcher = DefaultDispatcher
//) {
//    val pooledClients = arrayListOf<Redis>()
//
//    private fun allocateClient(): Redis
//
//    private fun freeClient(client: Redis)
//
//    suspend fun session(callback: suspend Redis.() -> Unit) {
//        val client = allocateClient()
//        try {
//            callback(client)
//        } finally {
//            freeClient(client)
//        }
//    }
//}

typealias RedisClient = SingleRedisClient

class SingleRedisClient(
    private val address: SocketAddress = InetSocketAddress("127.0.0.1", 6379),
    private val password: String? = null,
    override val charset: Charset = Charsets.UTF_8,
    private val dispatcher: CoroutineDispatcher = DefaultDispatcher
) : Redis {
    private val selectorManager = ActorSelectorManager(dispatcher)
    private var _connection: RedisConnection? = null
    override val context: Job = Job()
    private var replyMode = RedisClientReplyMode.ON

    override suspend fun execute(vararg args: Any?): Any? {
        checkSpecialCommands(*args)
        val connection = getConnection()
        return when (replyMode) {
            RedisClientReplyMode.ON -> {
                println("MODE_ON")
                connection.writePacketAndWait(*args)
                connection.readPacket(args)
            }
            RedisClientReplyMode.SKIP -> {
                println("MODE_SKIP")
                connection.writePacketAsap(*args)
                connection.skipPacket(args)
                null
            }
            RedisClientReplyMode.OFF -> {
                println("MODE_OFF")
                connection.writePacketAsap(*args)
                null
            }
        }
    }

    override fun getMessageChannel(): ReceiveChannel<Any?> {
        replyMode = RedisClientReplyMode.OFF
        return produce {
            val connection = getConnection()
            while (true) {
                channel.send(connection.readPacket())
            }
        }
    }

    private suspend fun getConnection(): RedisConnection {
        if (_connection?.isAlive == false) {
            _connection = null
        }

        if (_connection == null) {
            val socket = aSocket(selectorManager).tcpNoDelay().tcp().connect(address)
            _connection = RedisConnection(context, socket, socket.openReadChannel(), socket.openWriteChannel(), charset)
            if (password != null) {
                _connection?.writePacketAndWait("AUTH", password)
                val result = _connection?.readPacket()
                if (result != "OK") {
                    throw RedisInvalidAuthException(result?.toString() ?: "error")
                }
            }
        }
        return _connection!!
    }

    private fun checkSpecialCommands(vararg args: Any?) {
        if (args.getOrNull(0)?.toString()?.equals("client", ignoreCase = true) == true) {
            if (args.getOrNull(1)?.toString()?.equals("reply", ignoreCase = true) == true) {
                val mode = args.getOrNull(2)?.toString()?.toLowerCase() ?: "on"
                replyMode = when (mode) {
                    "on" -> RedisClientReplyMode.ON
                    "off" -> RedisClientReplyMode.OFF
                    "skip" -> RedisClientReplyMode.SKIP
                    else -> RedisClientReplyMode.ON
                }
            }
        }
    }

    override fun close() {
        _connection?.close()
        _connection = null
    }
}

private class RedisConnection(
    context: CoroutineContext,
    val socket: Socket,
    val input: ByteReadChannel,
    val output: ByteWriteChannel,
    val charset: Charset
) {
    val isAlive get() = !socket.isClosed
    val decoder = charset.newDecoder()
    /*
    val readQueue = AsyncQueue(context)
    val writeQueue = AsyncQueue(context)

    suspend fun writePacketAndWait(vararg data: Any?) {
        writeQueue.sync {
            output.writePacket { writeRedisValue(data, charset = charset) }
            output.flush()
        }
    }

    suspend fun readPacket(): Any? = readQueue.sync {
        input.readRedisMessage(decoder)
    }

    fun writePacketAsap(vararg data: Any?) {
        writeQueue {
            output.writePacket { writeRedisValue(data, charset = charset) }
            output.flush()
        }
    }

    fun skipPacket(): Any? {
        readQueue {
            input.readRedisMessage(decoder)
        }
        return null
    }

    fun close() {
        socket.close()
    }
    */

    suspend fun writePacketAndWait(vararg data: Any?) {
        println("writePacketAndWait: ${data.toList()}")
        output.writePacket { writeRedisValue(data, charset = charset) }
        output.flush()
    }

    suspend fun readPacket(args: Array<out Any?>? = null): Any? {
        println("START readPacket")
        val msg = input.readRedisMessage(decoder, args)
        println("readPacket: $msg")
        return msg
    }

    suspend fun writePacketAsap(vararg data: Any?) {
        println("writePacketAsap: ${data.toList()}")
        output.writePacket { writeRedisValue(data, charset = charset) }
        output.flush()
    }

    suspend fun skipPacket(args: Array<out Any?>? = null): Any? {
        println("START skipPacket")
        val msg = input.readRedisMessage(decoder, args)
        println("skipPacket: $msg")
        return null
    }

    fun close() {
        socket.close()
    }
}
