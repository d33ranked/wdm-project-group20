# each service creates one connection pool on startup – cheap as these are just sockets
# each request borrows a socket from the pool using g.redis object
# when the request ends the socket is returned back to the pool automatically

# there is no BEGIN / COMMIT because redis commands are atomic in nature
# single command execution is atomic
# multi-command execution is queued and executed sequentially (not atomic)
# read-then-write operations require lua scripts to be atomic as all of them are executed in an uninterrupted unit

import os
import redis
import atexit
import logging
from flask import g
import redis.exceptions
from time import perf_counter

logger = logging.getLogger(__name__)


# create a connection pool
def create_redis_pool(service_name: str) -> redis.ConnectionPool:
    # max_connections=100 : each greenlet counts as one, so 100 is enough for a single-worker process
    # decode_responses=True : automatically decode byte responses to strings for the client
    # socket_keepalive=True : keeps idle sockets alive

    host = os.environ["REDIS_HOST"]
    port = int(os.environ.get("REDIS_PORT", 6379))

    pool = redis.ConnectionPool(
        host=host,
        port=port,
        max_connections=100,
        decode_responses=True,
        socket_keepalive=True,
        socket_connect_timeout=2,
        socket_timeout=5,
    )

    # clean up the connection pool when service shuts down
    atexit.register(pool.disconnect)
    logger.info("%s: Redis pool created → %s:%s", service_name, host, port)
    return pool


# create a client to borrow a socket from the pool
def get_redis(pool: redis.ConnectionPool) -> redis.Redis:
    # called once per request in before_request
    # a wrapper that borrows a socket from the pool on demand and returns it
    return redis.Redis(connection_pool=pool)


# flask lifecycle methods
def setup_flask_lifecycle(app, pool: redis.ConnectionPool, service_name: str):
    # wire up g.redis for every request
    # redis transactions are automatically committed

    @app.before_request
    def _before():
        g.start_time = perf_counter()
        g.redis = get_redis(pool)

    @app.after_request
    def _after(response):
        duration = perf_counter() - g.start_time
        logger.debug("%s: request took %.4fs", service_name, duration)
        return response


# green unicorn logging settings
def setup_gunicorn_logging(app):
    # route through green unicorn to make them appear in docker logs
    gunicorn_logger = logging.getLogger("gunicorn.error")
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)


# redis is single-threaded and runs one command at a time
# it cannot handle logical transactions like read-then-write operations
# we use lua scripts so no two commands from different requests can interleave
# each script is executed as a single uninterrupted unit
# we create the scripts and register them once at module import time
# redis-py sends them to redis and it computes SHA1 to cache the script body
# future calls use just the hash so there is no network overhead for the script body


class LuaScripts:
    # container for all scripts

    def __init__(self, r: redis.Redis):
        self._r = r
        self._register_all()

    def _register_all(self):

        # idempotency check-and-mark
        # exists : return [status_code, body]
        # not exists : return nil
        self.check_idempotency = self._r.register_script(
            """
            local val = redis.call('HMGET', KEYS[1], 'status_code', 'body')
            if val[1] == false then return nil end
            return val
        """
        )

        # save a completed operation
        # save the result of a completed operation to prevent duplicate executions
        # KEYS[1] = "idem:{key}"
        # ARGV[1] = status_code (string)
        # ARGV[2] = body (string)
        # TT (expiration time) = 1 hour
        self.save_idempotency = self._r.register_script(
            """
            redis.call('HSET',   KEYS[1], 'status_code', ARGV[1], 'body', ARGV[2])
            redis.call('EXPIRE', KEYS[1], 3600)
            return 1
        """
        )

        # mark an order as paid
        # return 0 if already paid (200)
        # return 1 if marked paid now (200)
        # return nil if order does not exist (404)
        # KEYS[1] = "order:{order_id}"
        self.mark_order_paid = self._r.register_script(
            """
            local exists = redis.call('EXISTS', KEYS[1])
            if exists == 0 then return nil end
            local paid = redis.call('HGET', KEYS[1], 'paid')
            if paid == 'true' then return 0 end
            redis.call('HSET', KEYS[1], 'paid', 'true')
            return 1
        """
        )

        # deduct stock batch (all-or-nothing)
        # checks every item has sufficient stock before deductions
        # return an error if any of the items are not found or have insufficient stock
        # KEYS = ["item:{id1}", "item:{id2}", ...]
        # ARGV = [qty1, qty2, ...]
        self.deduct_stock_batch = self._r.register_script(
            """
            local n = #KEYS
            for i = 1, n do
                local stock = tonumber(redis.call('HGET', KEYS[i], 'stock'))
                if stock == nil then
                    return redis.error_reply('NOT_FOUND:' .. KEYS[i])
                end
                if stock < tonumber(ARGV[i]) then
                    return redis.error_reply('INSUFFICIENT:' .. KEYS[i])
                end
            end
            for i = 1, n do
                local new_stock = tonumber(redis.call('HGET', KEYS[i], 'stock')) - tonumber(ARGV[i])
                redis.call('HSET', KEYS[i], 'stock', new_stock)
            end
            return 1
        """
        )

        # restore stock batch
        # adds stock back to each item
        # always succeeds as restoring can never fail
        # KEYS = ["item:{id1}", "item:{id2}", ...]
        # ARGV = [qty1, qty2, ...]
        self.restore_stock_batch = self._r.register_script(
            """
            for i = 1, #KEYS do
                redis.call('HINCRBY', KEYS[i], 'stock', tonumber(ARGV[i]))
            end
            return 1
        """
        )

        # tpc prepare stock batch
        # idempotency check – stock check – deduct – record reservation
        # reservation key TTL : 10mins and auto-aborted if coordinator never sends commit/abort (e.g. coordinator crash)
        # KEYS[1] = "prepared:stock:{txn_id}" (reservation record)
        # KEYS[2..n+1] = "item:{id}" for each item
        # ARGV[1] = number of items (n)
        # ARGV[2..n+1] = item_id strings (for recording in the hash)
        # ARGV[n+2..2n+1] = quantities
        self.prepare_stock_batch = self._r.register_script(
            """
            local n = tonumber(ARGV[1])

            -- Idempotency: if already prepared, return success immediately
            if redis.call('EXISTS', KEYS[1]) == 1 then return 0 end

            -- Check all items have sufficient stock
            for i = 1, n do
                local item_key = KEYS[i + 1]
                local qty = tonumber(ARGV[n + 1 + i])
                local stock = tonumber(redis.call('HGET', item_key, 'stock'))
                if stock == nil then
                    return redis.error_reply('NOT_FOUND:' .. item_key)
                end
                if stock < qty then
                    return redis.error_reply('INSUFFICIENT:' .. item_key)
                end
            end

            -- Deduct stock and record reservation
            for i = 1, n do
                local item_key  = KEYS[i + 1]
                local item_id   = ARGV[1 + i]
                local qty       = tonumber(ARGV[n + 1 + i])
                local new_stock = tonumber(redis.call('HGET', item_key, 'stock')) - qty
                redis.call('HSET', item_key, 'stock', new_stock)
                redis.call('HSET', KEYS[1], item_id, qty)
            end
            redis.call('EXPIRE', KEYS[1], 600)
            return 1
        """
        )

        # tpc abort stock
        # reads reservation record, restores stock for each item, then deletes the reservation
        # safe to call multiple times (idempotent)
        # KEYS[1] = "prepared:stock:{txn_id}"
        self.abort_stock = self._r.register_script(
            """
            local fields = redis.call('HGETALL', KEYS[1])
            if #fields == 0 then return 0 end
            for i = 1, #fields, 2 do
                local item_key = 'item:' .. fields[i]
                local qty      = tonumber(fields[i + 1])
                redis.call('HINCRBY', item_key, 'stock', qty)
            end
            redis.call('DEL', KEYS[1])
            return 1
        """
        )

        # tpc commit stock
        # deduction was already applied during prepare - commit just deletes the reservation
        # KEYS[1] = "prepared:stock:{txn_id}"
        self.commit_stock = self._r.register_script(
            """
            redis.call('DEL', KEYS[1])
            return 1
        """
        )

        # deduct credit (atomic check-then-deduct)
        # returns new credit balance on success
        # returns error string on insufficient funds or user not found
        # KEYS[1] = "user:{user_id}"
        # ARGV[1] = amount to deduct
        self.deduct_credit = self._r.register_script(
            """
            local credit = tonumber(redis.call('HGET', KEYS[1], 'credit'))
            if credit == nil then
                return redis.error_reply('NOT_FOUND')
            end
            if credit < tonumber(ARGV[1]) then
                return redis.error_reply('INSUFFICIENT_CREDIT')
            end
            local new_credit = credit - tonumber(ARGV[1])
            redis.call('HSET', KEYS[1], 'credit', new_credit)
            return new_credit
        """
        )

        # tpc prepare payment
        # idempotency check – credit check – deduct – record reservation
        # reservation key TTL : 10mins and auto-aborted if coordinator never sends commit/abort (e.g. coordinator crash)
        # KEYS[1] = "prepared:payment:{txn_id}" (reservation record)
        # KEYS[2] = "user:{user_id}"
        # ARGV[1] = amount to deduct
        # ARGV[2] = user_id (stored in reservation for abort recovery)
        self.prepare_payment = self._r.register_script(
            """
            -- Idempotency: already prepared
            if redis.call('EXISTS', KEYS[1]) == 1 then return 0 end

            local credit = tonumber(redis.call('HGET', KEYS[2], 'credit'))
            if credit == nil then
                return redis.error_reply('NOT_FOUND')
            end
            if credit < tonumber(ARGV[1]) then
                return redis.error_reply('INSUFFICIENT_CREDIT')
            end

            local new_credit = credit - tonumber(ARGV[1])
            redis.call('HSET', KEYS[2], 'credit', new_credit)
            redis.call('HMSET', KEYS[1], 'user_id', ARGV[2], 'amount', ARGV[1])
            redis.call('EXPIRE', KEYS[1], 600)
            return new_credit
        """
        )

        # tpc abort payment
        # reads reservation record, restores credit, then deletes the reservation
        # safe to call multiple times (idempotent)
        # KEYS[1] = "prepared:payment:{txn_id}"
        self.abort_payment = self._r.register_script(
            """
            local fields = redis.call('HMGET', KEYS[1], 'user_id', 'amount')
            if fields[1] == false then return 0 end
            redis.call('HINCRBY', 'user:' .. fields[1], 'credit', tonumber(fields[2]))
            redis.call('DEL', KEYS[1])
            return 1
        """
        )

        # tpc commit payment
        # deduction was already applied during prepare - commit just deletes the reservation
        # KEYS[1] = "prepared:payment:{txn_id}"
        self.commit_payment = self._r.register_script(
            """
            redis.call('DEL', KEYS[1])
            return 1
        """
        )