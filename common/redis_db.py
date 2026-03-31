import os
import redis
import atexit
import logging
from flask import g
import redis.exceptions
from time import perf_counter

logger = logging.getLogger(__name__)


def create_redis_pool(service_name: str) -> redis.ConnectionPool:
    master_name = os.environ["REDIS_HOST"]
    max_connections = int(os.environ.get("REDIS_MAX_CONNECTIONS", "6000"))

    sentinel_hosts = os.environ.get("SENTINEL_HOSTS", "")
    if sentinel_hosts:
        from redis.sentinel import Sentinel

        sentinel_port = int(os.environ.get("SENTINEL_PORT", "26379"))
        addrs = [(h.strip(), sentinel_port) for h in sentinel_hosts.split(",")]
        s = Sentinel(addrs, socket_timeout=0.5, socket_connect_timeout=2)
        pool = s.master_for(
            master_name,
            decode_responses=True,
            socket_keepalive=True,
            socket_connect_timeout=2,
            socket_timeout=5,
            max_connections=max_connections,
        ).connection_pool
        atexit.register(pool.disconnect)
        logger.info(
            "%s: Sentinel pool → master '%s' via %s", service_name, master_name, addrs
        )
        return pool

    host = master_name
    port = int(os.environ.get("REDIS_PORT", 6379))
    pool = redis.ConnectionPool(
        host=host,
        port=port,
        max_connections=max_connections,
        decode_responses=True,
        socket_keepalive=True,
        socket_connect_timeout=2,
        socket_timeout=5,
    )
    atexit.register(pool.disconnect)
    logger.info(
        "%s: Redis pool → %s:%s (direct, no sentinel)", service_name, host, port
    )
    return pool


def get_redis(pool: redis.ConnectionPool) -> redis.Redis:
    return redis.Redis(connection_pool=pool)


def warmup_pool(pool: redis.ConnectionPool, count: int = 20) -> None:
    """Pre-establish `count` connections in the pool by issuing concurrent PINGs.

    On cold start, all pool connections are created lazily on first use.
    When 1000 requests hit simultaneously, hundreds of TCP connections are
    established at once, causing a queuing spike that can push some responses
    past the gateway's 30s timeout. Pre-warming avoids that spike.
    """
    import gevent

    def _ping():
        try:
            redis.Redis(connection_pool=pool).ping()
        except Exception:
            pass

    gevent.joinall([gevent.spawn(_ping) for _ in range(count)])


def setup_flask_lifecycle(app, pool: redis.ConnectionPool, service_name: str):
    @app.before_request
    def _before():
        g.start_time = perf_counter()
        g.redis = get_redis(pool)

    @app.after_request
    def _after(response):
        duration = perf_counter() - g.start_time
        logger.debug("%s: request took %.4fs", service_name, duration)
        return response


def setup_gunicorn_logging(app):
    gunicorn_logger = logging.getLogger("gunicorn.error")
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)


class LuaScripts:
    def __init__(self, r: redis.Redis):
        self._r = r
        self._register_all()

    def _register_all(self):

        self.check_idempotency = self._r.register_script(
            """
            local val = redis.call('HMGET', KEYS[1], 'status_code', 'body')
            if val[1] == false then return nil end
            return val
        """
        )

        self.save_idempotency = self._r.register_script(
            """
            redis.call('HSET',   KEYS[1], 'status_code', ARGV[1], 'body', ARGV[2])
            redis.call('EXPIRE', KEYS[1], 3600)
            return 1
        """
        )

        # returns 0 (already paid), 1 (marked now), nil (not found)
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

        # all-or-nothing: validates all items then deducts; returns array of new stock values
        self.deduct_stock_batch = self._r.register_script(
            """
            local n = #KEYS
            local stocks = {}
            for i = 1, n do
                local stock = tonumber(redis.call('HGET', KEYS[i], 'stock'))
                if stock == nil then
                    return redis.error_reply('NOT_FOUND:' .. KEYS[i])
                end
                if stock < tonumber(ARGV[i]) then
                    return redis.error_reply('INSUFFICIENT:' .. KEYS[i])
                end
                stocks[i] = stock
            end
            local result = {}
            for i = 1, n do
                local new_stock = stocks[i] - tonumber(ARGV[i])
                redis.call('HSET', KEYS[i], 'stock', new_stock)
                result[i] = new_stock
            end
            return result
        """
        )

        # returns array of new stock values (HINCRBY returns the post-increment value)
        self.restore_stock_batch = self._r.register_script(
            """
            local result = {}
            for i = 1, #KEYS do
                result[i] = redis.call('HINCRBY', KEYS[i], 'stock', tonumber(ARGV[i]))
            end
            return result
        """
        )

        # idempotent; reservation key ttl 600s
        self.prepare_stock_batch = self._r.register_script(
            """
            local n = tonumber(ARGV[1])

            -- Idempotency: if already prepared, return success immediately
            if redis.call('EXISTS', KEYS[1]) == 1 then return 0 end

            -- Validate and cache stock values in one pass
            local stocks = {}
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
                stocks[i] = stock
            end

            -- Deduct stock and record reservation using cached values
            for i = 1, n do
                local item_key  = KEYS[i + 1]
                local item_id   = ARGV[1 + i]
                local qty       = tonumber(ARGV[n + 1 + i])
                local new_stock = stocks[i] - qty
                redis.call('HSET', item_key, 'stock', new_stock)
                redis.call('HSET', KEYS[1], item_id, qty)
            end
            redis.call('EXPIRE', KEYS[1], 600)
            return 1
        """
        )

        # idempotent; restores stock from reservation then deletes it
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

        # deduction already applied during prepare; commit just drops the reservation
        self.commit_stock = self._r.register_script(
            """
            redis.call('DEL', KEYS[1])
            return 1
        """
        )

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

        # idempotent; reservation key ttl 600s
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

        # idempotent; restores credit from reservation then deletes it
        self.abort_payment = self._r.register_script(
            """
            local fields = redis.call('HMGET', KEYS[1], 'user_id', 'amount')
            if fields[1] == false then return 0 end
            redis.call('HINCRBY', 'user:' .. fields[1], 'credit', tonumber(fields[2]))
            redis.call('DEL', KEYS[1])
            return 1
        """
        )

        self.commit_payment = self._r.register_script(
            """
            redis.call('DEL', KEYS[1])
            return 1
        """
        )
