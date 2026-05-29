import { PrismaClient } from "@prisma/client";
import { Pool, PoolClient } from "pg";
import { PrismaPg } from "@prisma/adapter-pg";
import dotenv from "dotenv";
import { trace } from "./tracing";

dotenv.config();

const connectionString = process.env.DATABASE_URL;

// ---------------------------------------------------------------------------
// Pool configuration — tuneable via environment variables
// ---------------------------------------------------------------------------
function positiveIntEnv(name: string, fallback: number): number {
  const raw = process.env[name];
  const parsed = raw === undefined ? fallback : Number.parseInt(raw, 10);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    console.warn(`[POOL] Invalid ${name}=${raw}; using ${fallback}`);
    return fallback;
  }
  return parsed;
}

const POOL_MAX = positiveIntEnv("POOL_MAX_CONNECTIONS", 20);
const POOL_MIN = positiveIntEnv("POOL_MIN_CONNECTIONS", 2);
const POOL_IDLE_TIMEOUT_MS = positiveIntEnv("POOL_IDLE_TIMEOUT_MS", 30000);
const POOL_CONNECTION_TIMEOUT_MS = positiveIntEnv("POOL_CONNECTION_TIMEOUT_MS", 5000);
const POOL_HEALTH_CHECK_INTERVAL_MS = positiveIntEnv("POOL_HEALTH_CHECK_INTERVAL_MS", 30000);
const POOL_CONNECT_RETRY_LIMIT = positiveIntEnv("POOL_CONNECT_RETRY_LIMIT", 3);
const POOL_CONNECT_RETRY_BASE_DELAY_MS = positiveIntEnv("POOL_CONNECT_RETRY_BASE_DELAY_MS", 500);
const POOL_MAX_USES = positiveIntEnv("POOL_MAX_USES", 7500);
const POOL_MAX_LIFETIME_SECONDS = positiveIntEnv("POOL_MAX_LIFETIME_SECONDS", 1800);
const POOL_STATEMENT_TIMEOUT_MS = positiveIntEnv("POOL_STATEMENT_TIMEOUT_MS", 5000);
const POOL_LOCK_TIMEOUT_MS = positiveIntEnv("POOL_LOCK_TIMEOUT_MS", 1000);
const POOL_IDLE_IN_TX_TIMEOUT_MS = positiveIntEnv("POOL_IDLE_IN_TX_TIMEOUT_MS", 5000);

// ---------------------------------------------------------------------------
// Build the pool with resilient options
// ---------------------------------------------------------------------------
export const pool = new Pool({
  connectionString,
  max: POOL_MAX,
  min: POOL_MIN,
  idleTimeoutMillis: POOL_IDLE_TIMEOUT_MS,
  connectionTimeoutMillis: POOL_CONNECTION_TIMEOUT_MS,
  maxUses: POOL_MAX_USES,
  maxLifetimeSeconds: POOL_MAX_LIFETIME_SECONDS,
  statement_timeout: POOL_STATEMENT_TIMEOUT_MS,
  query_timeout: POOL_STATEMENT_TIMEOUT_MS + 500,
  lock_timeout: POOL_LOCK_TIMEOUT_MS,
  idle_in_transaction_session_timeout: POOL_IDLE_IN_TX_TIMEOUT_MS,
  application_name: process.env.PGAPPNAME || "lance-backend-api",
  keepAlive: true,
  keepAliveInitialDelayMillis: 10_000,
  allowExitOnIdle: false, // Keep the pool alive even when the event loop has no other work
});

// ---------------------------------------------------------------------------
// Pool event listeners — structured logging for diagnostics
// ---------------------------------------------------------------------------
pool.on("connect", (client: PoolClient) => {
  client
    .query(`
      SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL READ COMMITTED;
      SET statement_timeout = ${POOL_STATEMENT_TIMEOUT_MS};
      SET lock_timeout = ${POOL_LOCK_TIMEOUT_MS};
      SET idle_in_transaction_session_timeout = ${POOL_IDLE_IN_TX_TIMEOUT_MS};
    `)
    .catch((err) => {
      console.error("[POOL] Failed to configure session safety settings:", err.message);
    });

  if (process.env.NODE_ENV !== "production") {
    console.log(
      `[POOL] New client connected | total=${pool.totalCount} idle=${pool.idleCount} waiting=${pool.waitingCount}`
    );
  }
});

pool.on("acquire", () => {
  if (process.env.NODE_ENV !== "production") {
    console.log(
      `[POOL] Client acquired | total=${pool.totalCount} idle=${pool.idleCount} waiting=${pool.waitingCount}`
    );
  }
});

pool.on("remove", () => {
  if (process.env.NODE_ENV !== "production") {
    console.log(
      `[POOL] Client removed | total=${pool.totalCount} idle=${pool.idleCount} waiting=${pool.waitingCount}`
    );
  }
});

pool.on("error", (err: Error) => {
  console.error("[POOL] Unexpected pool error:", err.message);
});

// ---------------------------------------------------------------------------
// Pool health statistics — exposed for the /api/v1/pool/health endpoint
// ---------------------------------------------------------------------------
export interface PoolHealthStats {
  totalConnections: number;
  idleConnections: number;
  activeConnections: number;
  waitingRequests: number;
  maxConnections: number;
  minConnections: number;
  idleTimeoutMs: number;
  connectionTimeoutMs: number;
  statementTimeoutMs: number;
  lockTimeoutMs: number;
  idleInTransactionTimeoutMs: number;
  maxUses: number;
  maxLifetimeSeconds: number;
  healthCheckIntervalMs: number;
  lastHealthCheckAt: string | null;
  lastHealthCheckOk: boolean;
  uptimeSeconds: number;
  healthChecksPassed: number;
  healthChecksFailed: number;
}

let lastHealthCheckAt: Date | null = null;
let lastHealthCheckOk = true;
let healthChecksPassed = 0;
let healthChecksFailed = 0;
const poolStartedAt = Date.now();

export function getPoolHealthStats(): PoolHealthStats {
  return {
    totalConnections: pool.totalCount,
    idleConnections: pool.idleCount,
    activeConnections: pool.totalCount - pool.idleCount,
    waitingRequests: pool.waitingCount,
    maxConnections: POOL_MAX,
    minConnections: POOL_MIN,
    idleTimeoutMs: POOL_IDLE_TIMEOUT_MS,
    connectionTimeoutMs: POOL_CONNECTION_TIMEOUT_MS,
    statementTimeoutMs: POOL_STATEMENT_TIMEOUT_MS,
    lockTimeoutMs: POOL_LOCK_TIMEOUT_MS,
    idleInTransactionTimeoutMs: POOL_IDLE_IN_TX_TIMEOUT_MS,
    maxUses: POOL_MAX_USES,
    maxLifetimeSeconds: POOL_MAX_LIFETIME_SECONDS,
    healthCheckIntervalMs: POOL_HEALTH_CHECK_INTERVAL_MS,
    lastHealthCheckAt: lastHealthCheckAt ? lastHealthCheckAt.toISOString() : null,
    lastHealthCheckOk,
    uptimeSeconds: Math.floor((Date.now() - poolStartedAt) / 1000),
    healthChecksPassed,
    healthChecksFailed,
  };
}

// ---------------------------------------------------------------------------
// Background health-check — validates an idle connection periodically
// ---------------------------------------------------------------------------
let healthCheckTimer: ReturnType<typeof setInterval> | null = null;

async function runPoolHealthCheck(): Promise<void> {
  let client: PoolClient | undefined;
  try {
    client = await pool.connect();
    await client.query("SELECT 1");
    lastHealthCheckOk = true;
    healthChecksPassed++;
  } catch (err: any) {
    lastHealthCheckOk = false;
    healthChecksFailed++;
    console.error("[POOL HEALTH] Background health-check failed:", err.message);
  } finally {
    lastHealthCheckAt = new Date();
    if (client) {
      client.release();
    }
  }
}

export function startPoolHealthCheck(): void {
  if (healthCheckTimer) return; // already running
  // Run once immediately then on an interval
  runPoolHealthCheck();
  healthCheckTimer = setInterval(runPoolHealthCheck, POOL_HEALTH_CHECK_INTERVAL_MS);
  // Allow the process to exit even if the timer is still active
  if (healthCheckTimer && typeof healthCheckTimer === "object" && "unref" in healthCheckTimer) {
    healthCheckTimer.unref();
  }
  console.log(
    `[POOL HEALTH] Background health-check started (interval: ${POOL_HEALTH_CHECK_INTERVAL_MS}ms)`
  );
}

export function stopPoolHealthCheck(): void {
  if (healthCheckTimer) {
    clearInterval(healthCheckTimer);
    healthCheckTimer = null;
  }
}

// ---------------------------------------------------------------------------
// connectWithRetry — Wraps initial connection with exponential backoff so the
// API doesn't crash on cold-start if the database is momentarily unavailable.
// ---------------------------------------------------------------------------
export async function connectWithRetry(): Promise<void> {
  for (let attempt = 1; attempt <= POOL_CONNECT_RETRY_LIMIT; attempt++) {
    try {
      const client = await pool.connect();
      await client.query("SELECT 1");
      client.release();
      console.log(`[POOL] Database connected successfully on attempt ${attempt}`);
      return;
    } catch (err: any) {
      const delay = Math.min(
        POOL_CONNECT_RETRY_BASE_DELAY_MS * Math.pow(2, attempt - 1) + Math.random() * 100,
        5000
      );
      console.error(
        `[POOL] Connection attempt ${attempt}/${POOL_CONNECT_RETRY_LIMIT} failed: ${err.message}. ` +
          `Retrying in ${delay.toFixed(0)}ms...`
      );
      if (attempt === POOL_CONNECT_RETRY_LIMIT) {
        throw new Error(
          `Failed to connect to the database after ${POOL_CONNECT_RETRY_LIMIT} attempts: ${err.message}`
        );
      }
      await new Promise((resolve) => setTimeout(resolve, delay));
    }
  }
}

// ---------------------------------------------------------------------------
// Prisma Client with the pg pool adapter
// ---------------------------------------------------------------------------
const adapter = new PrismaPg(pool);

const globalForPrisma = global as unknown as { prisma: ReturnType<typeof createPrismaClient> };

function createPrismaClient() {
  const client = new PrismaClient({
    adapter,
    log: process.env.NODE_ENV === "development" ? ["query", "error", "warn"] : ["error"],
  });

  return client.$extends({
    query: {
      $allModels: {
        async $allOperations({ model, operation, args, query }) {
          const startTime = Date.now();
          const logger = trace.getLogger("db-query");

          try {
            const result = await query(args);
            const duration = Date.now() - startTime;

            if (duration > 1000) {
              logger.warn(`Slow query detected: ${model}.${operation}`, {
                duration,
                model,
                action: operation,
                args: JSON.stringify(args).substring(0, 200),
              });
            }

            logger.debug(`Query completed: ${model}.${operation}`, {
              duration,
              model,
              action: operation,
            });

            return result;
          } catch (error) {
            const duration = Date.now() - startTime;
            logger.error(`Query failed: ${model}.${operation}`, {
              duration,
              model,
              action: operation,
              error: error instanceof Error ? error.message : String(error),
            });
            throw error;
          }
        },
      },
    },
  });
}

export const prisma = globalForPrisma.prisma || createPrismaClient();

if (process.env.NODE_ENV !== "production") globalForPrisma.prisma = prisma;

// ---------------------------------------------------------------------------
// Graceful shutdown — release pool connections on process exit signals
// ---------------------------------------------------------------------------
async function gracefulShutdown(signal: string): Promise<void> {
  console.log(`[POOL] Received ${signal}. Draining connection pool...`);
  stopPoolHealthCheck();
  try {
    await prisma.$disconnect();
    await pool.end();
    console.log("[POOL] Connection pool drained successfully.");
  } catch (err: any) {
    console.error("[POOL] Error during pool shutdown:", err.message);
  }
  process.exit(0);
}

process.on("SIGTERM", () => gracefulShutdown("SIGTERM"));
process.on("SIGINT", () => gracefulShutdown("SIGINT"));
