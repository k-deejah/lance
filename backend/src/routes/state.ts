import { Router, Request, Response } from "express";
import { z } from "zod";
import { pool } from "../config/db";
import { buildJobSearchQuery, summarizePlan } from "../utils/jobSearchPlan";
import { logger } from "../utils/tracing";

const router = Router();

function positiveTimeoutMs(name: string, fallback: number): number {
  const parsed = Number.parseInt(process.env[name] || String(fallback), 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}


const jobSearchPlanQuerySchema = z.object({
  query: z.string().optional(),
  status: z.string().optional(),
  tag: z.string().optional(),
  sort: z.enum(["created_at", "budget"]).default("created_at"),
  limit: z.coerce.number().int().min(1).max(100).default(25),
  cursor_created_at: z.coerce.date().optional(),
  cursor_id: z.string().uuid().optional(),
  min_budget: z.coerce.number().int().nonnegative().optional(),
  max_budget: z.coerce.number().int().nonnegative().optional(),
  skills: z.string().optional(),
  deadline_before: z.coerce.date().optional(),
});

const recoveryQuerySchema = z.object({
  status: z.enum(["pending", "committed", "failed", "abandoned"]).optional(),
  limit: z.coerce.number().int().min(1).max(200).default(50),
});

/**
 * GET /api/v1/state/write-recovery
 *
 * Lists durable write-recovery rows for interrupted or retryable database
 * mutations. The query is intentionally bounded and ordered by the indexed
 * status/updated_at tuple from the migration to avoid table scans under load.
 */
router.get("/write-recovery", async (req: Request, res: Response) => {
  try {
    const query = recoveryQuerySchema.parse(req.query);
    const params: Array<string | number> = [query.limit];

    let sql = `
      SELECT id, idempotency_key, operation, entity_type, entity_id, status,
             attempts, last_error, recovery_payload, created_at, updated_at
      FROM write_recovery_records
    `;

    if (query.status) {
      params.unshift(query.status);
      sql += " WHERE status = $1 ORDER BY updated_at DESC, id DESC LIMIT $2";
    } else {
      sql += " ORDER BY updated_at DESC, id DESC LIMIT $1";
    }

    const result = await pool.query(sql, params);

    logger.info("Write recovery state queried", {
      status: query.status || "any",
      limit: query.limit,
      returned: result.rowCount,
    });

    res.status(200).json(result.rows);
  } catch (error: any) {
    if (error instanceof z.ZodError) {
      return res.status(400).json({ error: error.issues });
    }

    logger.error("Write recovery state query failed", { error: error.message });
    res.status(500).json({ error: "Failed to retrieve write recovery state" });
  }
});

/**
 * GET /api/v1/state/job-search-plan
 *
 * Audits the planner cost for the same bounded SQL used by GET /api/v1/jobs.
 * The endpoint runs EXPLAIN without ANALYZE inside a read-only transaction so
 * diagnostics cannot mutate state or hold write locks during production checks.
 */
router.get("/job-search-plan", async (req: Request, res: Response) => {
  const startedAt = Date.now();
  const client = await pool.connect();

  try {
    const query = jobSearchPlanQuerySchema.parse(req.query);

    if ((query.cursor_created_at && !query.cursor_id) || (!query.cursor_created_at && query.cursor_id)) {
      return res.status(400).json({
        error: "cursor_created_at and cursor_id must be provided together",
      });
    }

    if (
      query.min_budget !== undefined &&
      query.max_budget !== undefined &&
      query.min_budget > query.max_budget
    ) {
      return res.status(400).json({ error: "min_budget cannot be greater than max_budget" });
    }

    const builtQuery = buildJobSearchQuery(query);
    await client.query("BEGIN READ ONLY ISOLATION LEVEL READ COMMITTED");
    await client.query(`SET LOCAL statement_timeout = ${positiveTimeoutMs("JOB_SEARCH_PLAN_TIMEOUT_MS", 1000)}`);

    const explain = await client.query(
      `EXPLAIN (FORMAT JSON, COSTS TRUE, VERBOSE FALSE, BUFFERS FALSE) ${builtQuery.sql}`,
      builtQuery.params
    );
    await client.query("COMMIT");

    const planRoot = explain.rows[0]["QUERY PLAN"][0];
    const summary = summarizePlan(planRoot.Plan);

    logger.info("Job search query plan audited", {
      planKey: builtQuery.planKey,
      totalCost: summary.totalCost,
      jobsSequentialScan: summary.jobsSequentialScan,
      durationMs: Date.now() - startedAt,
      poolTotal: pool.totalCount,
      poolIdle: pool.idleCount,
      poolWaiting: pool.waitingCount,
    });

    return res.status(200).json({
      route: "GET /api/v1/jobs",
      plan_key: builtQuery.planKey,
      search_term: builtQuery.normalizedSearchTerm || null,
      skills: builtQuery.normalizedSkills,
      summary,
      planner: planRoot,
      pool: {
        totalConnections: pool.totalCount,
        idleConnections: pool.idleCount,
        waitingRequests: pool.waitingCount,
      },
    });
  } catch (error: any) {
    await client.query("ROLLBACK").catch(() => undefined);

    if (error instanceof z.ZodError) {
      return res.status(400).json({ error: error.issues });
    }

    logger.error("Job search plan audit failed", {
      error: error.message || String(error),
      durationMs: Date.now() - startedAt,
      poolTotal: pool.totalCount,
      poolIdle: pool.idleCount,
      poolWaiting: pool.waitingCount,
    });
    return res.status(500).json({ error: "Failed to audit job search query plan" });
  } finally {
    client.release();
  }
});


export default router;
