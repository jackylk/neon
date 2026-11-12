/* Service-side ceilings around PostgreSQL native resource GUCs.
 * Defaults alone are USERSET and are not a security boundary. These checks
 * cover SET/set_config/startup/role settings without parsing customer SQL.
 */
#include "postgres.h"
#include <math.h>
#include <limits.h>
#include "miscadmin.h"
#include "utils/guc.h"
#include "utils/guc_tables.h"
#include "public_query_limits.h"

static bool enabled = false;
static bool initialized = false;
static bool hooks_ready = false;

enum LimitId { STATEMENT, LOCK_WAIT, IDLE_TRANSACTION, WORK_MEM, MAINTENANCE_MEM, PARALLEL_WORKERS, N_LIMITS };
static const char *names[N_LIMITS] = {
    "statement_timeout", "lock_timeout", "idle_in_transaction_session_timeout",
    "work_mem", "maintenance_work_mem", "max_parallel_workers_per_gather"
};
static int maxima[N_LIMITS] = {60000, 5000, 60000, 16384, 65536, 2};
static GucIntCheckHook previous[N_LIMITS];
static int *current_values[N_LIMITS];
static double *current_hash_multiplier;
static GucRealCheckHook previous_hash_multiplier;
static double max_hash_multiplier = 2.0;

/* Background workers must retain PostgreSQL's internal ability to disable
 * native timeouts. Public SQL executes in regular backends; the postmaster
 * is checked separately so unsafe service startup configuration still fails.
 */
static bool
public_backend(void)
{
    return enabled && (!IsUnderPostmaster || AmRegularBackendProcess());
}

static void
reject_stored_setting(GucSource source, const char *name)
{
    /* PG normally downgrades invalid ALTER ROLE/DATABASE defaults to WARNING.
     * A service budget is authoritative: reject this login instead of silently
     * masking a persisted policy/configuration error.
     */
    if (source == PGC_S_DATABASE || source == PGC_S_USER || source == PGC_S_DATABASE_USER)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                       errmsg("public query budget rejects stored setting for %s", name)));
}

static bool
check_enabled(bool *value, void **extra, GucSource source)
{
    int i;

    if (!*value || !hooks_ready || (IsUnderPostmaster && !AmRegularBackendProcess()))
        return true;
    for (i = 0; i < N_LIMITS; i++)
    {
        if ((*current_values[i] <= 0 && i != PARALLEL_WORKERS) || *current_values[i] > maxima[i])
        {
            reject_stored_setting(source, names[i]);
            GUC_check_errdetail("Cannot enable public query guard with unsafe current setting: %s.", names[i]);
            return false;
        }
    }
    if (!isfinite(max_hash_multiplier) || !isfinite(*current_hash_multiplier)
        || *current_hash_multiplier < 1.0 || *current_hash_multiplier > max_hash_multiplier)
    {
        reject_stored_setting(source, "hash_mem_multiplier");
        GUC_check_errdetail("Cannot enable public query guard with unsafe hash memory setting.");
        return false;
    }
    return true;
}

static bool
check_int_limit(enum LimitId id, int *value, void **extra, GucSource source)
{
    if (previous[id] && !previous[id](value, extra, source))
        return false;
    if (public_backend() && ((*value <= 0 && id != PARALLEL_WORKERS) || *value > maxima[id]))
    {
        reject_stored_setting(source, names[id]);
        GUC_check_errdetail("Public query budget for %s permits %s through %d.",
                            names[id], id == PARALLEL_WORKERS ? "0" : "1", maxima[id]);
        return false;
    }
    return true;
}

#define LIMIT_CHECK(fn, id) \
static bool fn(int *value, void **extra, GucSource source) \
{ return check_int_limit(id, value, extra, source); }
LIMIT_CHECK(check_statement, STATEMENT)
LIMIT_CHECK(check_lock, LOCK_WAIT)
LIMIT_CHECK(check_idle, IDLE_TRANSACTION)
LIMIT_CHECK(check_work, WORK_MEM)
LIMIT_CHECK(check_maintenance, MAINTENANCE_MEM)
LIMIT_CHECK(check_parallel, PARALLEL_WORKERS)

static bool
check_hash_multiplier(double *value, void **extra, GucSource source)
{
    if (previous_hash_multiplier && !previous_hash_multiplier(value, extra, source))
        return false;
    if (public_backend() && (!isfinite(*value) || *value < 1.0 || *value > max_hash_multiplier))
    {
        reject_stored_setting(source, "hash_mem_multiplier");
        GUC_check_errdetail("Public query hash memory multiplier exceeds the service budget.");
        return false;
    }
    return true;
}

void
neon_public_query_limits_init(void)
{
    int count;
    int i;
    struct config_generic **gucs;
    GucIntCheckHook hooks[N_LIMITS] = {
        check_statement, check_lock, check_idle, check_work, check_maintenance, check_parallel
    };
    const char *cap_names[N_LIMITS] = {
        "neon.public_max_statement_ms", "neon.public_max_lock_ms",
        "neon.public_max_idle_transaction_ms", "neon.public_max_work_mem_kb",
        "neon.public_max_maintenance_mem_kb", "neon.public_max_parallel_workers"
    };
    bool found[N_LIMITS] = {false};
    bool found_hash = false;

    if (initialized)
        return;
    initialized = true;
    DefineCustomBoolVariable("neon.public_query_limits",
        "Enforce public query resource ceilings.", NULL, &enabled, false,
        PGC_SUSET, 0, check_enabled, NULL, NULL);
    for (i = 0; i < N_LIMITS; i++)
        DefineCustomIntVariable(cap_names[i], "Maximum public query resource setting.",
            NULL, &maxima[i], maxima[i], i == PARALLEL_WORKERS ? 0 : 1,
            INT_MAX, PGC_POSTMASTER, 0, NULL, NULL, NULL);
    DefineCustomRealVariable("neon.public_max_hash_multiplier",
        "Maximum public hash memory multiplier.", NULL, &max_hash_multiplier,
        2.0, 1.0, 100.0, PGC_POSTMASTER, 0, NULL, NULL, NULL);

#if PG_VERSION_NUM >= 160000
    gucs = get_guc_variables(&count);
#else
    gucs = get_guc_variables();
    count = GetNumConfigOptions();
#endif
    for (i = 0; i < count; i++)
    {
        struct config_generic *guc = gucs[i];
        int j;
        for (j = 0; j < N_LIMITS; j++)
        {
            if (strcmp(guc->name, names[j]) == 0)
            {
                struct config_int *setting;
                if (guc->vartype != PGC_INT)
                    elog(FATAL, "unexpected GUC type for public query budget");
                setting = (struct config_int *) guc;
                current_values[j] = setting->variable;
                previous[j] = setting->check_hook;
                setting->check_hook = hooks[j];
                found[j] = true;
                if (enabled && ((*setting->variable <= 0 && j != PARALLEL_WORKERS)
                                || *setting->variable > maxima[j]))
                    ereport(FATAL, (errmsg("public query budget has unsafe startup setting: %s", names[j])));
            }
        }
        if (strcmp(guc->name, "hash_mem_multiplier") == 0)
        {
            struct config_real *setting = (struct config_real *) guc;
            current_hash_multiplier = setting->variable;
            previous_hash_multiplier = setting->check_hook;
            setting->check_hook = check_hash_multiplier;
            found_hash = true;
            if (enabled && (!isfinite(*setting->variable) || !isfinite(max_hash_multiplier)
                            || *setting->variable < 1.0 || *setting->variable > max_hash_multiplier))
                elog(FATAL, "public query budget has unsafe hash memory multiplier");
        }
    }
    for (i = 0; i < N_LIMITS; i++)
        if (!found[i])
            elog(FATAL, "required public query resource parameter is missing");
    if (!found_hash)
        elog(FATAL, "required public hash memory parameter is missing");
    hooks_ready = true;
}
