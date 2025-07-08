/**
 * @file sqlite-stddev-extension.c
 * @brief SQLite extension for calculating sample and population variance and standard deviation.
 *
 * This extension provides `stddev`, `variance`, and their aliases as user-defined aggregate
 * and window functions. It is optimized for window function performance by using a circular
 * buffer to efficiently manage the sliding window of data.
 */
#include <math.h>
#include <sqlite3ext.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

SQLITE_EXTENSION_INIT1

// --- Configuration Constants for Statistics Calculation ---

// The initial capacity for the dynamic arrays holding values.
#define INITIAL_CAPACITY 100
// The factor by which the capacity of arrays is increased when they become full.
#define CAPACITY_GROWTH_FACTOR 2

// --- End of Configuration Constants ---

/**
 * @struct WindowStatsData
 * @brief Holds the statistical data for a window function's state.
 *
 * This structure maintains the state required for calculating running
 * statistics. It uses a circular buffer and keeps track of the sum and
 * sum of squares for efficient O(1) updates.
 */
typedef struct {
    double *values; // Pointer to a dynamic array of values (circular buffer).
    int count;      // The current number of values stored in the buffer.
    int capacity;   // The current allocated capacity of the `values` buffer.
    int head;       // Index of the oldest element (the "front" of the circular buffer).
    int tail;       // Index where the next new element will be inserted (the "back").
    double sum;     // Running sum of all values in the buffer.
    double sum_sq;  // Running sum of the squares of all values.
} WindowStatsData;

/**
 * @struct StatsWindowContext
 * @brief Wrapper structure for the statistics window function context.
 *
 * This is the structure that SQLite's aggregate context pointer will point to.
 */
typedef struct {
    WindowStatsData data; // The statistical data and state.
} StatsWindowContext;

// --- Circular Buffer and Calculation Helper Functions ---

/**
 * @brief Gets a value at a logical index in the circular buffer.
 * @param data The window statistics data structure.
 * @param logical_index The 0-based logical index from the start of the window.
 * @return The value at the specified logical index.
 */
static double get_circular_value(WindowStatsData *data, int logical_index) {
    int physical_index = (data->head + logical_index) % data->capacity;
    return data->values[physical_index];
}

/**
 * @brief Adds a new value to the end (tail) of the circular buffer.
 * @param data The window statistics data structure.
 * @param value The value to add.
 */
static void add_to_circular_buffer(WindowStatsData *data, double value) {
    data->values[data->tail] = value;
    data->tail = (data->tail + 1) % data->capacity;
    data->count++;
}

/**
 * @brief Removes a value from the beginning (head) of the circular buffer.
 * @param data The window statistics data structure.
 * @return The value that was removed.
 */
static double remove_from_circular_buffer(WindowStatsData *data) {
    if (data->count == 0)
        return 0.0;
    double removed_value = data->values[data->head];
    data->head = (data->head + 1) % data->capacity;
    data->count--;
    return removed_value;
}

/**
 * @brief Calculate the sample variance (using n-1 in the denominator).
 *
 * This uses Bessel's correction, which is standard for estimating population
 * variance from a sample, making it an unbiased estimator.
 * @param data The window statistics data structure.
 * @return The calculated sample variance, or NAN if count < 2.
 */
static double calculate_variance_sample(WindowStatsData *data) {
    if (data->count < 2)
        return NAN;
    double mean = data->sum / data->count;
    // This is Welford's algorithm for variance, adapted for a full dataset.
    double variance_pop = (data->sum_sq / data->count) - (mean * mean);
    // Apply Bessel's correction for sample variance.
    return variance_pop * ((double)data->count / (data->count - 1));
}

/**
 * @brief Calculate the population variance (using n in the denominator).
 * @param data The window statistics data structure.
 * @return The calculated population variance, or NAN if count < 1.
 */
static double calculate_variance_population(WindowStatsData *data) {
    if (data->count < 1)
        return NAN;
    double mean = data->sum / data->count;
    return (data->sum_sq / data->count) - (mean * mean);
}

/**
 * @brief Calculate the sample standard deviation.
 * @param data The window statistics data structure.
 * @return The calculated sample standard deviation.
 */
static double calculate_stddev_sample(WindowStatsData *data) {
    double variance = calculate_variance_sample(data);
    return isnan(variance) ? NAN : sqrt(variance);
}

/**
 * @brief Calculate the population standard deviation.
 * @param data The window statistics data structure.
 * @return The calculated population standard deviation.
 */
static double calculate_stddev_population(WindowStatsData *data) {
    double variance = calculate_variance_population(data);
    return isnan(variance) ? NAN : sqrt(variance);
}

// --- Context Management and Result Handling ---

/**
 * @brief Initializes the WindowStatsData structure.
 * @param context The SQLite function context for error reporting.
 * @param data The WindowStatsData structure to initialize.
 * @return SQLITE_OK on success, SQLITE_NOMEM on memory allocation failure.
 */
static int init_window_stats_data(sqlite3_context *context, WindowStatsData *data) {
    data->capacity = INITIAL_CAPACITY;
    data->values = (double *)malloc(data->capacity * sizeof(double));
    if (!data->values) {
        sqlite3_result_error_nomem(context);
        return SQLITE_NOMEM;
    }
    data->count = 0;
    data->head = 0;
    data->tail = 0;
    data->sum = 0.0;
    data->sum_sq = 0.0;
    return SQLITE_OK;
}

/**
 * @brief Grows the buffer within the WindowStatsData structure.
 * @param context The SQLite function context for error reporting.
 * @param data The WindowStatsData structure to grow.
 * @return SQLITE_OK on success, SQLITE_NOMEM on memory allocation failure.
 */
static int grow_stats_buffer(sqlite3_context *context, WindowStatsData *data) {
    int new_capacity = data->capacity * CAPACITY_GROWTH_FACTOR;
    double *new_values = (double *)malloc(new_capacity * sizeof(double));
    if (!new_values) {
        sqlite3_result_error_nomem(context);
        return SQLITE_NOMEM;
    }
    // Copy existing data to the new, larger buffer.
    for (int i = 0; i < data->count; i++) {
        new_values[i] = get_circular_value(data, i);
    }
    free(data->values);
    data->values = new_values;
    data->capacity = new_capacity;
    data->head = 0;
    data->tail = data->count;
    return SQLITE_OK;
}

/**
 * @brief Helper to set the result, handling NAN/INF values.
 * @param context The SQLite function context.
 * @param result The double result to set.
 */
static void set_result(sqlite3_context *context, double result) {
    if (isnan(result) || isinf(result)) {
        sqlite3_result_null(context);
    } else {
        sqlite3_result_double(context, result);
    }
}

// --- UNIFIED CALLBACKS FOR AGGREGATE AND WINDOW FUNCTIONS ---

/**
 * @brief The "step" function, called for each row in the aggregate or window frame.
 *
 * This function adds a new value to the statistical context. It handles context
 * initialization, buffer growth, and data type validation.
 *
 * @param context The SQLite function context.
 * @param argc The number of arguments.
 * @param argv The argument values.
 */
static void stats_step(sqlite3_context *context, int argc, sqlite3_value **argv) {
    if (argc != 1) {
        sqlite3_result_error(context, "Statistics functions require exactly 1 argument", -1);
        return;
    }

    StatsWindowContext *ctx = (StatsWindowContext *)sqlite3_aggregate_context(context, sizeof(StatsWindowContext));
    if (!ctx) {
        sqlite3_result_error_nomem(context);
        return;
    }

    // Initialize context on the first call.
    if (ctx->data.values == NULL) {
        if (init_window_stats_data(context, &ctx->data) != SQLITE_OK)
            return;
    }

    // Check the type of the incoming value.
    int value_type = sqlite3_value_type(argv[0]);
    if (value_type == SQLITE_NULL)
        return; // Ignore NULLs.

    if (value_type != SQLITE_INTEGER && value_type != SQLITE_FLOAT) {
        sqlite3_result_error(context, "Invalid data type, expected numeric value.", -1);
        return;
    }

    // Grow buffer if it is full.
    if (ctx->data.count >= ctx->data.capacity) {
        if (grow_stats_buffer(context, &ctx->data) != SQLITE_OK)
            return;
    }

    // Add the new value to the context.
    double value = sqlite3_value_double(argv[0]);
    add_to_circular_buffer(&ctx->data, value);
    ctx->data.sum += value;
    ctx->data.sum_sq += value * value;
}

/**
 * @brief The "inverse" function, called when a row moves out of a window frame.
 * This removes the oldest value from the statistical context.
 * @param context The SQLite function context.
 * @param argc The number of arguments.
 * @param argv The argument values of the row leaving the window.
 */
static void stats_inverse(sqlite3_context *context, int argc, sqlite3_value **argv) {
    StatsWindowContext *ctx = (StatsWindowContext *)sqlite3_aggregate_context(context, 0);
    if (!ctx || !ctx->data.values || ctx->data.count <= 0)
        return;

    if (sqlite3_value_type(argv[0]) == SQLITE_NULL)
        return;

    double removed_value = remove_from_circular_buffer(&ctx->data);
    ctx->data.sum -= removed_value;
    ctx->data.sum_sq -= removed_value * removed_value;
}

// --- Value/Final Callback Helpers ---

// A function pointer type for the statistical calculation functions.
typedef double (*stats_func)(WindowStatsData *);

/**
 * @brief Generic "value" function for statistical calculations.
 * @param context The SQLite function context.
 * @param func The specific statistical function to call (e.g., calculate_stddev_sample).
 * @param min_count The minimum number of data points required for the calculation.
 */
static void stats_value_helper(sqlite3_context *context, stats_func func, int min_count) {
    StatsWindowContext *ctx = (StatsWindowContext *)sqlite3_aggregate_context(context, 0);
    if (!ctx || !ctx->data.values || ctx->data.count < min_count) {
        sqlite3_result_null(context);
        return;
    }
    set_result(context, func(&ctx->data));
}

/**
 * @brief Generic "final" function for statistical calculations.
 * This also handles cleaning up the allocated memory.
 * @param context The SQLite function context.
 * @param func The specific statistical function to call.
 * @param min_count The minimum number of data points required.
 */
static void stats_final_helper(sqlite3_context *context, stats_func func, int min_count) {
    StatsWindowContext *ctx = (StatsWindowContext *)sqlite3_aggregate_context(context, 0);
    if (ctx && ctx->data.values && ctx->data.count >= min_count) {
        set_result(context, func(&ctx->data));
    } else {
        sqlite3_result_null(context);
    }
    // Clean up memory for the aggregate context.
    if (ctx && ctx->data.values) {
        free(ctx->data.values);
        ctx->data.values = NULL;
    }
}

// --- Specific Implementations for Value/Final Callbacks ---

static void stddev_samp_value(sqlite3_context *context) { stats_value_helper(context, calculate_stddev_sample, 2); }
static void stddev_pop_value(sqlite3_context *context) { stats_value_helper(context, calculate_stddev_population, 1); }
static void variance_samp_value(sqlite3_context *context) { stats_value_helper(context, calculate_variance_sample, 2); }
static void variance_pop_value(sqlite3_context *context) { stats_value_helper(context, calculate_variance_population, 1); }

static void stddev_samp_final(sqlite3_context *context) { stats_final_helper(context, calculate_stddev_sample, 2); }
static void stddev_pop_final(sqlite3_context *context) { stats_final_helper(context, calculate_stddev_population, 1); }
static void variance_samp_final(sqlite3_context *context) { stats_final_helper(context, calculate_variance_sample, 2); }
static void variance_pop_final(sqlite3_context *context) { stats_final_helper(context, calculate_variance_population, 1); }

// --- Extension Initialization ---

/**
 * @struct StatsFunctionGroup
 * @brief Defines a group of related statistical functions to be registered.
 * This structure helps to reduce code duplication during function registration.
 */
typedef struct {
    const char **names;                // Array of function names/aliases.
    int name_count;                    // Number of names in the array.
    void (*xValue)(sqlite3_context *); // Pointer to the xValue function.
    void (*xFinal)(sqlite3_context *); // Pointer to the xFinal function.
} StatsFunctionGroup;

/**
 * @brief Helper function to register a unified statistical function (lowercase and uppercase).
 * @param db The database connection.
 * @param name The name of the function to register.
 * @param xFinal The final function for aggregate mode.
 * @param xValue The value function for window mode.
 * @return SQLITE_OK on success, or an error code on failure.
 */
static int register_unified_stats_function(sqlite3 *db, const char *name, void (*xFinal)(sqlite3_context *), void (*xValue)(sqlite3_context *)) {
    int rc;
    // Register the lowercase version.
    rc = sqlite3_create_window_function(db, name, 1, SQLITE_UTF8 | SQLITE_DETERMINISTIC, 0, stats_step, xFinal, xValue, stats_inverse, NULL);
    if (rc != SQLITE_OK)
        return rc;

    // Create and register the uppercase version.
    char *upper_name = malloc(strlen(name) + 1);
    if (!upper_name)
        return SQLITE_NOMEM;
    strcpy(upper_name, name);
    for (int i = 0; upper_name[i]; i++) {
        if (upper_name[i] >= 'a' && upper_name[i] <= 'z') {
            upper_name[i] = upper_name[i] - 'a' + 'A';
        }
    }

    rc = sqlite3_create_window_function(db, upper_name, 1, SQLITE_UTF8 | SQLITE_DETERMINISTIC | SQLITE_INNOCUOUS, 0, stats_step, xFinal, xValue, stats_inverse, NULL);
    free(upper_name);
    return rc;
}

/**
 * @brief The main entry point for the SQLite extension.
 *
 * This function is called by SQLite when the extension is loaded. It registers
 * all the custom statistical functions and their aliases.
 *
 * @param db The database connection.
 * @param pzErrMsg A pointer to an error message string.
 * @param pApi A pointer to the SQLite API routines.
 * @return SQLITE_OK on success, or an error code on failure.
 */
int sqlite3_extension_init(sqlite3 *db, char **pzErrMsg, const sqlite3_api_routines *pApi) {
    int rc = SQLITE_OK;
    SQLITE_EXTENSION_INIT2(pApi);

    // Define the names and aliases for each statistical function.
    const char *stddev_samp_names[] = {"stddev_samp", "stddev_sample", "stdev_samp", "stdev_sample", "stddev", "stdev", "std_dev", "standard_deviation"};
    const char *stddev_pop_names[] = {"stddev_pop", "stddev_population", "stdev_pop", "stdev_population"};
    const char *variance_samp_names[] = {"variance_samp", "variance_sample", "var_samp", "var_sample", "variance", "var"};
    const char *variance_pop_names[] = {"variance_pop", "variance_population", "var_pop", "var_population"};

    // Define the groups of functions to be registered.
    StatsFunctionGroup functions_to_register[] = {
        {stddev_samp_names, sizeof(stddev_samp_names) / sizeof(stddev_samp_names[0]), stddev_samp_value, stddev_samp_final},
        {stddev_pop_names, sizeof(stddev_pop_names) / sizeof(stddev_pop_names[0]), stddev_pop_value, stddev_pop_final},
        {variance_samp_names, sizeof(variance_samp_names) / sizeof(variance_samp_names[0]), variance_samp_value, variance_samp_final},
        {variance_pop_names, sizeof(variance_pop_names) / sizeof(variance_pop_names[0]), variance_pop_value, variance_pop_final}};

    // Iterate through the groups and register each function and its aliases.
    int num_groups = sizeof(functions_to_register) / sizeof(functions_to_register[0]);
    for (int i = 0; i < num_groups; i++) {
        StatsFunctionGroup *group = &functions_to_register[i];
        for (int j = 0; j < group->name_count; j++) {
            rc = register_unified_stats_function(db, group->names[j], group->xFinal, group->xValue);
            if (rc != SQLITE_OK)
                return rc;
        }
    }

    return rc;
}
