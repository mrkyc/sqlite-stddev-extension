/**
 * @file sqlite-stddev-extension.c
 * @brief SQLite extension for calculating sample and population variance and standard deviation.
 *
 * This extension provides `stddev`, `variance`, and their aliases as user-defined aggregate
 * and window functions. It is optimized for window function performance by using a circular
 * buffer to efficiently manage the sliding window of data.
 */
#include <ctype.h>
#include <math.h>
#include <sqlite3ext.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

SQLITE_EXTENSION_INIT1

// --- Configuration Constants ---

// The initial capacity for the dynamic arrays holding values.
#define INITIAL_CAPACITY 100
// The factor by which the capacity of arrays is increased when they become full.
#define CAPACITY_GROWTH_FACTOR 2
// The minimum number of data points required for population statistics.
#define MIN_COUNT_POPULATION 1
// The minimum number of data points required for sample statistics.
#define MIN_COUNT_SAMPLE 2

// --- End of Configuration Constants ---

/**
 * @struct WindowStatsData
 * @brief Holds the state for aggregate and window statistical calculations.
 *
 * This structure is the core of the extension, pointed to by SQLite's aggregate
 * context. For window functions, it implements a circular buffer (`values`) to
 * efficiently add and remove values from the sliding window. The `head` and `tail`
 * indices manage this buffer. For both aggregate and window modes, it maintains
 * a running `sum` and `sum_sq` (sum of squares) to allow for efficient,
 * on-the-fly calculation of variance and standard deviation.
 */
typedef struct {
    double *values;  // A dynamic array of values, used as a circular buffer for window functions.
    size_t count;    // The current number of values stored in the buffer.
    size_t capacity; // The current allocated capacity of the `values` buffer.
    size_t head;     // Index of the oldest element (the "front" of the circular buffer).
    size_t tail;     // Index where the next new element will be inserted (the "back").
    double sum;      // Running sum of all values in the buffer.
    double sum_sq;   // Running sum of the squares of all values.
} WindowStatsData;

/**
 * @struct StatsFunctionGroup
 * @brief Defines a group of related statistical functions to be registered.
 * This structure helps to reduce code duplication during function registration.
 */
typedef struct {
    const char **names;                // Array of function names/aliases.
    size_t name_count;                 // Number of names in the array.
    void (*xValue)(sqlite3_context *); // Pointer to the xValue function.
    void (*xFinal)(sqlite3_context *); // Pointer to the xFinal function.
} StatsFunctionGroup;

// A function pointer type for the statistical calculation functions.
typedef double (*stats_func)(const WindowStatsData *);

// --- Forward Declarations ---

// Core Calculation Logic
static double calculate_variance_sample(const WindowStatsData *data);
static double calculate_variance_population(const WindowStatsData *data);
static double calculate_stddev_sample(const WindowStatsData *data);
static double calculate_stddev_population(const WindowStatsData *data);

// SQLite Callback Functions
static void stats_step(sqlite3_context *context, int argc, sqlite3_value **argv);
static void stats_inverse(sqlite3_context *context, int argc, sqlite3_value **argv);
static void stddev_samp_value(sqlite3_context *context);
static void stddev_pop_value(sqlite3_context *context);
static void variance_samp_value(sqlite3_context *context);
static void variance_pop_value(sqlite3_context *context);
static void stddev_samp_final(sqlite3_context *context);
static void stddev_pop_final(sqlite3_context *context);
static void variance_samp_final(sqlite3_context *context);
static void variance_pop_final(sqlite3_context *context);
static void stats_destroy(void *pAggregate);

// Helper Functions
static double get_circular_value(const WindowStatsData *data, size_t logical_index);
static void add_to_circular_buffer(WindowStatsData *data, double value);
static double remove_from_circular_buffer(WindowStatsData *data);
static int init_window_stats_data(WindowStatsData *data);
static int grow_stats_buffer(WindowStatsData *data);
static void set_result(sqlite3_context *context, double result);
static void stats_value_helper(sqlite3_context *context, stats_func func, int min_count);
static void stats_final_helper(sqlite3_context *context, stats_func func, int min_count);

// Extension Initialization
static int register_stats_function_group(sqlite3 *db, const StatsFunctionGroup *group);

// --- Core Calculation Logic ---

/**
 * @brief Calculate the sample variance (using n-1 in the denominator).
 *
 * This uses Bessel's correction, which is standard for estimating population
 * variance from a sample, making it an unbiased estimator. The calculation
 * uses a standard two-pass approach (or one-pass with stored sums) for
 * numerical stability.
 * @param data The window statistics data structure.
 * @return The calculated sample variance, or NAN if count < 2.
 */
static double calculate_variance_sample(const WindowStatsData *data) {
    if (data->count < MIN_COUNT_SAMPLE)
        return NAN;
    double mean = data->sum / data->count;
    // First, calculate population variance using the formula: (sum_sq / n) - mean^2
    double variance_pop = (data->sum_sq / data->count) - (mean * mean);
    // Then, apply Bessel's correction for sample variance.
    return variance_pop * ((double)data->count / (data->count - 1));
}

/**
 * @brief Calculate the population variance (using n in the denominator).
 * @param data The window statistics data structure.
 * @return The calculated population variance, or NAN if count < 1.
 */
static double calculate_variance_population(const WindowStatsData *data) {
    if (data->count < MIN_COUNT_POPULATION)
        return NAN;
    double mean = data->sum / data->count;
    return (data->sum_sq / data->count) - (mean * mean);
}

/**
 * @brief Calculate the sample standard deviation.
 * @param data The window statistics data structure.
 * @return The calculated sample standard deviation.
 */
static double calculate_stddev_sample(const WindowStatsData *data) {
    double variance = calculate_variance_sample(data);
    return isnan(variance) ? NAN : sqrt(variance);
}

/**
 * @brief Calculate the population standard deviation.
 * @param data The window statistics data structure.
 * @return The calculated population standard deviation.
 */
static double calculate_stddev_population(const WindowStatsData *data) {
    double variance = calculate_variance_population(data);
    return isnan(variance) ? NAN : sqrt(variance);
}

// --- SQLite Callback Functions ---

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

    WindowStatsData *ctx = (WindowStatsData *)sqlite3_aggregate_context(context, sizeof(WindowStatsData));
    if (!ctx) {
        sqlite3_result_error_nomem(context);
        return;
    }

    // Initialize context on the first call.
    if (ctx->values == NULL) {
        if (init_window_stats_data(ctx) != SQLITE_OK) {
            sqlite3_result_error_nomem(context);
            return;
        }
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
    if (ctx->count >= ctx->capacity) {
        if (grow_stats_buffer(ctx) != SQLITE_OK) {
            sqlite3_result_error_nomem(context);
            return;
        }
    }

    // Add the new value to the context.
    double value = sqlite3_value_double(argv[0]);
    add_to_circular_buffer(ctx, value);
    ctx->sum += value;
    ctx->sum_sq += value * value;
}

/**
 * @brief The "inverse" function, called when a row moves out of a window frame.
 *
 * This function removes the oldest value from the statistical context, which is
 * assumed to be the one leaving the window frame. It relies on the circular
 * buffer to retrieve the value that was added earliest. The arguments (`argv`)
 * are ignored, as the function maintains its own state.
 * @param context The SQLite function context.
 * @param argc The number of arguments.
 * @param argv The argument values of the row leaving the window (ignored).
 */
static void stats_inverse(sqlite3_context *context, int argc, sqlite3_value **argv) {
    WindowStatsData *ctx = (WindowStatsData *)sqlite3_aggregate_context(context, 0);
    if (!ctx || !ctx->values || ctx->count <= 0)
        return;

    // Ignore NULL values leaving the window, consistent with how they are ignored on entry.
    if (sqlite3_value_type(argv[0]) == SQLITE_NULL)
        return;

    double removed_value = remove_from_circular_buffer(ctx);
    ctx->sum -= removed_value;
    ctx->sum_sq -= removed_value * removed_value;
}

static void stddev_samp_value(sqlite3_context *context) { stats_value_helper(context, calculate_stddev_sample, MIN_COUNT_SAMPLE); }
static void stddev_pop_value(sqlite3_context *context) { stats_value_helper(context, calculate_stddev_population, MIN_COUNT_POPULATION); }
static void variance_samp_value(sqlite3_context *context) { stats_value_helper(context, calculate_variance_sample, MIN_COUNT_SAMPLE); }
static void variance_pop_value(sqlite3_context *context) { stats_value_helper(context, calculate_variance_population, MIN_COUNT_POPULATION); }

static void stddev_samp_final(sqlite3_context *context) { stats_final_helper(context, calculate_stddev_sample, MIN_COUNT_SAMPLE); }
static void stddev_pop_final(sqlite3_context *context) { stats_final_helper(context, calculate_stddev_population, MIN_COUNT_POPULATION); }
static void variance_samp_final(sqlite3_context *context) { stats_final_helper(context, calculate_variance_sample, MIN_COUNT_SAMPLE); }
static void variance_pop_final(sqlite3_context *context) { stats_final_helper(context, calculate_variance_population, MIN_COUNT_POPULATION); }

/**
 * @brief Destructor for the aggregate context.
 *
 * This function is registered with SQLite and is guaranteed to be called,
 * even if the query is aborted or encounters an error. It ensures that all
 * dynamically allocated memory within the context is freed, preventing memory leaks.
 * @param pAggregate The aggregate context to be destroyed.
 */
static void stats_destroy(void *pAggregate) {
    WindowStatsData *ctx = (WindowStatsData *)pAggregate;
    if (ctx && ctx->values) {
        free(ctx->values);
        ctx->values = NULL;
    }
}

// --- Helper Functions ---

/**
 * @brief Gets a value at a logical index in the circular buffer.
 * @param data The window statistics data structure.
 * @param logical_index The 0-based logical index from the start of the window.
 * @return The value at the specified logical index.
 */
static double get_circular_value(const WindowStatsData *data, size_t logical_index) {
    size_t physical_index = (data->head + logical_index) % data->capacity;
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
 * @brief Initializes the WindowStatsData structure.
 * @param data The WindowStatsData structure to initialize.
 * @return SQLITE_OK on success, SQLITE_NOMEM on memory allocation failure.
 */
static int init_window_stats_data(WindowStatsData *data) {
    data->capacity = INITIAL_CAPACITY;
    data->values = (double *)malloc(data->capacity * sizeof(double));
    if (!data->values) {
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
 *
 * This function allocates a new, larger buffer and copies the existing elements
 * from the old circular buffer into a contiguous block at the start of the new one.
 * This "unrolls" the circular buffer, simplifying future operations until the
 * buffer wraps around again.
 * @param data The WindowStatsData structure to grow.
 * @return SQLITE_OK on success, SQLITE_NOMEM on memory allocation failure.
 */
static int grow_stats_buffer(WindowStatsData *data) {
    size_t new_capacity = data->capacity * CAPACITY_GROWTH_FACTOR;
    double *new_values = (double *)malloc(new_capacity * sizeof(double));
    if (!new_values) {
        return SQLITE_NOMEM;
    }
    // Copy existing data to the new, larger buffer.
    for (size_t i = 0; i < data->count; i++) {
        new_values[i] = get_circular_value(data, i);
    }
    if (data->values) {
        free(data->values);
        data->values = NULL;
    }
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

/**
 * @brief Generic "value" function for statistical calculations.
 * @param context The SQLite function context.
 * @param func The specific statistical function to call (e.g., calculate_stddev_sample).
 * @param min_count The minimum number of data points required for the calculation.
 */
static void stats_value_helper(sqlite3_context *context, stats_func func, int min_count) {
    WindowStatsData *ctx = (WindowStatsData *)sqlite3_aggregate_context(context, 0);
    if (!ctx || !ctx->values || ctx->count < (size_t)min_count) {
        sqlite3_result_null(context);
        return;
    }
    set_result(context, func(ctx));
}

/**
 * @brief Generic "final" function for statistical calculations.
 *
 * This function calculates the final result for an aggregate. Memory is cleaned
 * up by the xDestroy callback, not here, to ensure it's freed correctly even
 * if the query is aborted.
 * @param context The SQLite function context.
 * @param func The specific statistical function to call.
 * @param min_count The minimum number of data points required.
 */
static void stats_final_helper(sqlite3_context *context, stats_func func, int min_count) {
    WindowStatsData *ctx = (WindowStatsData *)sqlite3_aggregate_context(context, 0);
    if (ctx && ctx->values && ctx->count >= (size_t)min_count) {
        set_result(context, func(ctx));
    } else {
        sqlite3_result_null(context);
    }
}

// --- Extension Initialization ---

/**
 * @brief Helper function to register a group of statistical functions (lowercase and uppercase).
 * @param db The database connection.
 * @param group The function group to register.
 * @return SQLITE_OK on success, or an error code on failure.
 */
static int register_stats_function_group(sqlite3 *db, const StatsFunctionGroup *group) {
    int rc = SQLITE_OK;
    int flags = SQLITE_UTF8 | SQLITE_DETERMINISTIC | SQLITE_INNOCUOUS;

    for (size_t i = 0; i < group->name_count; i++) {
        const char *name = group->names[i];
        rc = sqlite3_create_window_function(db, name, 1, flags, 0, stats_step, group->xFinal, group->xValue, stats_inverse, stats_destroy);
        if (rc != SQLITE_OK)
            return rc;

        // Create and register the uppercase version.
        size_t name_len = strlen(name);
        char *upper_name = (char *)malloc(name_len + 1);
        if (!upper_name)
            return SQLITE_NOMEM;
        for (size_t j = 0; j < name_len; j++) {
            upper_name[j] = toupper((unsigned char)name[j]);
        }
        upper_name[name_len] = '\0';

        rc = sqlite3_create_window_function(db, upper_name, 1, flags, 0, stats_step, group->xFinal, group->xValue, stats_inverse, stats_destroy);
        if (upper_name) {
            free(upper_name);
            upper_name = NULL;
        }
        if (rc != SQLITE_OK)
            return rc;
    }
    return SQLITE_OK;
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
    size_t num_groups = sizeof(functions_to_register) / sizeof(functions_to_register[0]);
    for (size_t i = 0; i < num_groups; i++) {
        rc = register_stats_function_group(db, &functions_to_register[i]);
        if (rc != SQLITE_OK) {
            return rc;
        }
    }

    return rc;
}