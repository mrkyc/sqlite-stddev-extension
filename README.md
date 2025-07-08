# Standard Deviation and Variance Extension for SQLite

This code implements `stddev` (standard deviation) and `variance` functions as an extension for the SQLite database. It allows you to calculate these statistical measures for both sample and population data, supporting both aggregate and window function contexts.

## Table of Contents

- [How It Works](#how-it-works)
- [Available Functions](#available-functions)
- [Compilation and Loading](#compilation-and-loading)
- [Usage](#usage)
  - [Syntax](#syntax)
  - [Arguments](#arguments)
- [Examples](#examples)
- [Limitations and Error Handling](#limitations-and-error-handling)

## How It Works

The extension provides functions to calculate the standard deviation and variance of a set of numeric values. It is designed to work efficiently as both an aggregate function (calculating the statistic over an entire group) and a window function (calculating the statistic over a defined window frame).

### Key Features:

-   **Sample vs. Population:** Distinct functions are provided for calculating sample statistics (using `n-1` in the denominator, applying Bessel's correction for unbiased estimation) and population statistics (using `n` in the denominator).
-   **Aliases:** For convenience, multiple aliases are registered for each function (e.g., `stddev`, `stdev`, `stddev_samp`, `variance`, `var`, `var_samp`, etc.). Both lowercase and uppercase versions of the primary function names are supported.

## Available Functions

### `stddev_samp(numeric_value)`
-   **Returns:** A single floating-point number (`DOUBLE`).
-   **Description:** Calculates the sample standard deviation. This uses Bessel's correction (`n-1` in the denominator) for an unbiased estimate of the population standard deviation from a sample. Aliases include `stddev`, `stdev`, `stddev_sample`, `stdev_samp`, `std_dev`, `standard_deviation`.

### `stddev_pop(numeric_value)`
-   **Returns:** A single floating-point number (`DOUBLE`).
-   **Description:** Calculates the population standard deviation (`n` in the denominator). Aliases include `stddev_population`, `stdev_pop`, `stdev_population`.

### `variance_samp(numeric_value)`
-   **Returns:** A single floating-point number (`DOUBLE`).
-   **Description:** Calculates the sample variance. This uses Bessel's correction (`n-1` in the denominator). Aliases include `variance`, `var`, `variance_sample`, `var_samp`, `var_sample`.

### `variance_pop(numeric_value)`
-   **Returns:** A single floating-point number (`DOUBLE`).
-   **Description:** Calculates the population variance (`n` in the denominator). Aliases include `variance_population`, `var_pop`, `var_population`.

## Compilation and Loading

To use this extension, you first need to compile it into a shared library.

### Prerequisites

Before compiling, you must have the SQLite development libraries installed, which provide the necessary header files (`sqlite3.h` and `sqlite3ext.h`).

-   **On Debian/Ubuntu:** `sudo apt-get install sqlite3-dev`
-   **On Fedora/CentOS:** `sudo dnf install sqlite-devel`
-   **On macOS (with Homebrew):** `brew install sqlite`
-   **On Windows:** Ensure your compiler (like MinGW-w64) has access to the SQLite source code or pre-compiled headers.

### Compilation Instructions

Compile `sqlite-stddev-extension.c` into a shared library.

- **Linux:** `gcc -shared -fPIC -o sqlite-stddev-extension.so sqlite-stddev-extension.c -lm`
- **macOS:** `gcc -shared -fPIC -I$(brew --prefix sqlite)/include -undefined dynamic_lookup -o sqlite-stddev-extension.dylib sqlite-stddev-extension.c -lm`
- **Windows:** `gcc -shared -o sqlite-stddev-extension.dll sqlite-stddev-extension.c -lm`

### Loading the Extension

Once compiled, you can load the extension in your SQLite session:

```sql
-- On Linux
.load ./sqlite-stddev-extension.so

-- On macOS
.load ./sqlite-stddev-extension.dylib

-- On Windows
.load ./sqlite-stddev-extension.dll
```

## Usage

The `stddev` and `variance` functions are available as aggregate functions and window functions. They are registered under various names and aliases.

### Syntax

#### Aggregate Function Syntax

```sql
FUNCTION_NAME(numeric_value)
```

#### Window Function Syntax

```sql
FUNCTION_NAME(numeric_value) OVER (PARTITION BY ... ORDER BY ... ROWS BETWEEN ...)
```

### Arguments

1.  `numeric_value` (numeric): The numeric value for which to calculate the statistic.

## Examples

Let's assume we have a `measurements` table with the following data:

```sql
CREATE TABLE measurements (
  id INTEGER PRIMARY KEY,
  value REAL
);

INSERT INTO measurements (value) VALUES
(10), (12), (15), (13), (18), (20), (22), (25), (23), (28);
```

### Aggregate Function Examples

#### Sample Standard Deviation

Calculates the sample standard deviation for all values in the `measurements` table.

```sql
SELECT stddev_samp(value) AS sample_stddev
FROM measurements;
```

#### Population Standard Deviation

Calculates the population standard deviation for all values in the `measurements` table.

```sql
SELECT stddev_pop(value) AS population_stddev
FROM measurements;
```

#### Sample Variance

Calculates the sample variance for all values in the `measurements` table.

```sql
SELECT variance_samp(value) AS sample_variance
FROM measurements;
```

#### Population Variance

Calculates the population variance for all values in the `measurements` table.

```sql
SELECT variance_pop(value) AS population_variance
FROM measurements;
```

### Window Function Examples

#### Rolling Sample Standard Deviation

Calculates the sample standard deviation for a rolling window of the current row and the thirty preceding rows, ordered by `id`.

```sql
SELECT
  id,
  value,
  stddev_samp(value) OVER (
    ORDER BY id
    ROWS BETWEEN 30 PRECEDING AND CURRENT ROW
  ) AS rolling_sample_stddev
FROM measurements;
```

#### Rolling Population Variance

Calculates the population variance for a rolling window of the current row and the thirty preceding rows, ordered by `id`.

```sql
SELECT
  id,
  value,
  variance_pop(value) OVER (
    ORDER BY id
    ROWS BETWEEN 30 PRECEDING AND CURRENT ROW
  ) AS rolling_population_variance
FROM measurements;
```

## Limitations and Error Handling

-   **Minimum Data Points:**
    -   Sample standard deviation and variance functions (`stddev_samp`, `variance_samp`, and their aliases) require at least two data points. If fewer than two points are available, they will return `NULL`.
    -   Population standard deviation and variance functions (`stddev_pop`, `variance_pop`, and their aliases) require at least one data point. If no points are available, they will return `NULL`.
-   **Data Type:** Only numeric values (INTEGER or REAL) are supported. Non-numeric values will result in an error.
-   **NULL Handling:** `NULL` values in the input are ignored and do not contribute to the calculation. If all values in a group or window are `NULL`, the result will be `NULL`.
-   **NaN/Infinity:** Results that are Not-a-Number (NaN) or Infinity (INF) will be returned as `NULL` by SQLite. This can occur in edge cases, such as attempting to calculate standard deviation from a single data point (for sample) or from a set of identical values (for variance where the sum of squares might lead to floating point issues if not handled carefully).
-   **Internal Error Handling (C Code):**
    -   **Invalid Arguments:** The C code explicitly checks for the correct number of arguments (exactly 1) and valid numeric data types. If an invalid argument count or non-numeric input is provided, `sqlite3_result_error` is used to return an error message to SQLite.
    -   **Memory Allocation Failures:** In cases where dynamic memory allocation (e.g., for the circular buffer) fails, `sqlite3_result_error_nomem` is used to signal an out-of-memory condition to SQLite.
    -   **Insufficient Data/Edge Cases:** As mentioned above, `NULL` is returned for insufficient data points (e.g., less than 2 for sample statistics) or when calculations yield `NaN` or `Infinity` (e.g., division by zero in variance calculation for a single data point). This is handled by `sqlite3_result_null`.