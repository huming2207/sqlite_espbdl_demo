/*
 * SPDX-FileCopyrightText: 2026
 * SPDX-License-Identifier: CC0-1.0
 */
#include <freertos/FreeRTOS.h>
#include <freertos/task.h>
#include <inttypes.h>
#include <stdbool.h>
#include <stdio.h>
#include <string.h>

#include "esp_blockdev.h"
#include "esp_err.h"
#include "esp_log.h"
#include "esp_partition.h"
#include "esp_timer.h"
#include "esp_vfs_fat.h"
#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "sqlite3.h"
#include "sqlite_esp_fs.h"
#include "sqlite_espbdl.h"
#include "wear_levelling.h"
#include "freertos/task.h"

#define SQLITE_PARTITION_LABEL "sqlite"
#define SQLITE_DATABASE_NAME   "main.db"
#define FATFS_MOUNT_PATH        "/fat"
#define FATFS_DATABASE_PATH     FATFS_MOUNT_PATH "/data.db"
#define API_BENCHMARK_SAMPLES  8
#define API_BENCHMARK_BLOB_SIZE 512

static const char *TAG = "sqlite_demo";

static esp_err_t sqlite_result(sqlite3 *db, int rc, const char *operation)
{
    if (rc == SQLITE_OK || rc == SQLITE_DONE || rc == SQLITE_ROW) {
        return ESP_OK;
    }
    ESP_LOGE(TAG, "%s failed: SQLite rc=%d, message=%s", operation, rc,
             db ? sqlite3_errmsg(db) : sqlite3_errstr(rc));
    return ESP_FAIL;
}

static esp_err_t execute_sql(sqlite3 *db, const char *sql)
{
    char *error_message = NULL;
    int rc = sqlite3_exec(db, sql, NULL, NULL, &error_message);
    if (rc != SQLITE_OK) {
        ESP_LOGE(TAG, "SQL failed: %s; statement: %s",
                 error_message ? error_message : sqlite3_errmsg(db), sql);
        sqlite3_free(error_message);
        return ESP_FAIL;
    }
    sqlite3_free(error_message);
    return ESP_OK;
}

static esp_err_t enable_wal(sqlite3 *db)
{
    if (execute_sql(db, "PRAGMA locking_mode=NORMAL") != ESP_OK ||
        execute_sql(db, "PRAGMA synchronous=FULL") != ESP_OK) {
        return ESP_FAIL;
    }

    sqlite3_stmt *statement = NULL;
    int rc = sqlite3_prepare_v2(db, "PRAGMA journal_mode=WAL", -1,
                                &statement, NULL);
    if (sqlite_result(db, rc, "prepare journal_mode") != ESP_OK) {
        return ESP_FAIL;
    }

    rc = sqlite3_step(statement);
    if (rc != SQLITE_ROW) {
        sqlite_result(db, rc, "enable WAL");
        sqlite3_finalize(statement);
        return ESP_FAIL;
    }
    const unsigned char *mode = sqlite3_column_text(statement, 0);
    bool wal_enabled = mode && strcmp((const char *)mode, "wal") == 0;
    ESP_LOGI(TAG, "journal mode: %s", mode ? (const char *)mode : "(null)");
    rc = sqlite3_finalize(statement);
    if (!wal_enabled || sqlite_result(db, rc, "finalize journal_mode") != ESP_OK) {
        return ESP_FAIL;
    }
    return ESP_OK;
}

static esp_err_t create_schema(sqlite3 *db)
{
    if (execute_sql(db,
        "PRAGMA foreign_keys=ON") != ESP_OK || execute_sql(db,
        "CREATE TABLE IF NOT EXISTS inventory ("
        "id INTEGER PRIMARY KEY,"
        "name TEXT NOT NULL UNIQUE,"
        "quantity INTEGER NOT NULL CHECK(quantity >= 0),"
        "price_cents INTEGER NOT NULL CHECK(price_cents >= 0)"
        ")") != ESP_OK || execute_sql(db,
        "CREATE TABLE IF NOT EXISTS telemetry ("
        "id INTEGER PRIMARY KEY,"
        "inventory_id INTEGER NOT NULL REFERENCES inventory(id) ON DELETE CASCADE,"
        "device_id TEXT NOT NULL,"
        "captured_at INTEGER NOT NULL,"
        "payload_json TEXT NOT NULL CHECK(json_valid(payload_json)),"
        "raw_packet BLOB NOT NULL"
        ")") != ESP_OK || execute_sql(db,
        "CREATE INDEX IF NOT EXISTS telemetry_device_time_idx "
        "ON telemetry(device_id, captured_at DESC)") != ESP_OK || execute_sql(db,
        "CREATE INDEX IF NOT EXISTS telemetry_sensor_idx "
        "ON telemetry(json_extract(payload_json, '$.sensor'))") != ESP_OK ||
        execute_sql(db,
        "CREATE TABLE IF NOT EXISTS binary_assets ("
        "id INTEGER PRIMARY KEY,"
        "name TEXT NOT NULL UNIQUE,"
        "mime_type TEXT NOT NULL,"
        "metadata_json TEXT NOT NULL CHECK(json_valid(metadata_json)),"
        "data BLOB NOT NULL"
        ")") != ESP_OK) {
        return ESP_FAIL;
    }
    return ESP_OK;
}

static esp_err_t insert_demo_rows(sqlite3 *db)
{
    static const struct {
        const char *name;
        int quantity;
        int price_cents;
    } rows[] = {
        { "sensor",  12, 1599 },
        { "display",  5, 2499 },
        { "relay",   20,  375 },
        { "enclosure", 8, 1290 },
    };

    if (execute_sql(db, "BEGIN IMMEDIATE") != ESP_OK) return ESP_FAIL;

    sqlite3_stmt *statement = NULL;
    int rc = sqlite3_prepare_v2(db,
        "INSERT INTO inventory(name, quantity, price_cents) VALUES(?1, ?2, ?3)",
        -1, &statement, NULL);
    if (sqlite_result(db, rc, "prepare insert") != ESP_OK) {
        execute_sql(db, "ROLLBACK");
        return ESP_FAIL;
    }

    esp_err_t result = ESP_OK;
    for (size_t i = 0; i < sizeof(rows) / sizeof(rows[0]); ++i) {
        rc = sqlite3_bind_text(statement, 1, rows[i].name, -1, SQLITE_STATIC);
        if (rc == SQLITE_OK) rc = sqlite3_bind_int(statement, 2, rows[i].quantity);
        if (rc == SQLITE_OK) rc = sqlite3_bind_int(statement, 3, rows[i].price_cents);
        if (rc == SQLITE_OK) rc = sqlite3_step(statement);
        if (rc != SQLITE_DONE) {
            sqlite_result(db, rc, "insert row");
            result = ESP_FAIL;
            break;
        }
        rc = sqlite3_reset(statement);
        if (rc == SQLITE_OK) rc = sqlite3_clear_bindings(statement);
        if (sqlite_result(db, rc, "reset insert") != ESP_OK) {
            result = ESP_FAIL;
            break;
        }
    }

    rc = sqlite3_finalize(statement);
    if (result == ESP_OK && sqlite_result(db, rc, "finalize insert") != ESP_OK) {
        result = ESP_FAIL;
    }
    if (result != ESP_OK) {
        execute_sql(db, "ROLLBACK");
        return result;
    }
    return execute_sql(db, "COMMIT");
}

static esp_err_t insert_telemetry(sqlite3 *db)
{
    static const struct {
        const char *inventory_name;
        const char *device_id;
        int64_t captured_at;
        const char *json;
        uint8_t packet_seed;
    } rows[] = {
        {
            "sensor", "esp32-lab-01", INT64_C(1770000001),
            "{\"sensor\":\"temperature\",\"value\":23.75,\"unit\":\"C\","
            "\"tags\":[\"lab\",\"critical\"],"
            "\"calibration\":{\"offset\":-0.25}}", 0x10
        },
        {
            "sensor", "esp32-lab-01", INT64_C(1770000061),
            "{\"sensor\":\"pressure\",\"value\":101.42,\"unit\":\"kPa\","
            "\"tags\":[\"lab\",\"environment\"],"
            "\"calibration\":{\"offset\":0.02}}", 0x40
        },
        {
            "display", "esp32-panel-02", INT64_C(1770000121),
            "{\"sensor\":\"brightness\",\"value\":78,\"unit\":\"percent\","
            "\"tags\":[\"panel\",\"ui\"],"
            "\"mode\":{\"automatic\":true,\"profile\":\"day\"}}", 0x80
        },
    };

    if (execute_sql(db, "BEGIN IMMEDIATE") != ESP_OK) return ESP_FAIL;
    sqlite3_stmt *statement = NULL;
    int rc = sqlite3_prepare_v2(db,
        "INSERT INTO telemetry(inventory_id, device_id, captured_at, payload_json, raw_packet) "
        "SELECT id, ?1, ?2, ?3, ?4 FROM inventory WHERE name=?5",
        -1, &statement, NULL);
    if (sqlite_result(db, rc, "prepare telemetry insert") != ESP_OK) {
        execute_sql(db, "ROLLBACK");
        return ESP_FAIL;
    }

    esp_err_t result = ESP_OK;
    for (size_t row = 0; row < sizeof(rows) / sizeof(rows[0]); ++row) {
        uint8_t packet[48];
        for (size_t i = 0; i < sizeof(packet); ++i) {
            packet[i] = (uint8_t)(rows[row].packet_seed + i);
        }
        rc = sqlite3_bind_text(statement, 1, rows[row].device_id, -1, SQLITE_STATIC);
        if (rc == SQLITE_OK) rc = sqlite3_bind_int64(statement, 2, rows[row].captured_at);
        if (rc == SQLITE_OK) rc = sqlite3_bind_text(statement, 3, rows[row].json, -1, SQLITE_STATIC);
        if (rc == SQLITE_OK) rc = sqlite3_bind_blob(statement, 4, packet,
                                                     sizeof(packet), SQLITE_TRANSIENT);
        if (rc == SQLITE_OK) rc = sqlite3_bind_text(statement, 5,
                                                     rows[row].inventory_name,
                                                     -1, SQLITE_STATIC);
        if (rc == SQLITE_OK) rc = sqlite3_step(statement);
        if (rc != SQLITE_DONE || sqlite3_changes(db) != 1) {
            sqlite_result(db, rc, "insert telemetry");
            result = ESP_FAIL;
            break;
        }
        rc = sqlite3_reset(statement);
        if (rc == SQLITE_OK) rc = sqlite3_clear_bindings(statement);
        if (sqlite_result(db, rc, "reset telemetry insert") != ESP_OK) {
            result = ESP_FAIL;
            break;
        }
    }
    int finalize_rc = sqlite3_finalize(statement);
    if (result == ESP_OK) {
        result = sqlite_result(db, finalize_rc, "finalize telemetry insert");
    }
    if (result != ESP_OK) {
        execute_sql(db, "ROLLBACK");
        return result;
    }
    return execute_sql(db, "COMMIT");
}

static esp_err_t run_json_queries(sqlite3 *db)
{
    sqlite3_stmt *statement = NULL;
    int rc = sqlite3_prepare_v2(db,
        "WITH decoded AS ("
        " SELECT t.device_id, i.name AS component,"
        " json_extract(t.payload_json, '$.sensor') AS sensor,"
        " CAST(json_extract(t.payload_json, '$.value') AS REAL) AS value,"
        " json_extract(t.payload_json, '$.unit') AS unit,"
        " json_array_length(t.payload_json, '$.tags') AS tag_count,"
        " length(t.raw_packet) AS packet_bytes,"
        " hex(substr(t.raw_packet, 1, 8)) AS packet_prefix"
        " FROM telemetry AS t JOIN inventory AS i ON i.id=t.inventory_id"
        ")"
        "SELECT device_id, component, sensor, value, unit, tag_count,"
        " packet_bytes, packet_prefix,"
        " round(avg(value) OVER (PARTITION BY component), 2)"
        " FROM decoded ORDER BY device_id, sensor",
        -1, &statement, NULL);
    if (sqlite_result(db, rc, "prepare JSON/BLOB report") != ESP_OK) {
        return ESP_FAIL;
    }
    ESP_LOGI(TAG, "decoded JSON and BLOB report:");
    while ((rc = sqlite3_step(statement)) == SQLITE_ROW) {
        ESP_LOGI(TAG,
            "  device=%s component=%s sensor=%s value=%.2f %s tags=%d "
            "packet=%d bytes prefix=%s component_avg=%.2f",
            (const char *)sqlite3_column_text(statement, 0),
            (const char *)sqlite3_column_text(statement, 1),
            (const char *)sqlite3_column_text(statement, 2),
            sqlite3_column_double(statement, 3),
            (const char *)sqlite3_column_text(statement, 4),
            sqlite3_column_int(statement, 5),
            sqlite3_column_int(statement, 6),
            (const char *)sqlite3_column_text(statement, 7),
            sqlite3_column_double(statement, 8));
    }
    esp_err_t result = sqlite_result(db, rc, "execute JSON/BLOB report");
    int finalize_rc = sqlite3_finalize(statement);
    if (result == ESP_OK) result = sqlite_result(db, finalize_rc, "finalize JSON/BLOB report");
    if (result != ESP_OK) return result;

    rc = sqlite3_prepare_v2(db,
        "SELECT t.device_id, tag.value "
        "FROM telemetry AS t, json_each(t.payload_json, '$.tags') AS tag "
        "ORDER BY t.device_id, tag.value",
        -1, &statement, NULL);
    if (sqlite_result(db, rc, "prepare json_each") != ESP_OK) return ESP_FAIL;
    ESP_LOGI(TAG, "expanded JSON tags:");
    while ((rc = sqlite3_step(statement)) == SQLITE_ROW) {
        ESP_LOGI(TAG, "  device=%s tag=%s",
                 (const char *)sqlite3_column_text(statement, 0),
                 (const char *)sqlite3_column_text(statement, 1));
    }
    result = sqlite_result(db, rc, "execute json_each");
    finalize_rc = sqlite3_finalize(statement);
    if (result == ESP_OK) result = sqlite_result(db, finalize_rc, "finalize json_each");
    if (result != ESP_OK) return result;

    rc = sqlite3_prepare_v2(db,
        "SELECT json_group_array(json_object("
        " 'device', device_id,"
        " 'sensor', json_extract(payload_json, '$.sensor'),"
        " 'value', json_extract(payload_json, '$.value'),"
        " 'packet_bytes', length(raw_packet)))"
        " FROM telemetry",
        -1, &statement, NULL);
    if (sqlite_result(db, rc, "prepare JSON aggregate") != ESP_OK) return ESP_FAIL;
    rc = sqlite3_step(statement);
    if (rc == SQLITE_ROW) {
        ESP_LOGI(TAG, "JSON aggregate: %s",
                 (const char *)sqlite3_column_text(statement, 0));
    } else {
        sqlite_result(db, rc, "execute JSON aggregate");
        sqlite3_finalize(statement);
        return ESP_FAIL;
    }
    finalize_rc = sqlite3_finalize(statement);
    if (sqlite_result(db, finalize_rc, "finalize JSON aggregate") != ESP_OK) {
        return ESP_FAIL;
    }

    if (execute_sql(db,
        "UPDATE telemetry SET payload_json=json_set("
        "payload_json, '$.processed', json('true'),"
        "'$.calibration.applied_at', captured_at) "
        "WHERE json_extract(payload_json, '$.sensor')='temperature'") != ESP_OK ||
        sqlite3_changes(db) != 1) {
        ESP_LOGE(TAG, "JSON update did not change exactly one row");
        return ESP_FAIL;
    }
    if (execute_sql(db,
        "DELETE FROM telemetry "
        "WHERE CAST(json_extract(payload_json, '$.value') AS REAL) > 100") != ESP_OK ||
        sqlite3_changes(db) != 1) {
        ESP_LOGE(TAG, "JSON predicate delete did not change exactly one row");
        return ESP_FAIL;
    }
    return ESP_OK;
}

static esp_err_t run_incremental_blob_demo(sqlite3 *db)
{
    enum { BLOB_SIZE = 512, BLOB_CHUNK = 64 };
    uint8_t expected[BLOB_SIZE];
    uint8_t actual[BLOB_SIZE];
    for (size_t i = 0; i < sizeof(expected); ++i) {
        expected[i] = (uint8_t)((i * 37u + 11u) & 0xffu);
    }

    if (execute_sql(db, "BEGIN IMMEDIATE") != ESP_OK) return ESP_FAIL;
    sqlite3_stmt *statement = NULL;
    int rc = sqlite3_prepare_v2(db,
        "INSERT INTO binary_assets(name, mime_type, metadata_json, data) "
        "VALUES(?1, ?2, ?3, zeroblob(?4))", -1, &statement, NULL);
    if (sqlite_result(db, rc, "prepare zeroblob insert") != ESP_OK) {
        execute_sql(db, "ROLLBACK");
        return ESP_FAIL;
    }
    sqlite3_bind_text(statement, 1, "calibration-table", -1, SQLITE_STATIC);
    sqlite3_bind_text(statement, 2, "application/octet-stream", -1, SQLITE_STATIC);
    sqlite3_bind_text(statement, 3,
        "{\"encoding\":\"u8\",\"samples\":512,\"version\":1}",
        -1, SQLITE_STATIC);
    sqlite3_bind_int(statement, 4, BLOB_SIZE);
    rc = sqlite3_step(statement);
    esp_err_t result = sqlite_result(db, rc, "insert zeroblob");
    int finalize_rc = sqlite3_finalize(statement);
    if (result == ESP_OK) result = sqlite_result(db, finalize_rc, "finalize zeroblob insert");
    if (result != ESP_OK) {
        execute_sql(db, "ROLLBACK");
        return result;
    }

    sqlite3_int64 rowid = sqlite3_last_insert_rowid(db);
    sqlite3_blob *blob = NULL;
    rc = sqlite3_blob_open(db, "main", "binary_assets", "data", rowid, 1, &blob);
    if (sqlite_result(db, rc, "open writable BLOB") != ESP_OK) {
        execute_sql(db, "ROLLBACK");
        return ESP_FAIL;
    }
    for (int offset = 0; offset < BLOB_SIZE; offset += BLOB_CHUNK) {
        rc = sqlite3_blob_write(blob, expected + offset, BLOB_CHUNK, offset);
        if (rc != SQLITE_OK) break;
    }
    int close_rc = sqlite3_blob_close(blob);
    if (rc != SQLITE_OK || close_rc != SQLITE_OK) {
        sqlite_result(db, rc != SQLITE_OK ? rc : close_rc, "write incremental BLOB");
        execute_sql(db, "ROLLBACK");
        return ESP_FAIL;
    }
    if (execute_sql(db, "COMMIT") != ESP_OK) return ESP_FAIL;

    blob = NULL;
    rc = sqlite3_blob_open(db, "main", "binary_assets", "data", rowid, 0, &blob);
    if (sqlite_result(db, rc, "open readable BLOB") != ESP_OK) return ESP_FAIL;
    if (sqlite3_blob_bytes(blob) != BLOB_SIZE) {
        ESP_LOGE(TAG, "unexpected BLOB size: %d", sqlite3_blob_bytes(blob));
        sqlite3_blob_close(blob);
        return ESP_FAIL;
    }
    rc = sqlite3_blob_read(blob, actual, sizeof(actual), 0);
    close_rc = sqlite3_blob_close(blob);
    if (rc != SQLITE_OK || close_rc != SQLITE_OK ||
        memcmp(actual, expected, sizeof(actual)) != 0) {
        ESP_LOGE(TAG, "incremental BLOB verification failed");
        return ESP_FAIL;
    }

    rc = sqlite3_prepare_v2(db,
        "SELECT name, json_extract(metadata_json, '$.samples'), length(data),"
        " hex(substr(data, 1, 16)) FROM binary_assets WHERE id=?1",
        -1, &statement, NULL);
    if (sqlite_result(db, rc, "prepare BLOB metadata query") != ESP_OK) return ESP_FAIL;
    sqlite3_bind_int64(statement, 1, rowid);
    rc = sqlite3_step(statement);
    if (rc == SQLITE_ROW) {
        ESP_LOGI(TAG, "BLOB asset=%s samples=%d bytes=%d prefix=%s",
                 (const char *)sqlite3_column_text(statement, 0),
                 sqlite3_column_int(statement, 1),
                 sqlite3_column_int(statement, 2),
                 (const char *)sqlite3_column_text(statement, 3));
    } else {
        sqlite_result(db, rc, "read BLOB metadata");
        sqlite3_finalize(statement);
        return ESP_FAIL;
    }
    finalize_rc = sqlite3_finalize(statement);
    return sqlite_result(db, finalize_rc, "finalize BLOB metadata query");
}

typedef struct {
    const char *name;
    uint64_t calls;
    uint64_t total_us;
    uint64_t minimum_us;
    uint64_t maximum_us;
} api_timing_t;

static void api_timing_add(api_timing_t *timing, int64_t started_us)
{
    uint64_t elapsed = (uint64_t)(esp_timer_get_time() - started_us);
    timing->calls++;
    timing->total_us += elapsed;
    if (timing->calls == 1 || elapsed < timing->minimum_us) {
        timing->minimum_us = elapsed;
    }
    if (elapsed > timing->maximum_us) timing->maximum_us = elapsed;
}

static void api_timing_log(const api_timing_t *timing)
{
    uint64_t average = timing->calls ? timing->total_us / timing->calls : 0;
    ESP_LOGI(TAG,
        "SQLITE_API %-22s calls=%llu total=%llu us avg=%llu us min=%llu us max=%llu us",
        timing->name, (unsigned long long)timing->calls,
        (unsigned long long)timing->total_us, (unsigned long long)average,
        (unsigned long long)timing->minimum_us,
        (unsigned long long)timing->maximum_us);
}

#define TIME_SQLITE_CALL(timing, result, expression) do { \
    int64_t api_started_us = esp_timer_get_time(); \
    (result) = (expression); \
    api_timing_add(&(timing), api_started_us); \
} while (0)

static esp_err_t run_sqlite_api_benchmark(sqlite3 *db,
                                          const char *backend_name)
{
    api_timing_t prepare_insert = { .name = "sqlite3_prepare(insert)" };
    api_timing_t begin = { .name = "sqlite3_exec(BEGIN)" };
    api_timing_t bind_int = { .name = "sqlite3_bind_int" };
    api_timing_t bind_text = { .name = "sqlite3_bind_text" };
    api_timing_t bind_blob = { .name = "sqlite3_bind_blob" };
    api_timing_t insert_step = { .name = "sqlite3_step(insert)" };
    api_timing_t reset = { .name = "sqlite3_reset" };
    api_timing_t commit = { .name = "sqlite3_exec(COMMIT)" };
    api_timing_t prepare_select = { .name = "sqlite3_prepare(select)" };
    api_timing_t select_step = { .name = "sqlite3_step(select)" };
    api_timing_t prepare_update = { .name = "sqlite3_prepare(update)" };
    api_timing_t update_step = { .name = "sqlite3_step(update)" };
    api_timing_t prepare_delete = { .name = "sqlite3_prepare(delete)" };
    api_timing_t delete_step = { .name = "sqlite3_step(delete)" };
    api_timing_t blob_open = { .name = "sqlite3_blob_open" };
    api_timing_t blob_write = { .name = "sqlite3_blob_write" };
    api_timing_t blob_read = { .name = "sqlite3_blob_read" };
    api_timing_t blob_close = { .name = "sqlite3_blob_close" };
    api_timing_t checkpoint = { .name = "sqlite3_exec(checkpoint)" };
    api_timing_t finalize = { .name = "sqlite3_finalize" };
    api_timing_t *all_timings[] = {
        &prepare_insert, &begin, &bind_int, &bind_text, &bind_blob,
        &insert_step, &reset, &commit, &prepare_select, &select_step,
        &prepare_update, &update_step, &prepare_delete, &delete_step,
        &blob_open, &blob_write, &blob_read, &blob_close, &checkpoint, &finalize,
    };

    if (execute_sql(db,
        "DROP TABLE IF EXISTS api_benchmark;"
        "CREATE TABLE api_benchmark("
        "id INTEGER PRIMARY KEY, sequence INTEGER NOT NULL UNIQUE,"
        "payload_json TEXT NOT NULL CHECK(json_valid(payload_json)),"
        "data BLOB NOT NULL)") != ESP_OK ||
        execute_sql(db, "PRAGMA wal_checkpoint(TRUNCATE)") != ESP_OK) {
        return ESP_FAIL;
    }

    ESP_LOGI(TAG, "========== direct SQLite API timing: %s =========",
             backend_name);
    sqlite3_stmt *statement = NULL;
    sqlite3_blob *blob = NULL;
    int rc = SQLITE_OK;
    uint8_t packet[64];
    memset(packet, 0xa5, sizeof(packet));
    const char *json =
        "{\"kind\":\"api-benchmark\",\"value\":42.5,"
        "\"tags\":[\"sqlite\",\"timing\"]}";

    TIME_SQLITE_CALL(prepare_insert, rc,
        sqlite3_prepare_v2(db,
            "INSERT INTO api_benchmark(sequence, payload_json, data) "
            "VALUES(?1, json_set(?2, '$.sequence', ?1), ?3)",
            -1, &statement, NULL));
    if (rc != SQLITE_OK) goto failed;

    TIME_SQLITE_CALL(begin, rc,
                     sqlite3_exec(db, "BEGIN IMMEDIATE", NULL, NULL, NULL));
    if (rc != SQLITE_OK) goto failed;
    for (int i = 0; i < API_BENCHMARK_SAMPLES; ++i) {
        TIME_SQLITE_CALL(bind_int, rc, sqlite3_bind_int(statement, 1, i));
        if (rc == SQLITE_OK) {
            TIME_SQLITE_CALL(bind_text, rc,
                sqlite3_bind_text(statement, 2, json, -1, SQLITE_STATIC));
        }
        if (rc == SQLITE_OK) {
            TIME_SQLITE_CALL(bind_blob, rc,
                sqlite3_bind_blob(statement, 3, packet, sizeof(packet),
                                  SQLITE_STATIC));
        }
        if (rc == SQLITE_OK) {
            TIME_SQLITE_CALL(insert_step, rc, sqlite3_step(statement));
        }
        if (rc != SQLITE_DONE) goto failed;
        TIME_SQLITE_CALL(reset, rc, sqlite3_reset(statement));
        if (rc != SQLITE_OK || sqlite3_clear_bindings(statement) != SQLITE_OK) {
            goto failed;
        }
        vTaskDelay(1);
    }
    TIME_SQLITE_CALL(finalize, rc, sqlite3_finalize(statement));
    statement = NULL;
    if (rc != SQLITE_OK) goto failed;
    TIME_SQLITE_CALL(commit, rc,
                     sqlite3_exec(db, "COMMIT", NULL, NULL, NULL));
    if (rc != SQLITE_OK) goto failed;
    vTaskDelay(1);

    TIME_SQLITE_CALL(prepare_select, rc,
        sqlite3_prepare_v2(db,
            "SELECT json_extract(payload_json, '$.value'), length(data) "
            "FROM api_benchmark WHERE sequence=?1",
            -1, &statement, NULL));
    if (rc != SQLITE_OK) goto failed;
    for (int i = 0; i < API_BENCHMARK_SAMPLES; ++i) {
        if (sqlite3_bind_int(statement, 1, i) != SQLITE_OK) goto failed;
        TIME_SQLITE_CALL(select_step, rc, sqlite3_step(statement));
        if (rc != SQLITE_ROW || sqlite3_column_int(statement, 1) != sizeof(packet)) {
            goto failed;
        }
        TIME_SQLITE_CALL(reset, rc, sqlite3_reset(statement));
        if (rc != SQLITE_OK || sqlite3_clear_bindings(statement) != SQLITE_OK) {
            goto failed;
        }
        vTaskDelay(1);
    }
    TIME_SQLITE_CALL(finalize, rc, sqlite3_finalize(statement));
    statement = NULL;
    if (rc != SQLITE_OK) goto failed;

    TIME_SQLITE_CALL(prepare_update, rc,
        sqlite3_prepare_v2(db,
            "UPDATE api_benchmark SET payload_json=json_set("
            "payload_json, '$.updated', json('true')) WHERE sequence=?1",
            -1, &statement, NULL));
    if (rc != SQLITE_OK) goto failed;
    for (int i = 0; i < 2; ++i) {
        if (sqlite3_bind_int(statement, 1, i) != SQLITE_OK) goto failed;
        TIME_SQLITE_CALL(update_step, rc, sqlite3_step(statement));
        if (rc != SQLITE_DONE) goto failed;
        if (sqlite3_reset(statement) != SQLITE_OK ||
            sqlite3_clear_bindings(statement) != SQLITE_OK) goto failed;
        vTaskDelay(1);
    }
    TIME_SQLITE_CALL(finalize, rc, sqlite3_finalize(statement));
    statement = NULL;
    if (rc != SQLITE_OK) goto failed;

    TIME_SQLITE_CALL(prepare_delete, rc,
        sqlite3_prepare_v2(db,
            "DELETE FROM api_benchmark WHERE sequence=?1",
            -1, &statement, NULL));
    if (rc != SQLITE_OK || sqlite3_bind_int(statement, 1,
                                             API_BENCHMARK_SAMPLES - 1) != SQLITE_OK) {
        goto failed;
    }
    TIME_SQLITE_CALL(delete_step, rc, sqlite3_step(statement));
    if (rc != SQLITE_DONE) goto failed;
    TIME_SQLITE_CALL(finalize, rc, sqlite3_finalize(statement));
    statement = NULL;
    if (rc != SQLITE_OK) goto failed;
    vTaskDelay(1);

    if (execute_sql(db,
        "INSERT INTO api_benchmark(sequence, payload_json, data) "
        "VALUES(1000, '{\"kind\":\"blob\"}', zeroblob(512))") != ESP_OK) {
        goto failed;
    }
    sqlite3_int64 blob_rowid = sqlite3_last_insert_rowid(db);
    uint8_t blob_data[API_BENCHMARK_BLOB_SIZE];
    for (size_t i = 0; i < sizeof(blob_data); ++i) blob_data[i] = (uint8_t)i;
    TIME_SQLITE_CALL(blob_open, rc,
        sqlite3_blob_open(db, "main", "api_benchmark", "data", blob_rowid,
                          1, &blob));
    if (rc != SQLITE_OK) goto failed;
    TIME_SQLITE_CALL(blob_write, rc,
                     sqlite3_blob_write(blob, blob_data, sizeof(blob_data), 0));
    if (rc != SQLITE_OK) goto failed;
    TIME_SQLITE_CALL(blob_close, rc, sqlite3_blob_close(blob));
    blob = NULL;
    if (rc != SQLITE_OK) goto failed;
    vTaskDelay(1);

    memset(blob_data, 0, sizeof(blob_data));
    TIME_SQLITE_CALL(blob_open, rc,
        sqlite3_blob_open(db, "main", "api_benchmark", "data", blob_rowid,
                          0, &blob));
    if (rc != SQLITE_OK) goto failed;
    TIME_SQLITE_CALL(blob_read, rc,
                     sqlite3_blob_read(blob, blob_data, sizeof(blob_data), 0));
    if (rc != SQLITE_OK) goto failed;
    TIME_SQLITE_CALL(blob_close, rc, sqlite3_blob_close(blob));
    blob = NULL;
    if (rc != SQLITE_OK) goto failed;

    TIME_SQLITE_CALL(checkpoint, rc,
        sqlite3_exec(db, "PRAGMA wal_checkpoint(TRUNCATE)", NULL, NULL, NULL));
    if (rc != SQLITE_OK) goto failed;

    for (size_t i = 0; i < sizeof(all_timings) / sizeof(all_timings[0]); ++i) {
        api_timing_log(all_timings[i]);
    }
    ESP_LOGI(TAG, "========== end SQLite API timing: %s ============",
             backend_name);
    return ESP_OK;

failed:
    ESP_LOGE(TAG, "SQLite API benchmark failed: rc=%d message=%s",
             rc, sqlite3_errmsg(db));
    if (blob) sqlite3_blob_close(blob);
    if (statement) sqlite3_finalize(statement);
    sqlite3_exec(db, "ROLLBACK", NULL, NULL, NULL);
    return ESP_FAIL;
}

#undef TIME_SQLITE_CALL

static esp_err_t print_inventory(sqlite3 *db, const char *heading)
{
    sqlite3_stmt *statement = NULL;
    int rc = sqlite3_prepare_v2(db,
        "SELECT id, name, quantity, price_cents FROM inventory ORDER BY id",
        -1, &statement, NULL);
    if (sqlite_result(db, rc, "prepare select") != ESP_OK) return ESP_FAIL;

    ESP_LOGI(TAG, "%s", heading);
    while ((rc = sqlite3_step(statement)) == SQLITE_ROW) {
        ESP_LOGI(TAG, "  id=%" PRId64 " name=%s quantity=%d price=$%d.%02d",
                 (int64_t)sqlite3_column_int64(statement, 0),
                 sqlite3_column_text(statement, 1),
                 sqlite3_column_int(statement, 2),
                 sqlite3_column_int(statement, 3) / 100,
                 sqlite3_column_int(statement, 3) % 100);
    }
    esp_err_t result = sqlite_result(db, rc, "read inventory");
    int finalize_rc = sqlite3_finalize(statement);
    if (result == ESP_OK) {
        result = sqlite_result(db, finalize_rc, "finalize select");
    }
    return result;
}

static esp_err_t update_inventory(sqlite3 *db)
{
    sqlite3_stmt *statement = NULL;
    int rc = sqlite3_prepare_v2(db,
        "UPDATE inventory SET quantity=quantity+?1, price_cents=?2 WHERE name=?3",
        -1, &statement, NULL);
    if (sqlite_result(db, rc, "prepare update") != ESP_OK) return ESP_FAIL;

    sqlite3_bind_int(statement, 1, 3);
    sqlite3_bind_int(statement, 2, 1499);
    sqlite3_bind_text(statement, 3, "sensor", -1, SQLITE_STATIC);
    rc = sqlite3_step(statement);
    esp_err_t result = sqlite_result(db, rc, "update sensor");
    int finalize_rc = sqlite3_finalize(statement);
    if (result == ESP_OK) result = sqlite_result(db, finalize_rc, "finalize update");
    if (result == ESP_OK && sqlite3_changes(db) != 1) {
        ESP_LOGE(TAG, "update changed %d rows instead of one", sqlite3_changes(db));
        return ESP_FAIL;
    }
    return result;
}

static esp_err_t delete_inventory_row(sqlite3 *db)
{
    sqlite3_stmt *statement = NULL;
    int rc = sqlite3_prepare_v2(db,
        "DELETE FROM inventory WHERE name=?1", -1, &statement, NULL);
    if (sqlite_result(db, rc, "prepare delete") != ESP_OK) return ESP_FAIL;

    sqlite3_bind_text(statement, 1, "relay", -1, SQLITE_STATIC);
    rc = sqlite3_step(statement);
    esp_err_t result = sqlite_result(db, rc, "delete relay");
    int finalize_rc = sqlite3_finalize(statement);
    if (result == ESP_OK) result = sqlite_result(db, finalize_rc, "finalize delete");
    if (result == ESP_OK && sqlite3_changes(db) != 1) {
        ESP_LOGE(TAG, "delete changed %d rows instead of one", sqlite3_changes(db));
        return ESP_FAIL;
    }
    return result;
}

static esp_err_t verify_database(sqlite3 *db)
{
    sqlite3_stmt *statement = NULL;
    int rc = sqlite3_prepare_v2(db, "PRAGMA integrity_check", -1,
                                &statement, NULL);
    if (sqlite_result(db, rc, "prepare integrity_check") != ESP_OK) {
        return ESP_FAIL;
    }
    rc = sqlite3_step(statement);
    const unsigned char *answer = rc == SQLITE_ROW
        ? sqlite3_column_text(statement, 0) : NULL;
    bool valid = answer && strcmp((const char *)answer, "ok") == 0;
    ESP_LOGI(TAG, "integrity_check: %s", answer ? (const char *)answer : "failed");
    int finalize_rc = sqlite3_finalize(statement);
    if (!valid || sqlite_result(db, finalize_rc, "finalize integrity_check") != ESP_OK) {
        return ESP_FAIL;
    }
    return execute_sql(db, "PRAGMA wal_checkpoint(TRUNCATE)");
}

static esp_err_t run_database_workload(const char *database_path,
                                       const char *backend_name)
{
    esp_err_t result = ESP_FAIL;
    sqlite3 *db = NULL;
    sqlite3 *reader = NULL;

    ESP_LOGI(TAG, "========== %s demo and benchmark ==========", backend_name);
    int rc = sqlite3_open(database_path, &db);
    if (sqlite_result(db, rc, "open database") != ESP_OK) goto cleanup;
    sqlite3_busy_timeout(db, 3000);

    /* Initialize page 1 in rollback mode before changing a brand-new FATFS
     * database to WAL. Both backends use this exact sequence. */
    if (create_schema(db) != ESP_OK ||
        enable_wal(db) != ESP_OK ||
        execute_sql(db,
            "DELETE FROM telemetry;"
            "DELETE FROM binary_assets;"
            "DELETE FROM inventory") != ESP_OK ||
        insert_demo_rows(db) != ESP_OK ||
        insert_telemetry(db) != ESP_OK ||
        run_json_queries(db) != ESP_OK ||
        run_incremental_blob_demo(db) != ESP_OK ||
        print_inventory(db, "inventory after CREATE/INSERT:") != ESP_OK ||
        update_inventory(db) != ESP_OK ||
        delete_inventory_row(db) != ESP_OK ||
        print_inventory(db, "inventory after UPDATE/DELETE:") != ESP_OK ||
        run_sqlite_api_benchmark(db, backend_name) != ESP_OK) {
        goto cleanup;
    }

    rc = sqlite3_open(database_path, &reader);
    if (sqlite_result(reader, rc, "open second connection") != ESP_OK) {
        goto cleanup;
    }
    sqlite3_busy_timeout(reader, 3000);
    if (execute_sql(reader, "PRAGMA locking_mode=NORMAL") != ESP_OK ||
        print_inventory(reader,
                        "same data through a second connection:") != ESP_OK) {
        goto cleanup;
    }

    rc = sqlite3_close(reader);
    if (rc != SQLITE_OK) {
        ESP_LOGE(TAG, "failed to close second connection: rc=%d", rc);
        goto cleanup;
    }
    reader = NULL;
    if (verify_database(db) != ESP_OK) goto cleanup;
    result = ESP_OK;

cleanup:
    if (reader && sqlite3_close(reader) != SQLITE_OK) result = ESP_FAIL;
    if (db && sqlite3_close(db) != SQLITE_OK) result = ESP_FAIL;
    ESP_LOGI(TAG, "========== %s %s ==========", backend_name,
             result == ESP_OK ? "completed" : "failed");
    return result;
}

static esp_err_t release_bdl(esp_blockdev_handle_t *handle, const char *name)
{
    if (!handle || *handle == ESP_BLOCKDEV_HANDLE_INVALID) return ESP_OK;
    if (!(*handle)->ops || !(*handle)->ops->release) return ESP_ERR_INVALID_ARG;
    esp_err_t result = (*handle)->ops->release(*handle);
    if (result != ESP_OK) {
        ESP_LOGE(TAG, "%s release failed: %s", name, esp_err_to_name(result));
        return result;
    }
    *handle = ESP_BLOCKDEV_HANDLE_INVALID;
    return ESP_OK;
}

static esp_err_t run_espbdl_demo(void)
{
    esp_err_t result = ESP_FAIL;
    esp_blockdev_handle_t partition_bdl = ESP_BLOCKDEV_HANDLE_INVALID;
    esp_blockdev_handle_t wl_bdl = ESP_BLOCKDEV_HANDLE_INVALID;
    bool sqlite_port_initialized = false;

    const esp_partition_t *partition = esp_partition_find_first(
        ESP_PARTITION_TYPE_DATA, ESP_PARTITION_SUBTYPE_ANY,
        SQLITE_PARTITION_LABEL);
    if (!partition) {
        ESP_LOGE(TAG, "data partition '%s' was not found", SQLITE_PARTITION_LABEL);
        goto cleanup;
    }
    ESP_LOGI(TAG, "partition '%s': address=0x%08" PRIx32 ", size=%" PRIu32,
             partition->label, partition->address, partition->size);

    result = esp_partition_ptr_get_blockdev(partition, &partition_bdl);
    if (result != ESP_OK) {
        ESP_LOGE(TAG, "esp_partition_ptr_get_blockdev failed: %s",
                 esp_err_to_name(result));
        goto cleanup;
    }
    result = wl_get_blockdev(partition_bdl, &wl_bdl);
    if (result != ESP_OK) {
        ESP_LOGE(TAG, "wl_get_blockdev failed: %s", esp_err_to_name(result));
        goto cleanup;
    }
    result = sqlite_espbdl_init(&wl_bdl);
    if (result != ESP_OK) {
        ESP_LOGE(TAG, "sqlite_espbdl_init failed: %s", esp_err_to_name(result));
        goto cleanup;
    }
    sqlite_port_initialized = true;

    uint64_t database_capacity = 0;
    uint64_t wal_capacity = 0;
    result = sqlite_espbdl_get_capacity(&database_capacity, &wal_capacity);
    if (result != ESP_OK) goto cleanup;
    ESP_LOGI(TAG, "SQLite capacities: database=%llu, WAL=%llu bytes",
             (unsigned long long)database_capacity,
             (unsigned long long)wal_capacity);

    result = run_database_workload(SQLITE_DATABASE_NAME, "ESP-BDL");

cleanup:
    bool release_partition_bdl = true;
    if (sqlite_port_initialized) {
        esp_err_t deinit_result = sqlite_espbdl_deinit();
        if (deinit_result != ESP_OK) {
            ESP_LOGE(TAG, "sqlite_espbdl_deinit failed: %s",
                     esp_err_to_name(deinit_result));
            result = deinit_result;
            release_partition_bdl = false;
        }
    } else if (wl_bdl != ESP_BLOCKDEV_HANDLE_INVALID) {
        esp_err_t release_result = release_bdl(&wl_bdl, "WL BDL");
        if (release_result != ESP_OK) {
            result = release_result;
            release_partition_bdl = false;
        }
    }
    if (release_partition_bdl) {
        esp_err_t release_result = release_bdl(&partition_bdl, "partition BDL");
        if (release_result != ESP_OK && result == ESP_OK) result = release_result;
    }
    return result;
}

static esp_err_t run_fatfs_demo(void)
{
    esp_err_t result = ESP_FAIL;
    esp_blockdev_handle_t partition_bdl = ESP_BLOCKDEV_HANDLE_INVALID;
    esp_blockdev_handle_t wl_bdl = ESP_BLOCKDEV_HANDLE_INVALID;
    bool fatfs_mounted = false;
    bool sqlite_port_initialized = false;

    const esp_partition_t *partition = esp_partition_find_first(
        ESP_PARTITION_TYPE_DATA, ESP_PARTITION_SUBTYPE_ANY,
        SQLITE_PARTITION_LABEL);
    if (!partition) {
        ESP_LOGE(TAG, "data partition '%s' was not found", SQLITE_PARTITION_LABEL);
        goto cleanup;
    }

    result = esp_partition_ptr_get_blockdev(partition, &partition_bdl);
    if (result != ESP_OK) {
        ESP_LOGE(TAG, "partition BDL creation for FATFS failed: %s",
                 esp_err_to_name(result));
        goto cleanup;
    }
    result = wl_get_blockdev(partition_bdl, &wl_bdl);
    if (result != ESP_OK) {
        ESP_LOGE(TAG, "WL BDL creation for FATFS failed: %s",
                 esp_err_to_name(result));
        goto cleanup;
    }

    const esp_vfs_fat_mount_config_t mount_config = {
        .format_if_mount_failed = true,
        .max_files = 8,
        .allocation_unit_size = 0,
    };
    ESP_LOGI(TAG,
             "mounting '%s' at %s; the raw-BDL contents will be formatted as FATFS",
             SQLITE_PARTITION_LABEL, FATFS_MOUNT_PATH);
    result = esp_vfs_fat_bdl_mount(FATFS_MOUNT_PATH, wl_bdl, &mount_config);
    if (result != ESP_OK) {
        ESP_LOGE(TAG, "FATFS format/mount failed: %s", esp_err_to_name(result));
        goto cleanup;
    }
    fatfs_mounted = true;

    uint64_t total_bytes = 0;
    uint64_t free_bytes = 0;
    result = esp_vfs_fat_info(FATFS_MOUNT_PATH, &total_bytes, &free_bytes);
    if (result != ESP_OK) {
        ESP_LOGE(TAG, "FATFS capacity query failed: %s", esp_err_to_name(result));
        goto cleanup;
    }
    ESP_LOGI(TAG, "FATFS capacity: total=%llu free=%llu bytes",
             (unsigned long long)total_bytes, (unsigned long long)free_bytes);

    result = sqlite_fatfs_init();
    if (result != ESP_OK) {
        ESP_LOGE(TAG, "sqlite_fatfs_init failed: %s", esp_err_to_name(result));
        goto cleanup;
    }
    sqlite_port_initialized = true;
    result = run_database_workload(FATFS_DATABASE_PATH, "FATFS");

cleanup:
    bool can_unmount = true;
    if (sqlite_port_initialized) {
        esp_err_t deinit_result = sqlite_fatfs_deinit();
        if (deinit_result != ESP_OK) {
            ESP_LOGE(TAG, "sqlite_fatfs_deinit failed: %s",
                     esp_err_to_name(deinit_result));
            result = deinit_result;
            can_unmount = false;
        }
    }
    if (fatfs_mounted && can_unmount) {
        esp_err_t unmount_result = esp_vfs_fat_bdl_unmount(FATFS_MOUNT_PATH,
                                                            wl_bdl);
        if (unmount_result != ESP_OK) {
            ESP_LOGE(TAG, "FATFS unmount failed: %s",
                     esp_err_to_name(unmount_result));
            result = unmount_result;
            can_unmount = false;
        }
    }
    if (can_unmount) {
        esp_err_t release_result = release_bdl(&wl_bdl, "FATFS WL BDL");
        if (release_result != ESP_OK) {
            result = release_result;
            can_unmount = false;
        }
    }
    if (can_unmount) {
        esp_err_t release_result = release_bdl(&partition_bdl,
                                               "FATFS partition BDL");
        if (release_result != ESP_OK) result = release_result;
    }
    return result;
}

void app_main(void)
{
    esp_err_t bdl_result = run_espbdl_demo();
    if (bdl_result != ESP_OK) {
        ESP_LOGE(TAG, "SQLite ESP-BDL demo failed: %s",
                 esp_err_to_name(bdl_result));
        return;
    }
    ESP_LOGI(TAG, "SQLite ESP-BDL demo completed; starting FATFS comparison");

    esp_err_t fatfs_result = run_fatfs_demo();
    if (fatfs_result != ESP_OK) {
        ESP_LOGE(TAG, "SQLite FATFS demo failed: %s",
                 esp_err_to_name(fatfs_result));
        return;
    }
    ESP_LOGI(TAG, "SQLite ESP-BDL and FATFS comparison completed successfully");
}
