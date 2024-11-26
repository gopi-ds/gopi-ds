| **Key Type**               | **Use Case**                   | **Strategy**                                                    | **Efficiency**    |
|----------------------------|---------------------------------|-----------------------------------------------------------------|-------------------|
| **Range-Based Shard Key**  | Range queries or batch deletes | Query by shard key range; delete chunk by chunk.                | High              |
| **Hashed Shard Key**       | Evenly distributed data        | Use hashed shard key; fetch hashed chunk ranges from config.chunks. | Medium            |
| **Non-Shard Indexed Field**| Secondary targeting            | Use indexed field to limit query scope; batch deletes.          | Medium            |
| **Non-Shard Non-Indexed**  | Rare, unavoidable queries      | Perform full collection scan or add temporary index; batch deletes if possible. | Low               |
| **No Shard Key Query**     | Edge cases or unplanned schema | Query broadcasts to all shards; avoid unless necessary.         | Low (Expensive)   |

# Rolling Drop Script Documentation

## Key Features

### 1. Configuration Reading
- Reads configuration from a JSON file.
- Configurations include:
    - Database names.
    - Connection URIs.
    - Logging details.
    - Deletion parameters such as `DRY_RUN` and batch size.

---

### 2. Logging Setup
- Initializes a logging mechanism for:
    - Informational messages.
    - Warnings.
    - Errors.
- Captures all key events for auditing and debugging purposes.

---

### 3. MongoDB Connections
- Establishes connections to:
    - Tenant database.
    - Config database.
    - Alcatraz (status tracking) database.

---

### 4. Retry Logic
- Provides a robust retry mechanism with:
    - Exponential backoff.
    - Infinite retry attempts for transient errors.

---

### 5. Shard Processing
#### `process_sharded_collection`
- Iterates through each shard in a sharded collection.
- Processes each chunk while:
    - Deleting documents in batches.
    - Tracking progress for resumability.

#### `delete_from_shard`
- Handles deletion at the shard level.
- Manages chunk-by-chunk document deletion.
- Updates status to allow resumable operations.

---

### 6. Unsharded Collection Processing
#### `process_unsharded_collection`
- Deletes all documents in unsharded collections using:
    - Full scan deletions.
    - Simulated deletions in `DRY_RUN` mode.

---

### 7. Chunk and Shard Metadata Handling
#### `all_chunk_info`
- Retrieves and organizes chunk metadata for sharded collections.
- Logs:
    - Chunk sizes (in MB).
    - Document counts.
    - Shard-wise consolidated totals.

#### `is_hashed_shard_key`
- Checks whether the shard key is hashed.

---

### 8. Status Tracking
#### `initialize_collection_status`
- Creates or updates the initial status for:
    - Sharded collections.
    - Unsharded collections.

#### `update_collection_status`
- Updates the progress of shard/collection processing, including:
    - Chunks processed.
    - Documents deleted.
    - Errors encountered.

#### `get_collection_status`
- Retrieves the current status for resumable operations.

---

### 9. Rebalancing Detection
#### `is_rebalancing`
- Detects active chunk migration (rebalancing) operations.

#### `wait_for_rebalancing_to_complete`
- Pauses deletion processes until rebalancing completes.

---

### 10. Main Process
#### `perform_rolling_drop_for_all_collections`
- Coordinates the processing of:
    - Sharded collections.
    - Unsharded collections.
- Skips completed shards/collections based on status tracking.

---

### 11. Execution Entry Point
- Logs:
    - Start and end of the rolling drop process.
    - Total execution time (hours, minutes, seconds).
    - Whether the operation was a dry run or actual deletion.

---

## Execution Flow

1. **Initialization**:
    - Logging is initialized.
    - Configuration is read.
    - Database connections are established.

2. **Categorization**:
    - Collections are categorized into:
        - **Sharded collections**.
        - **Unsharded collections**.

3. **Processing Sharded Collections**:
    - For each sharded collection:
        - Retrieve shard and chunk metadata using `all_chunk_info`.
        - Check if the shard key is hashed using `is_hashed_shard_key`.
        - For each shard:
            - Retrieve the current processing status.
            - Skip completed shards.
            - Delete documents chunk by chunk using `delete_from_shard`.

4. **Processing Unsharded Collections**:
    - For each unsharded collection:
        - Retrieve the current processing status.
        - Skip completed collections.
        - Perform a full scan deletion using `delete_with_full_scan`.

5. **Status Tracking**:
    - Status of each shard and collection is tracked, including:
        - Total documents deleted.
        - Chunks processed.
        - Errors encountered.

6. **Rebalancing Management**:
    - If chunk migrations are in progress:
        - Pause the deletion process.
        - Wait until rebalancing completes.

7. **Completion**:
    - Log:
        - Total execution time.
        - Summary of the rolling drop process.

---
