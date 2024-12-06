import aiohttp
import aiofiles
import asyncio
import os
import json
import re
import logging
from aiohttp import ClientSession
from asyncio import Semaphore
from logging.handlers import RotatingFileHandler

# Configure logging
log_folder = "./logs"
log_file = os.path.join(log_folder, "script.log")

# Ensure the log folder exists
if not os.path.exists(log_folder):
    os.makedirs(log_folder)

# Set up logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Create file handler for logging to a file
file_handler = RotatingFileHandler(log_file, maxBytes=5_000_000, backupCount=3)
file_handler.setLevel(logging.INFO)

# Create console handler for logging to the console
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)

# Set log format
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

# Add handlers to the logger
logger.addHandler(file_handler)
logger.addHandler(console_handler)


async def retry_with_backoff(coro, max_delay=300, initial_delay=1, factor=2, retry_exceptions=(aiohttp.ClientError,)):
    """
    Retry a coroutine indefinitely with exponential backoff on specified exceptions.

    Args:
        coro: The coroutine to retry.
        max_delay: Maximum delay between retries.
        initial_delay: Initial delay in seconds.
        factor: Multiplier for exponential backoff.
        retry_exceptions: Tuple of exceptions to trigger a retry.

    Returns:
        The result of the coroutine.
    """
    delay = initial_delay
    while True:
        try:
            return await coro()
        except retry_exceptions as e:
            logger.warning(f"Operation failed with error: {e}. Retrying in {delay} seconds...")
            await asyncio.sleep(delay)
            delay = min(delay * factor, max_delay)


async def fetch_json(session, url, method="GET", **kwargs):
    """
    Perform an async HTTP request and return the JSON response with retries.
    """
    async def request():
        async with session.request(method, url, **kwargs) as response:
            response.raise_for_status()
            return await response.json()
    return await retry_with_backoff(request)


async def get_indices_by_pattern(session, base_url, pattern):
    """
    Retrieve indices that match a specific pattern.
    """
    url = f"{base_url}/_cat/indices?format=json"
    all_indices = await fetch_json(session, url)
    filtered_indices = [
        index["index"] for index in all_indices if re.match(pattern.replace("*", ".*"), index["index"])
    ]
    return filtered_indices


async def calculate_optimal_slices(session, base_url, index_name, max_slices=64):
    """
    Dynamically calculate the number of slices based on index metadata.
    """
    url_stats = f"{base_url}/{index_name}/_stats"
    url_count = f"{base_url}/{index_name}/_count"
    index_stats = await fetch_json(session, url_stats)
    doc_count = await fetch_json(session, url_count)

    shard_count = index_stats["_shards"]["total"]
    total_docs = doc_count["count"]

    # One slice per shard, plus additional slices for every 10M documents
    base_slices = shard_count
    scale_factor = max(1, total_docs // 10_000_000)
    return min(base_slices + scale_factor, max_slices)


async def initialize_scroll(session, base_url, index_name, slice_id, total_slices, page_size, fields_to_export):
    """
    Initializes a scroll context for a specific slice in Elasticsearch with retries.
    """
    url = f"{base_url}/{index_name}/_search?scroll=5m"
    query = {
        "slice": {"id": slice_id, "max": total_slices},
        "track_total_hits": True,
        "query": {"match_all": {}},
        "_source": fields_to_export,
        "size": page_size,
        "sort": ["_doc"],
    }

    async def init_scroll():
        response = await fetch_json(session, url, method="POST", json=query)
        return response["_scroll_id"], response["hits"]["hits"], response["hits"]["total"]["value"]

    return await retry_with_backoff(init_scroll)



async def scroll_results(session, base_url, scroll_id):
    """
    Fetches the next batch of results for a slice using the scroll API with retries.
    """
    url = f"{base_url}/_search/scroll"
    query = {"scroll": "5m", "scroll_id": scroll_id}

    async def fetch_scroll():
        response = await fetch_json(session, url, method="POST", json=query)
        return response["_scroll_id"], response["hits"]["hits"]

    return await retry_with_backoff(fetch_scroll)


async def delete_scroll(session, base_url, scroll_id):
    """
    Deletes a scroll context to free up resources.
    """
    url = f"{base_url}/_search/scroll"
    try:
        await session.delete(url, json={"scroll_id": scroll_id})
        logger.info(f"Successfully deleted scroll context: {scroll_id}")
    except aiohttp.ClientError as e:
        logger.warning(f"Failed to delete scroll context {scroll_id}: {e}")


async def get_index_shard_count(session, base_url, indices):
    """
    Fetch the total shard count for the specified indices.
    """
    total_shards = 0
    for index in indices:
        url = f"{base_url}/{index}/_stats"
        stats = await fetch_json(session, url)
        total_shards += stats["_shards"]["total"]
    return total_shards


async def process_slice(session, base_url, index_name, slice_id, total_slices, page_size, fields_to_export, output_folder):
    scroll_id, hits, _ = await initialize_scroll(session, base_url, index_name, slice_id, total_slices, page_size, fields_to_export)
    if not scroll_id:
        return 0

    try:
        # Process results as usual...
        fetched_records = 0
        buffer = []
        async with aiofiles.open(os.path.join(output_folder, f"{index_name}_{slice_id}.csv"), "w") as f:
            await f.write(",".join(fields_to_export) + "\n")  # Write header
            while hits:
                # Process hits...
                for hit in hits:
                    buffer.append(",".join([str(hit["_source"].get(field, "")) for field in fields_to_export]))
                    fetched_records += 1

                if len(buffer) >= 10000:
                    await f.write("\n".join(buffer) + "\n")
                    buffer.clear()

                scroll_id, hits = await scroll_results(session, base_url, scroll_id)

            if buffer:
                await f.write("\n".join(buffer) + "\n")

        logger.info(f"Slice {slice_id}: Processed {fetched_records} records.")
        return fetched_records
    finally:
        if scroll_id:
            await delete_scroll(session, base_url, scroll_id)


async def process_index(session, base_url, index_name, total_slices, page_size, fields_to_export, output_folder):
    """
    Processes all slices of an index and merges the results into a single CSV.
    """
    tasks = [
        process_slice(session, base_url, index_name, slice_id, total_slices, page_size, fields_to_export, output_folder)
        for slice_id in range(total_slices)
    ]
    results = await asyncio.gather(*tasks)
    total_records = sum(results)
    logger.info(f"Index {index_name}: Processed {total_records} records.")

    # Merge slice-specific files into a single CSV for the index
    await merge_slices(index_name, total_slices, fields_to_export, output_folder)

    return total_records



async def merge_slices(index_name, total_slices, fields_to_export, output_folder):
    """
    Merges slice-specific CSV files into a single CSV file for the index.
    """
    final_file = os.path.join(output_folder, f"{index_name}.csv")
    async with aiofiles.open(final_file, "w") as outfile:
        # Write the header to the final file
        await outfile.write(",".join(fields_to_export) + "\n")

        for slice_id in range(total_slices):
            slice_file = os.path.join(output_folder, f"{index_name}_{slice_id}.csv")
            async with aiofiles.open(slice_file, "r") as infile:
                # Skip the header line in each slice file
                await infile.readline()
                async for line in infile:
                    await outfile.write(line)

            # Delete the slice file after merging
            os.remove(slice_file)
            logger.info(f"Deleted slice file: {slice_file}")

    logger.info(f"Merged all slices into: {final_file}")



async def calculate_max_concurrent_tasks(session, base_url, indices, threads_per_node=10, node_count=3, safe_utilization=0.5):
    """
    Calculate the max concurrent tasks based on index shard count and cluster capacity.
    """
    total_shards = await get_index_shard_count(session, base_url, indices)
    safe_thread_count = int(threads_per_node * node_count * safe_utilization)
    return min(total_shards, safe_thread_count)



async def main(config_path):
    # Load configuration
    with open(config_path, "r") as f:
        config = json.load(f)

    base_url = config["Elasticsearch"]["host"]
    index_pattern = config["Elasticsearch"]["index_pattern"]
    page_size = config["Pagination"]["page_size"]
    output_folder = config["Output"]["folder"]
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    fields_to_export = config["Query"]["fields_to_export"]

    async with ClientSession() as session:
        # Get indices matching the pattern
        indices = await get_indices_by_pattern(session, base_url, index_pattern)
        if not indices:
            logger.error(f"No indices found for pattern: {index_pattern}")
            return

        logger.info(f"Found indices: {indices}")

        # Dynamically calculate max concurrent tasks
        max_concurrent_tasks = await calculate_max_concurrent_tasks(session, base_url, indices)
        sem = Semaphore(max_concurrent_tasks)

        async def limited_task(coro):
            async with sem:
                return await coro

        # Process each index concurrently
        tasks = [
            limited_task(
                process_index(
                    session,
                    base_url,
                    index,
                    await calculate_optimal_slices(session, base_url, index),
                    page_size,
                    fields_to_export,
                    output_folder
                )
            )
            for index in indices
        ]

        # Run all tasks and handle exceptions
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for index, result in zip(indices, results):
            if isinstance(result, Exception):
                logger.error(f"Error processing index {index}: {result}")
            else:
                logger.info(f"Successfully processed index {index}.")



if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: python script.py <config_path>")
        sys.exit(1)

    config_path = sys.argv[1]
    asyncio.run(main(config_path))
