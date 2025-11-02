"""
Parallel data fetcher for Polygon API with configurable worker pool.

This module provides parallel downloading capabilities to maximize throughput
when fetching paginated data from Polygon.io API.
"""

import asyncio
from typing import List, Dict, Any, Optional, Callable
from concurrent.futures import ThreadPoolExecutor
import json


class ParallelFetcher:
    """Fetches paginated API data in parallel using multiple workers."""

    def __init__(self, num_workers: int = 5):
        """
        Initialize parallel fetcher.

        Args:
            num_workers: Number of parallel workers (default: 5)
        """
        self.num_workers = num_workers

    async def fetch_all_pages(
        self,
        fetch_func: Callable,
        max_workers: Optional[int] = None,
        batch_callback: Optional[Callable] = None,
    ) -> List[Dict[str, Any]]:
        """
        Fetch all pages in parallel using multiple workers.

        Strategy:
        1. Fetch first page to determine total count and pages needed
        2. Split remaining pages across workers
        3. Each worker fetches its assigned pages
        4. Combine all results in correct order (or stream via callback)

        Args:
            fetch_func: Function that fetches a single page
                       Should accept 'cursor' parameter and return (data, next_cursor)
            max_workers: Override number of workers (default: self.num_workers)
            batch_callback: Optional async callback(batch_num: int, data: List[Dict])
                          called for each page. If provided, data is NOT accumulated in memory.

        Returns:
            List of all data items from all pages (empty if batch_callback is used)

        Raises:
            asyncio.CancelledError: If interrupted, stops immediately and re-raises
        """
        workers = max_workers or self.num_workers

        try:
            # Fetch first page to understand pagination
            first_page_data, first_cursor = await self._fetch_page(fetch_func, None)

            # If callback provided, stream the first page
            if batch_callback:
                await batch_callback(0, first_page_data)

            if not first_cursor:
                # Only one page of data
                return [] if batch_callback else first_page_data

            # Fetch remaining pages in parallel
            remaining_results = await self._fetch_parallel_pages(
                fetch_func=fetch_func,
                first_cursor=first_cursor,
                workers=workers,
                batch_callback=batch_callback,
                batch_offset=1,  # First page is batch 0
            )

            if batch_callback:
                # Streaming mode - data was written via callbacks
                return []
            else:
                # Memory mode - accumulate and return
                all_results = first_page_data.copy()
                all_results.extend(remaining_results)
                return all_results

        except asyncio.CancelledError:
            # Interrupted - stop immediately and re-raise
            raise

    async def _fetch_page(
        self,
        fetch_func: Callable,
        cursor: Optional[str],
    ) -> tuple[List[Dict[str, Any]], Optional[str]]:
        """
        Fetch a single page.

        Args:
            fetch_func: Function to fetch page data
            cursor: Pagination cursor (None for first page)

        Returns:
            Tuple of (data_items, next_cursor)
        """
        # Run in thread pool to avoid blocking async loop
        loop = asyncio.get_event_loop()
        with ThreadPoolExecutor(max_workers=1) as executor:
            result = await loop.run_in_executor(
                executor,
                lambda: fetch_func(cursor=cursor),
            )

        return result

    async def _fetch_parallel_pages(
        self,
        fetch_func: Callable,
        first_cursor: str,
        workers: int,
        batch_callback: Optional[Callable] = None,
        batch_offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """
        Fetch multiple pages in parallel.

        This function creates worker tasks that each fetch pages sequentially,
        but multiple workers run in parallel.

        Args:
            fetch_func: Function to fetch a single page
            first_cursor: Cursor for the second page
            workers: Number of parallel workers
            batch_callback: Optional async callback for streaming writes
            batch_offset: Starting batch number (for numbering pages)

        Returns:
            List of all data items from remaining pages (empty if batch_callback is used)
        """
        # Queue to distribute work
        cursor_queue = asyncio.Queue()
        await cursor_queue.put(first_cursor)

        # Shared results list and batch counter
        results = []
        results_lock = asyncio.Lock()
        batch_counter = batch_offset
        counter_lock = asyncio.Lock()

        # Cancellation event to stop all workers immediately
        stop_event = asyncio.Event()

        # Worker coroutine
        async def worker():
            """Worker that fetches pages until no more cursors."""
            nonlocal batch_counter

            while not stop_event.is_set():
                try:
                    # Get next cursor (with timeout to avoid hanging)
                    cursor = await asyncio.wait_for(
                        cursor_queue.get(),
                        timeout=1.0,
                    )
                except asyncio.TimeoutError:
                    # No more work
                    break
                except asyncio.CancelledError:
                    # Interrupted - stop immediately
                    stop_event.set()
                    cursor_queue.task_done()
                    raise

                try:
                    # Fetch page
                    data, next_cursor = await self._fetch_page(fetch_func, cursor)

                    if batch_callback:
                        # Streaming mode - call callback
                        async with counter_lock:
                            current_batch = batch_counter
                            batch_counter += 1
                        await batch_callback(current_batch, data)
                    else:
                        # Memory mode - accumulate results
                        async with results_lock:
                            results.extend(data)

                    # Queue next page if available
                    if next_cursor:
                        await cursor_queue.put(next_cursor)

                    # Mark task done
                    cursor_queue.task_done()

                except asyncio.CancelledError:
                    # Interrupted during fetch - stop immediately
                    stop_event.set()
                    cursor_queue.task_done()
                    raise
                except Exception as e:
                    print(f"Worker error: {e}")
                    cursor_queue.task_done()
                    break

        # Start workers
        worker_tasks = [asyncio.create_task(worker()) for _ in range(workers)]

        try:
            # Wait for queue to be empty
            await cursor_queue.join()
        except asyncio.CancelledError:
            # Interrupted - signal all workers to stop
            stop_event.set()
            raise
        finally:
            # Cancel remaining workers
            for task in worker_tasks:
                if not task.done():
                    task.cancel()

            # Wait for workers to finish
            await asyncio.gather(*worker_tasks, return_exceptions=True)

        return results


class PolygonParallelFetcher(ParallelFetcher):
    """Specialized fetcher for Polygon.io SDK."""

    def __init__(self, polygon_client, num_workers: int = 5):
        """
        Initialize Polygon parallel fetcher.

        Args:
            polygon_client: Polygon RESTClient instance
            num_workers: Number of parallel workers (default: 5)
        """
        super().__init__(num_workers)
        self.client = polygon_client

    def create_fetch_function(
        self,
        method_name: str,
        use_vx: bool = False,
        **kwargs,
    ) -> Callable:
        """
        Create a fetch function for a specific Polygon SDK method.

        Args:
            method_name: Name of the SDK method (e.g., 'list_aggs')
            use_vx: If True, use client.vx.method instead of client.method
            **kwargs: Parameters to pass to the SDK method

        Returns:
            Callable that fetches a single page given a cursor
        """

        def fetch_page(cursor: Optional[str] = None):
            """Fetch a single page using Polygon SDK."""
            # Get the SDK method (handle vx client)
            if use_vx:
                method = getattr(self.client.vx, method_name)
            else:
                method = getattr(self.client, method_name)

            # Add cursor to params if provided
            params = kwargs.copy()
            if cursor:
                if "params" not in params:
                    params["params"] = {}
                params["params"]["cursor"] = cursor

            # Fetch with raw=True to get response metadata
            params["raw"] = True
            response = method(**params)

            # Parse response
            data_bytes = response.data
            data_json = json.loads(data_bytes.decode("utf-8"))

            # Extract results and next cursor
            results = data_json.get("results", [])
            next_cursor = data_json.get("next_url", "")

            # Extract cursor from next_url if present
            if next_cursor and "cursor=" in next_cursor:
                next_cursor = next_cursor.split("cursor=")[1].split("&")[0]
            else:
                next_cursor = None

            return results, next_cursor

        return fetch_page

    async def fetch_all(
        self,
        method_name: str,
        use_vx: bool = False,
        batch_callback: Optional[Callable] = None,
        **kwargs,
    ) -> List[Dict[str, Any]]:
        """
        Fetch all pages for a Polygon SDK method in parallel.

        Args:
            method_name: Name of the SDK method (e.g., 'list_aggs')
            use_vx: If True, use client.vx.method instead of client.method
            batch_callback: Optional async callback for streaming writes to disk
            **kwargs: Parameters to pass to the SDK method

        Returns:
            List of all data items (empty if batch_callback is used)

        Example:
            # Memory mode (old behavior)
            fetcher = PolygonParallelFetcher(polygon_client, num_workers=5)
            results = await fetcher.fetch_all(
                'list_aggs',
                ticker='AAPL',
                multiplier=1,
                timespan='day',
                from_='2023-01-01',
                to='2023-12-31',
                limit=50000,
            )

            # Streaming mode (new - writes to disk incrementally)
            async def save_batch(batch_num, data):
                # Write batch to disk immediately
                cache_mgr.save_batch(tool_name, params, data, batch_num)

            results = await fetcher.fetch_all(
                'list_aggs',
                ticker='AAPL',
                batch_callback=save_batch,
                ...
            )
        """
        fetch_func = self.create_fetch_function(method_name, use_vx=use_vx, **kwargs)
        return await self.fetch_all_pages(fetch_func, batch_callback=batch_callback)
