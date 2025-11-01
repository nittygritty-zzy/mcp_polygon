"""
Test parallel fetcher functionality.
"""

import asyncio
from unittest.mock import Mock
from mcp_polygon.parallel_fetcher import ParallelFetcher, PolygonParallelFetcher


def test_parallel_fetcher_single_page():
    """Test fetcher with single page of data."""

    async def run_test():
        fetcher = ParallelFetcher(num_workers=5)

        # Mock fetch function that returns single page
        def mock_fetch(cursor=None):
            if cursor is None:
                return [{"id": 1}, {"id": 2}], None
            return [], None

        results = await fetcher.fetch_all_pages(mock_fetch)
        assert len(results) == 2
        assert results[0]["id"] == 1
        assert results[1]["id"] == 2

    asyncio.run(run_test())


def test_parallel_fetcher_multiple_pages():
    """Test fetcher with multiple pages."""

    async def run_test():
        fetcher = ParallelFetcher(num_workers=3)

        # Mock fetch function with 3 pages
        page_data = {
            None: ([{"id": 1}, {"id": 2}], "cursor_1"),
            "cursor_1": ([{"id": 3}, {"id": 4}], "cursor_2"),
            "cursor_2": ([{"id": 5}], None),
        }

        def mock_fetch(cursor=None):
            return page_data.get(cursor, ([], None))

        results = await fetcher.fetch_all_pages(mock_fetch)
        assert len(results) == 5
        assert [r["id"] for r in results] == [1, 2, 3, 4, 5]

    asyncio.run(run_test())


def test_polygon_parallel_fetcher_create_function():
    """Test PolygonParallelFetcher creates correct fetch function."""
    # Create mock polygon client
    mock_client = Mock()
    mock_response = Mock()
    mock_response.data = b'{"results": [{"ticker": "AAPL"}], "next_url": null}'

    # Mock the SDK method
    mock_client.list_tickers = Mock(return_value=mock_response)

    # Create fetcher
    fetcher = PolygonParallelFetcher(mock_client, num_workers=5)

    # Create fetch function
    fetch_func = fetcher.create_fetch_function(
        method_name="list_tickers",
        market="stocks",
        active=True,
    )

    # Call fetch function
    results, next_cursor = fetch_func(cursor=None)

    # Verify
    assert len(results) == 1
    assert results[0]["ticker"] == "AAPL"
    assert next_cursor is None


def test_polygon_parallel_fetcher_vx_client():
    """Test PolygonParallelFetcher with VX client."""
    # Create mock polygon client with vx
    mock_client = Mock()
    mock_client.vx = Mock()
    mock_response = Mock()
    mock_response.data = b'{"results": [{"ticker": "AAPL"}], "next_url": null}'

    # Mock the VX SDK method
    mock_client.vx.list_ipos = Mock(return_value=mock_response)

    # Create fetcher
    fetcher = PolygonParallelFetcher(mock_client, num_workers=5)

    # Create fetch function with use_vx=True
    fetch_func = fetcher.create_fetch_function(
        method_name="list_ipos",
        use_vx=True,
        ipo_status="pending",
    )

    # Call fetch function
    results, next_cursor = fetch_func(cursor=None)

    # Verify
    assert len(results) == 1
    assert results[0]["ticker"] == "AAPL"
    assert next_cursor is None
    mock_client.vx.list_ipos.assert_called_once()


if __name__ == "__main__":
    test_parallel_fetcher_single_page()
    test_parallel_fetcher_multiple_pages()
    test_polygon_parallel_fetcher_create_function()
    test_polygon_parallel_fetcher_vx_client()
    print("✓ All tests passed!")
