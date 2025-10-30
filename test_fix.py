"""Quick test to verify vars() works with Polygon SDK objects"""
import os
os.environ['POLYGON_API_KEY'] = 'test_key'

from polygon import RESTClient

# Create a mock test
client = RESTClient('test_key')

# Test that we can create a TickerNews-like object and use vars() on it
from polygon.rest.models.tickers import TickerNews, Publisher, Insight

# Create a test object
news = TickerNews(
    id="test123",
    title="Test Article",
    author="Test Author",
    article_url="https://example.com",
    description="Test description",
    published_utc="2025-10-29T12:00:00Z",
    publisher=Publisher(name="Test Publisher"),
    insights=[Insight(ticker="NVDA", sentiment="positive", sentiment_reasoning="Good news")],
    keywords=["test", "news"],
    tickers=["NVDA"],
    amp_url=None,
    image_url=None
)

# Test vars() works
news_dict = vars(news)
print("✓ vars() works on TickerNews object")
print(f"✓ Converted to dict with {len(news_dict)} keys: {list(news_dict.keys())}")

# Test that the dict has expected values
assert news_dict['id'] == "test123"
assert news_dict['title'] == "Test Article"
assert news_dict['author'] == "Test Author"
print("✓ Dict values are correct")

print("\nAll tests passed! The fix works correctly.")
