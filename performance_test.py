"""
Performance test script to compare scraper versions.
"""
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from src.context import AppContext
from src.scraper_optimized import LetterboxdScraper
from src.scraper_async import AsyncLetterboxdScraper
from src.scraper_legacy import LegacyLetterboxdScraper
import time

print("Imported modules successfully.")


def test_scraper_performance(scraper_class, username, test_name):
    """Test a specific scraper implementation."""
    print(f"\n{'='*50}")
    print(f"TESTING: {test_name}")
    print(f"{'='*50}")
    
    # Create fresh context
    app_context = AppContext()
    scraper = scraper_class(app_context)
    
    try:
        start_time = time.time()
        scraper.scrape_user_profile(username)
        end_time = time.time()
        
        # Get results
        films_count = len(app_context.stats_data.url_list)
        total_time = end_time - start_time
        films_per_second = films_count / total_time if total_time > 0 else 0
        
        print(f"\n📊 RESULTS for {test_name}:")
        print(f"Films processed: {films_count}")
        print(f"Total time: {total_time:.1f}s")
        print(f"Speed: {films_per_second:.1f} films/second")
        
        return {
            'name': test_name,
            'films': films_count,
            'time': total_time,
            'speed': films_per_second
        }
        
    except Exception as e:
        print(f"❌ Error in {test_name}: {e}")
        return None


def main():
    print("🚀 SCRAPER PERFORMANCE COMPARISON")
    print("=" * 60)
    
    # Use Siryus with ~860 films for comprehensive testing
    test_username = "Siryus"
    print(f"Testing with user: {test_username} (~860 films)")
    
    results = []
    
    # Test legacy scraper (baseline)
    result1 = test_scraper_performance(LegacyLetterboxdScraper, test_username, "Legacy Scraper")
    if result1:
        results.append(result1)
    
    # Test current scraper (optimized)
    result2 = test_scraper_performance(LetterboxdScraper, test_username, "Optimized Scraper")
    if result2:
        results.append(result2)
    
    # Test async scraper  
    result3 = test_scraper_performance(AsyncLetterboxdScraper, test_username, "Async Scraper")
    if result3:
        results.append(result3)
    
    # Compare results
    if len(results) >= 2:
        print(f"\n{'='*60}")
        print("📈 PERFORMANCE COMPARISON")
        print(f"{'='*60}")
        
        for i, result in enumerate(results):
            requests_per_sec = (result['films'] * 2) / result['time'] if result['time'] > 0 else 0  # ~2 requests per film
            print(f"{result['name']}:")
            print(f"  Time: {result['time']:.1f}s")
            print(f"  Speed: {result['speed']:.1f} films/s")
            print(f"  Requests/sec: {requests_per_sec:.1f}")
            print()
        
        if len(results) >= 2:
            # Compare fastest vs slowest
            fastest = min(results, key=lambda x: x['time'])
            slowest = max(results, key=lambda x: x['time'])
            
            if slowest['time'] > 0:
                improvement = slowest['time'] / fastest['time']
                time_saved = slowest['time'] - fastest['time']
                
                print(f"🎯 PERFORMANCE RANKING:")
                sorted_results = sorted(results, key=lambda x: x['time'])
                for i, result in enumerate(sorted_results):
                    print(f"{i+1}. {result['name']}: {result['time']:.1f}s ({result['speed']:.1f} films/s)")
                
                print(f"\n🚀 BEST vs WORST:")
                print(f"Fastest: {fastest['name']} ({fastest['time']:.1f}s)")
                print(f"Slowest: {slowest['name']} ({slowest['time']:.1f}s)")
                print(f"Speed improvement: {improvement:.1f}x faster")
                print(f"Time saved: {time_saved:.1f} seconds ({time_saved/60:.1f} minutes)")


if __name__ == "__main__":
    main()