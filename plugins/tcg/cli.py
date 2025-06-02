"""
Command-line interface for TCG analytics platform.

This module provides a comprehensive CLI for data management, analysis, and visualization.
"""

import sys
import subprocess
from typing import List


class TCGCommandLineInterface:
    """Command-line interface for TCG analytics operations."""
    
    def __init__(self):
        """Initialize the CLI."""
        pass
    
    def show_help(self) -> None:
        """Display available commands."""
        print("Available commands:")
        print("  summary              - Show Pokemon sets summary")
        print("  trends [product_id] [days] - Show price trends")
        print("  fetch_one <product_id> - Fetch price history for one product")
        print("  fetch_all            - Fetch price history for all Pokemon sets")
        print("  test_api             - Test price history API with sample data")
        print("  query                - Query and display price history example")
        print("  latest_sales         - Test latest sales API")
        print()
        print("Analysis commands:")
        print("  analyze <product_id> [window_days] - Detailed analysis of a product")
        print("  export <product_id> [filename]     - Export time series data to CSV")
        print("  sets                              - List all available Pokemon sets")
        print("  recent [days]                     - Show recent price activity")
        print("  demo                              - Run analysis demo")
        print("  volume [filename]                 - Export volume sold data for all sets to CSV")
        print("  volume_summary                    - Show volume sold summary by set")
        print("  visualize                         - Create volume charts")
        print("                                      Use --include-type to include product type in volume export")
    
    def run_summary(self) -> None:
        """Show Pokemon sets summary."""
        from data_manager import get_pokemon_sets_summary
        get_pokemon_sets_summary()
    
    def run_trends(self, args: List[str]) -> None:
        """Show price trends."""
        from data_manager import analyze_price_trends
        product_id = int(args[0]) if len(args) > 0 else None
        days = int(args[1]) if len(args) > 1 else 30
        analyze_price_trends(product_id, days)
    
    def run_fetch_one(self, args: List[str]) -> None:
        """Fetch price history for one product."""
        if len(args) < 1:
            print("Usage: python cli.py fetch_one <product_id>")
            return
        from data_collector import fetch_and_save_price_history
        product_id = int(args[0])
        fetch_and_save_price_history(product_id)
    
    def run_fetch_all(self) -> None:
        """Fetch price history for all Pokemon sets."""
        from data_collector import fetch_all_pokemon_price_history
        fetch_all_pokemon_price_history()
    
    def run_test_api(self) -> None:
        """Test price history API with sample data."""
        from testing import test_price_history_api
        test_price_history_api()
    
    def run_query(self) -> None:
        """Query and display price history example."""
        from testing import query_price_history_example
        query_price_history_example()
    
    def run_latest_sales(self) -> None:
        """Test latest sales API."""
        from testing import main
        main()
    
    def run_analyze(self, args: List[str]) -> None:
        """Detailed analysis of a product."""
        if len(args) < 1:
            print("Usage: python cli.py analyze <product_id> [window_days]")
            return
        
        import analysis
        product_id = int(args[0])
        window_days = int(args[1]) if len(args) > 1 else 7
        
        print(f"=== Analysis for Product {product_id} ===")
        
        # Get time series data
        time_series = analysis.get_time_series_by_product_id(product_id)
        print(f"Data points: {len(time_series)}")
        
        if time_series:
            print(f"Date range: {time_series[0].date} to {time_series[-1].date}")
            print(f"Latest price: ${time_series[-1].market_price}")
            print(f"Latest volume: {time_series[-1].quantity_sold} sold")
            
            # Price summary
            summary = analysis.get_price_summary_by_product(product_id)
            print(f"\nPrice Summary:")
            print(f"  Min: ${summary['market_price']['min']:.2f}")
            print(f"  Max: ${summary['market_price']['max']:.2f}")
            print(f"  Avg: ${summary['market_price']['avg']:.2f}")
            print(f"  Total volume: {summary['sales_volume']['total']:,}")
            
            # Trend analysis
            trends = analysis.analyze_price_trends(product_id, window_days)
            if 'error' not in trends:
                print(f"\nTrend Analysis ({window_days}-day window):")
                print(f"  Moving avg: ${trends['moving_avg_price']:.2f}")
                print(f"  Recent trend: {trends['recent_trend']['direction']} (${trends['recent_trend']['change']:.2f})")
                print(f"  Overall trend: {trends['overall_trend']['direction']} (${trends['overall_trend']['change']:.2f})")
            else:
                print(f"\nTrend Analysis: {trends['error']}")
        else:
            print("No data found for this product ID")
    
    def run_export(self, args: List[str]) -> None:
        """Export time series data to CSV."""
        if len(args) < 1:
            print("Usage: python cli.py export <product_id> [filename]")
            return
        
        import analysis
        product_id = int(args[0])
        filename = args[1] if len(args) > 1 else f"product_{product_id}_data.csv"
        
        time_series = analysis.get_time_series_by_product_id(product_id)
        if time_series:
            analysis.export_to_csv(time_series, filename)
        else:
            print(f"No data found for product {product_id}")
    
    def run_sets(self) -> None:
        """List all available Pokemon sets."""
        import analysis
        sets = analysis.get_all_available_sets()
        print("=== Available Pokemon Sets ===")
        for i, set_info in enumerate(sets, 1):
            print(f"{i:2d}. {set_info['set_name']}")
            if set_info['booster_product_id']:
                print(f"     Booster Pack ID: {set_info['booster_product_id']} (Data: {set_info['has_booster_data']})")
            if set_info['booster_box_product_id']:
                print(f"     Booster Box ID: {set_info['booster_box_product_id']} (Data: {set_info['has_box_data']})")
            print(f"     Release: {set_info['release_date']}")
            print()
    
    def run_recent(self, args: List[str]) -> None:
        """Show recent price activity."""
        import analysis
        days = int(args[0]) if len(args) > 0 else 30
        activity = analysis.get_recent_activity(days)
        print(f"=== Recent Activity (Last {days} days) ===")
        
        if activity:
            for item in activity[:20]:  # Show first 20
                print(f"{item['date']}: {item['set_name']} - ${item['market_price']:.2f} ({item['quantity_sold']} sold)")
        else:
            print("No recent activity found")
    
    def run_demo(self) -> None:
        """Run analysis demo."""
        import analysis
        analysis.demo_analysis()
    
    def run_visualize(self) -> None:
        """Create volume charts."""
        result = subprocess.run([sys.executable, "visualize_volume.py"], 
                              capture_output=True, text=True)
        if result.returncode == 0:
            print(result.stdout)
        else:
            print("Error running visualization:")
            print(result.stderr)
    
    def run_volume(self, args: List[str]) -> None:
        """Export volume sold data for all sets to CSV."""
        import analysis
        filename = args[0] if len(args) > 0 else "pokemon_sets_volume_sold.csv"
        include_product_type = "--include-type" in sys.argv
        
        print("Generating volume sold data for all Pokemon sets...")
        analysis.export_volume_sold_csv(filename, include_product_type)
    
    def run_volume_summary(self) -> None:
        """Show volume sold summary by set."""
        import analysis
        summary = analysis.get_volume_summary_by_set()
        
        print("=== Volume Sold Summary by Pokemon Set ===")
        print(f"{'Set Name':<40} {'Total Volume ($)':<15} {'Qty Sold':<10} {'Avg Price':<10} {'Data Points':<12}")
        print("-" * 90)
        
        for set_data in summary:
            print(f"{set_data['set_name']:<40} ${set_data['total_volume']:>13,.2f} {set_data['total_quantity']:>9,} ${set_data['avg_price']:>8.2f} {set_data['data_points']:>11}")
        
        print(f"\nTotal sets with sales data: {len(summary)}")
        if summary:
            total_volume = sum(s['total_volume'] for s in summary)
            total_quantity = sum(s['total_quantity'] for s in summary)
            print(f"Overall total volume: ${total_volume:,.2f}")
            print(f"Overall total quantity: {total_quantity:,} units")
    
    def run_command(self, command: str, args: List[str]) -> None:
        """Execute a command with arguments."""
        command_map = {
            "summary": self.run_summary,
            "trends": lambda: self.run_trends(args),
            "fetch_one": lambda: self.run_fetch_one(args),
            "fetch_all": self.run_fetch_all,
            "test_api": self.run_test_api,
            "query": self.run_query,
            "latest_sales": self.run_latest_sales,
            "analyze": lambda: self.run_analyze(args),
            "export": lambda: self.run_export(args),
            "sets": self.run_sets,
            "recent": lambda: self.run_recent(args),
            "demo": self.run_demo,
            "visualize": self.run_visualize,
            "volume": lambda: self.run_volume(args),
            "volume_summary": self.run_volume_summary,
        }
        
        if command in command_map:
            command_map[command]()
        else:
            self.show_help()


def main() -> None:
    """Main CLI entry point."""
    cli = TCGCommandLineInterface()
    
    if len(sys.argv) > 1:
        command = sys.argv[1]
        args = sys.argv[2:]
        cli.run_command(command, args)
    else:
        # Default behavior - test price history
        from testing import test_price_history_api, query_price_history_example
        test_price_history_api()
        query_price_history_example()


if __name__ == "__main__":
    main()
