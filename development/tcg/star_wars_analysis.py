#!/usr/bin/env python3
"""
Star Wars TCG Dollar Volume Analysis

A simplified script to specifically analyze and visualize the Star Wars TCG dollar volume.
This script calls the main dollar_volume_analysis.py with the --only-star-wars flag.
"""

import os
import subprocess
import sys

def main():
    # Get the directory of this script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Path to the main analysis script
    analysis_script = os.path.join(script_dir, "dollar_volume_analysis.py")
    
    # Output file names
    output_png = os.path.join(script_dir, "star_wars_dollar_volume.png")
    output_csv = os.path.join(script_dir, "star_wars_dollar_volume_data.csv")
    
    # Build command with arguments
    cmd = [
        sys.executable,  # Use the same Python interpreter
        analysis_script,
        "--only-star-wars",
        "--output", output_png,
        "--csv", output_csv,
        "--min-volume", "0"  # Include all Star Wars sets regardless of volume
    ]
    
    print("Analyzing Star Wars TCG dollar volume...")
    print(f"Output will be saved to: {output_png}")
    
    # Run the analysis script
    try:
        result = subprocess.run(cmd, check=True)
        if result.returncode == 0:
            print(f"\nAnalysis complete! Visualization saved to {output_png}")
            print(f"Data exported to {output_csv}")
    except subprocess.CalledProcessError as e:
        print(f"Error running analysis: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")

if __name__ == "__main__":
    main()
