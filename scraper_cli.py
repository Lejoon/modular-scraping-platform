#!/usr/bin/env python3
"""
Scraper management CLI - Easy commands for monitoring and managing the scraping platform.

Usage: python scraper_cli.py <command> [options]

Commands:
    start           - Start the scraper platform (with pm2 if available)
    stop            - Stop the scraper platform
    restart         - Restart the scraper platform  
    status          - Show scheduler health status
    jobs            - List all scheduled jobs
    next [hours]    - Show jobs running in next N hours (default: 1)
    overdue         - Show overdue jobs
    watch           - Watch scheduler status (live updates)
    logs            - Show recent logs (pm2 logs if available)
    pm2-setup       - Setup pm2 configuration
"""

import subprocess
import sys
import os
import asyncio
import json
from pathlib import Path

# Add project root to PYTHONPATH
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from health_check import (
    show_health_status, show_detailed_jobs, show_next_jobs, 
    show_overdue_jobs, watch_status, Colors
)


def run_command(cmd: str, capture_output: bool = True) -> tuple[int, str, str]:
    """Run a shell command and return exit code, stdout, stderr."""
    try:
        result = subprocess.run(
            cmd.split(), 
            capture_output=capture_output, 
            text=True, 
            timeout=30
        )
        return result.returncode, result.stdout, result.stderr
    except subprocess.TimeoutExpired:
        return 1, "", "Command timed out"
    except Exception as e:
        return 1, "", str(e)


def is_pm2_available() -> bool:
    """Check if pm2 is available."""
    code, _, _ = run_command("pm2 --version")
    return code == 0


def is_running_with_pm2() -> bool:
    """Check if the scraper is currently running with pm2."""
    if not is_pm2_available():
        return False
    
    code, stdout, _ = run_command("pm2 jlist")
    if code != 0:
        return False
    
    try:
        processes = json.loads(stdout)
        return any(proc.get("name") == "scraper-platform" for proc in processes)
    except:
        return False


def setup_pm2():
    """Setup pm2 ecosystem configuration."""
    pm2_config = {
        "apps": [
            {
                "name": "scraper-platform",
                "script": "main.py",
                "interpreter": "python3",
                "cwd": str(Path(__file__).parent.absolute()),
                "env": {
                    "PYTHONPATH": str(Path(__file__).parent.absolute()),
                    "SCHEDULER_MODE": "enabled"
                },
                "instances": 1,
                "autorestart": True,
                "watch": False,
                "max_memory_restart": "1G",
                "error_file": "./logs/scraper-error.log",
                "out_file": "./logs/scraper-out.log",
                "log_file": "./logs/scraper-combined.log",
                "time": True,
                "merge_logs": True
            }
        ]
    }
    
    # Create logs directory
    logs_dir = Path("logs")
    logs_dir.mkdir(exist_ok=True)
    
    # Write ecosystem file
    with open("ecosystem.config.json", "w") as f:
        json.dump(pm2_config, f, indent=2)
    
    print(f"{Colors.GREEN}✅ PM2 configuration created: ecosystem.config.json{Colors.END}")
    print(f"{Colors.BLUE}💡 You can now use 'pm2 start ecosystem.config.json' to start the platform{Colors.END}")


def start_platform():
    """Start the scraper platform."""
    if is_pm2_available():
        # Check if already running
        if is_running_with_pm2():
            print(f"{Colors.YELLOW}⚠️  Platform is already running with PM2{Colors.END}")
            return
        
        # Try to start with pm2
        if os.path.exists("ecosystem.config.json"):
            print(f"{Colors.BLUE}🚀 Starting platform with PM2...{Colors.END}")
            code, stdout, stderr = run_command("pm2 start ecosystem.config.json", capture_output=False)
            if code == 0:
                print(f"{Colors.GREEN}✅ Platform started with PM2{Colors.END}")
                return
        else:
            print(f"{Colors.YELLOW}📝 PM2 config not found. Run 'python scraper_cli.py pm2-setup' first{Colors.END}")
    
    # Fallback to direct execution
    print(f"{Colors.BLUE}🚀 Starting platform directly...{Colors.END}")
    print(f"{Colors.YELLOW}💡 Press Ctrl+C to stop{Colors.END}")
    try:
        subprocess.run([sys.executable, "main.py"])
    except KeyboardInterrupt:
        print(f"\n{Colors.YELLOW}Platform stopped{Colors.END}")


def stop_platform():
    """Stop the scraper platform."""
    if is_running_with_pm2():
        print(f"{Colors.BLUE}🛑 Stopping platform with PM2...{Colors.END}")
        code, _, _ = run_command("pm2 stop scraper-platform")
        if code == 0:
            print(f"{Colors.GREEN}✅ Platform stopped{Colors.END}")
        else:
            print(f"{Colors.RED}❌ Failed to stop platform{Colors.END}")
    else:
        print(f"{Colors.YELLOW}⚠️  Platform not running with PM2{Colors.END}")
        print(f"{Colors.BLUE}💡 If running directly, use Ctrl+C in the terminal{Colors.END}")


def restart_platform():
    """Restart the scraper platform.""" 
    if is_running_with_pm2():
        print(f"{Colors.BLUE}🔄 Restarting platform with PM2...{Colors.END}")
        code, _, _ = run_command("pm2 restart scraper-platform")
        if code == 0:
            print(f"{Colors.GREEN}✅ Platform restarted{Colors.END}")
        else:
            print(f"{Colors.RED}❌ Failed to restart platform{Colors.END}")
    else:
        print(f"{Colors.YELLOW}⚠️  Platform not running with PM2{Colors.END}")
        print(f"{Colors.BLUE}💡 Stop and start manually if running directly{Colors.END}")


def show_logs():
    """Show recent logs."""
    if is_running_with_pm2():
        print(f"{Colors.BLUE}📜 Showing PM2 logs...{Colors.END}")
        subprocess.run(["pm2", "logs", "scraper-platform", "--lines", "50"])
    else:
        # Try to show logs from file if available
        log_files = ["logs/scraper-combined.log", "scraper.log", "platform.log"]
        
        for log_file in log_files:
            if os.path.exists(log_file):
                print(f"{Colors.BLUE}📜 Showing recent logs from {log_file}...{Colors.END}")
                with open(log_file, "r") as f:
                    lines = f.readlines()
                    for line in lines[-50:]:  # Last 50 lines
                        print(line.rstrip())
                return
        
        print(f"{Colors.YELLOW}⚠️  No log files found{Colors.END}")


async def main():
    """Main CLI entry point."""
    if len(sys.argv) < 2:
        print(__doc__)
        return
    
    command = sys.argv[1].lower()
    
    # Non-async commands
    if command == "start":
        start_platform()
        return
    elif command == "stop":
        stop_platform()
        return
    elif command == "restart":
        restart_platform()
        return
    elif command == "logs":
        show_logs()
        return
    elif command == "pm2-setup":
        setup_pm2()
        return
    
    # Async commands (health monitoring)
    try:
        if command == "status":
            await show_health_status()
        elif command == "jobs":
            await show_detailed_jobs()
        elif command == "next":
            hours = int(sys.argv[2]) if len(sys.argv) > 2 else 1
            await show_next_jobs(hours)
        elif command == "overdue":
            await show_overdue_jobs()
        elif command == "watch":
            await watch_status()
        else:
            print(f"{Colors.RED}Unknown command: {command}{Colors.END}")
            print(__doc__)
            
    except KeyboardInterrupt:
        print(f"\n{Colors.YELLOW}Interrupted{Colors.END}")
    except Exception as e:
        print(f"{Colors.RED}Error: {e}{Colors.END}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)
        
    command = sys.argv[1].lower()
    
    # Commands that don't need async
    if command in ["start", "stop", "restart", "logs", "pm2-setup"]:
        if command == "start":
            start_platform()
        elif command == "stop":
            stop_platform()
        elif command == "restart":
            restart_platform()
        elif command == "logs":
            show_logs()
        elif command == "pm2-setup":
            setup_pm2()
    else:
        # Async commands
        asyncio.run(main())
