#!/usr/bin/env python3
"""
Health check and monitoring script for the modular scraping platform scheduler.

Usage:
    python health_check.py [command]

Commands:
    status      - Show scheduler health status (default)
    jobs        - List all scheduled jobs with details
    next        - Show jobs running in the next hour
    overdue     - Show overdue jobs
    stats       - Show execution statistics
    watch       - Continuously monitor (refresh every 30s)
"""

import asyncio
import sys
import os
import json
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List

# Add project root to PYTHONPATH
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from core.infra.scheduler import Scheduler


class Colors:
    """ANSI color codes for terminal output."""
    GREEN = "\033[92m"
    YELLOW = "\033[93m" 
    RED = "\033[91m"
    BLUE = "\033[94m"
    CYAN = "\033[96m"
    WHITE = "\033[97m"
    BOLD = "\033[1m"
    END = "\033[0m"


def format_timestamp(dt: datetime) -> str:
    """Format datetime for display."""
    if not dt:
        return "N/A"
    return dt.strftime("%Y-%m-%d %H:%M:%S UTC")


def format_duration(seconds: float) -> str:
    """Format duration in seconds to human readable."""
    if seconds < 60:
        return f"{seconds:.0f}s"
    elif seconds < 3600:
        return f"{seconds/60:.1f}m"
    elif seconds < 86400:
        return f"{seconds/3600:.1f}h"
    else:
        return f"{seconds/86400:.1f}d"


def format_status_indicator(status: str) -> str:
    """Format status with color indicator."""
    indicators = {
        "scheduled": f"{Colors.GREEN}●{Colors.END}",
        "imminent": f"{Colors.YELLOW}●{Colors.END}",
        "overdue": f"{Colors.RED}●{Colors.END}",
        "inactive": f"{Colors.RED}○{Colors.END}",
    }
    return indicators.get(status, "●")


async def show_health_status():
    """Show basic scheduler health status."""
    scheduler = Scheduler()
    
    try:
        # We need to start the scheduler to access job store
        await scheduler.start()
        health = scheduler.get_health_status()
        
        print(f"{Colors.BOLD}📊 Scheduler Health Status{Colors.END}")
        print("=" * 50)
        
        # Status indicators
        status_color = Colors.GREEN if health["scheduler_running"] else Colors.RED
        print(f"Status: {status_color}{('🟢 Running' if health['scheduler_running'] else '🔴 Stopped')}{Colors.END}")
        print(f"Timezone: {Colors.CYAN}{health['timezone']}{Colors.END}")
        print(f"Last Check: {Colors.WHITE}{health['timestamp']}{Colors.END}")
        print()
        
        # Job summary
        print(f"{Colors.BOLD}Job Summary:{Colors.END}")
        print(f"  Total Jobs: {Colors.WHITE}{health['total_jobs']}{Colors.END}")
        print(f"  Active Jobs: {Colors.GREEN}{health['active_jobs']}{Colors.END}")
        
        if health["overdue_jobs"] > 0:
            print(f"  Overdue Jobs: {Colors.RED}{health['overdue_jobs']}{Colors.END}")
            print(f"  Overdue IDs: {Colors.YELLOW}{', '.join(health['overdue_job_ids'])}{Colors.END}")
        else:
            print(f"  Overdue Jobs: {Colors.GREEN}0{Colors.END}")
        
        print()
        
        # Overall health
        if health["scheduler_running"] and health["overdue_jobs"] == 0:
            print(f"{Colors.GREEN}✅ System Healthy{Colors.END}")
        elif health["overdue_jobs"] > 0:
            print(f"{Colors.YELLOW}⚠️  Warning: {health['overdue_jobs']} overdue jobs{Colors.END}")
        else:
            print(f"{Colors.RED}❌ System Issue: Scheduler not running{Colors.END}")
            
    finally:
        await scheduler.stop()


async def show_detailed_jobs():
    """Show detailed job information."""
    scheduler = Scheduler()
    
    try:
        await scheduler.start()
        jobs = scheduler.get_detailed_job_status()
        
        print(f"{Colors.BOLD}📋 Scheduled Jobs Details{Colors.END}")
        print("=" * 80)
        
        if not jobs:
            print(f"{Colors.YELLOW}No jobs scheduled{Colors.END}")
            return
        
        for job in jobs:
            status_icon = format_status_indicator(job["status"])
            print(f"{status_icon} {Colors.BOLD}{job['id']}{Colors.END}")
            print(f"   Type: {Colors.CYAN}{job['trigger_type']}{Colors.END}")
            print(f"   Trigger: {Colors.WHITE}{job['trigger_details']}{Colors.END}")
            
            if job["next_run"]:
                next_run_dt = datetime.fromisoformat(job["next_run"].replace('Z', '+00:00'))
                print(f"   Next Run: {Colors.GREEN}{format_timestamp(next_run_dt)}{Colors.END}")
                
                if "next_run_seconds" in job:
                    duration = format_duration(job["next_run_seconds"])
                    if job["status"] == "imminent":
                        print(f"   Time to Run: {Colors.YELLOW}{duration}{Colors.END}")
                    else:
                        print(f"   Time to Run: {Colors.WHITE}{duration}{Colors.END}")
                        
                if "overdue_seconds" in job:
                    overdue = format_duration(job["overdue_seconds"])
                    print(f"   Overdue by: {Colors.RED}{overdue}{Colors.END}")
            else:
                print(f"   Next Run: {Colors.RED}Not scheduled{Colors.END}")
            
            print(f"   Max Instances: {job['max_instances']}")
            print()
            
    finally:
        await scheduler.stop()


async def show_next_jobs(hours: int = 1):
    """Show jobs running in the next N hours."""
    scheduler = Scheduler()
    
    try:
        await scheduler.start()
        jobs = scheduler.get_detailed_job_status()
        
        now = datetime.now(timezone.utc)
        cutoff = now + timedelta(hours=hours)
        
        upcoming = []
        for job in jobs:
            if job["next_run"] and job["status"] in ["scheduled", "imminent"]:
                next_run_dt = datetime.fromisoformat(job["next_run"].replace('Z', '+00:00'))
                if next_run_dt <= cutoff:
                    upcoming.append((job, next_run_dt))
        
        upcoming.sort(key=lambda x: x[1])
        
        print(f"{Colors.BOLD}⏰ Jobs Running in Next {hours} Hour(s){Colors.END}")
        print("=" * 60)
        
        if not upcoming:
            print(f"{Colors.GREEN}No jobs scheduled in the next {hours} hour(s){Colors.END}")
            return
        
        for job, next_run_dt in upcoming:
            time_until = (next_run_dt - now).total_seconds()
            status_icon = format_status_indicator(job["status"])
            
            print(f"{status_icon} {Colors.BOLD}{job['id']}{Colors.END}")
            print(f"   Runs at: {Colors.GREEN}{format_timestamp(next_run_dt)}{Colors.END}")
            print(f"   In: {Colors.YELLOW}{format_duration(time_until)}{Colors.END}")
            print()
            
    finally:
        await scheduler.stop()


async def show_overdue_jobs():
    """Show overdue jobs."""
    scheduler = Scheduler()
    
    try:
        await scheduler.start()
        jobs = scheduler.get_detailed_job_status()
        
        overdue = [job for job in jobs if job["status"] == "overdue"]
        
        print(f"{Colors.BOLD}🚨 Overdue Jobs{Colors.END}")
        print("=" * 50)
        
        if not overdue:
            print(f"{Colors.GREEN}No overdue jobs{Colors.END}")
            return
        
        for job in overdue:
            overdue_time = format_duration(job.get("overdue_seconds", 0))
            print(f"{Colors.RED}● {Colors.BOLD}{job['id']}{Colors.END}")
            print(f"   Overdue by: {Colors.RED}{overdue_time}{Colors.END}")
            print(f"   Should have run: {Colors.WHITE}{job['next_run']}{Colors.END}")
            print()
            
    finally:
        await scheduler.stop()


async def show_stats():
    """Show execution statistics."""
    scheduler = Scheduler()
    
    try:
        await scheduler.start()
        stats = scheduler.get_job_execution_stats()
        
        print(f"{Colors.BOLD}📈 Job Statistics{Colors.END}")
        print("=" * 40)
        
        print(f"Total Jobs: {Colors.WHITE}{stats['total_jobs']}{Colors.END}")
        print(f"Job Types:")
        for job_type, count in stats["job_types"].items():
            print(f"  {job_type.title()}: {Colors.CYAN}{count}{Colors.END}")
        
    finally:
        await scheduler.stop()


async def watch_status():
    """Continuously monitor scheduler status."""
    print(f"{Colors.BOLD}👀 Watching Scheduler Status (Press Ctrl+C to exit){Colors.END}")
    print()
    
    try:
        while True:
            # Clear screen
            os.system('clear' if os.name == 'posix' else 'cls')
            
            print(f"{Colors.BOLD}📊 Live Scheduler Monitor{Colors.END}")
            print(f"Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print("=" * 60)
            
            await show_health_status()
            
            print()
            print(f"{Colors.BLUE}Refreshing in 30 seconds... (Ctrl+C to exit){Colors.END}")
            
            await asyncio.sleep(30)
            
    except KeyboardInterrupt:
        print(f"\n{Colors.YELLOW}Monitoring stopped{Colors.END}")


async def main():
    """Main entry point."""
    command = sys.argv[1] if len(sys.argv) > 1 else "status"
    
    commands = {
        "status": show_health_status,
        "jobs": show_detailed_jobs,
        "next": lambda: show_next_jobs(1),
        "overdue": show_overdue_jobs,
        "stats": show_stats,
        "watch": watch_status,
    }
    
    if command not in commands:
        print(f"Unknown command: {command}")
        print(f"Available commands: {', '.join(commands.keys())}")
        print()
        print(__doc__)
        sys.exit(1)
    
    try:
        await commands[command]()
    except KeyboardInterrupt:
        print(f"\n{Colors.YELLOW}Interrupted{Colors.END}")
    except Exception as e:
        print(f"{Colors.RED}Error: {e}{Colors.END}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
