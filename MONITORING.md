# Scheduler Health Check & Monitoring System

This document describes the health check and monitoring capabilities for the modular scraping platform scheduler.

## Overview

The monitoring system provides multiple ways to check the health of your scheduled jobs:

1. **Standalone Health Check Script** - `health_check.py`
2. **Comprehensive CLI Tool** - `scraper_cli.py` / `scraper` wrapper
3. **Discord Bot Commands** - Enhanced with monitoring features
4. **PM2 Integration** - For production deployments

## Quick Commands

### Using the `scraper` wrapper (recommended):
```bash
# Start the platform
./scraper start

# Check health status
./scraper status

# List all jobs with details
./scraper jobs

# Show jobs running in next hour
./scraper next

# Show jobs running in next 4 hours
./scraper next 4

# Show overdue jobs
./scraper overdue

# Watch status continuously (live updates every 30s)
./scraper watch

# Show recent logs
./scraper logs

# Stop the platform
./scraper stop

# Restart the platform
./scraper restart

# Setup PM2 configuration
./scraper pm2-setup
```

### Using Python directly:
```bash
# Health check commands
python health_check.py status
python health_check.py jobs
python health_check.py next
python health_check.py overdue
python health_check.py watch

# CLI management
python scraper_cli.py start
python scraper_cli.py status
python scraper_cli.py stop
```

## Features

### Health Status Check
Shows overall scheduler health including:
- ✅ Scheduler running status
- 📊 Job counts (total, active, overdue)
- 🕐 Timezone information
- ⚠️ Warnings for overdue jobs
- 🚨 Critical alerts for system issues

### Detailed Job Monitoring
For each scheduled job, displays:
- 🔵 Job ID and type (cron/interval)
- ⏰ Next run time
- ⏱️ Time until next run
- 🔴 Overdue status and duration
- ⚙️ Configuration details (max instances, coalesce settings)

### Status Indicators
- 🟢 **Green dot**: Job scheduled normally
- 🟡 **Yellow dot**: Job running soon (< 5 minutes)
- 🔴 **Red dot**: Job overdue
- ⚪ **White dot**: Job inactive/not scheduled

### Discord Bot Commands
If you have Discord integration enabled, use these slash commands:

- `/health` - Show scheduler health status
- `/jobs` - List all scheduled jobs
- `/next [hours]` - Show jobs running in next N hours
- `/overdue` - Show overdue jobs
- `/run <pipeline>` - Run a pipeline immediately
- `/schedule <pipeline> <cron>` - Schedule a pipeline
- `/remove <job_id>` - Remove a scheduled job
- `/pipelines` - List available pipelines

## PM2 Integration

For production deployments, the system integrates with PM2 for process management:

### Setup PM2
```bash
# Install PM2 globally (if not already installed)
npm install -g pm2

# Setup PM2 configuration for the scraper
./scraper pm2-setup

# Start with PM2
pm2 start ecosystem.config.json

# Or use the wrapper
./scraper start
```

### PM2 Monitoring
```bash
# Check PM2 status
pm2 status

# View logs
pm2 logs scraper-platform

# Monitor resources
pm2 monit

# Restart
pm2 restart scraper-platform
```

## Example Outputs

### Health Status
```
📊 Scheduler Health Status
==================================================
Status: 🟢 Running
Timezone: Europe/Stockholm
Last Check: 2025-06-11T10:30:45.123456+00:00

Job Summary:
  Total Jobs: 3
  Active Jobs: 3
  Overdue Jobs: 0

✅ System Healthy
```

### Job Details
```
📋 Scheduled Jobs Details
================================================================================
🟢 pipeline_appmagic_scraper
   Type: CronTrigger
   Trigger: cron[hour='9', minute='0']
   Next Run: 2025-06-12 09:00:00 UTC
   Time to Run: 22.5h
   Max Instances: 3

🟡 pipeline_tcg_update
   Type: IntervalTrigger
   Trigger: interval[0:05:00]
   Next Run: 2025-06-11 10:35:00 UTC
   Time to Run: 4.2m
   Max Instances: 3
```

### Overdue Jobs
```
🚨 Overdue Jobs
==================================================
🔴 pipeline_broken_job
   Overdue by: 2.5h
   Should have run: 2025-06-11T08:00:00+00:00
```

## Troubleshooting

### Common Issues

1. **"Scheduler not running"**
   - Check if main.py process is running
   - Verify SCHEDULER_MODE environment variable is not set to "disabled"
   - Check logs for startup errors

2. **Jobs showing as overdue**
   - Check system resources (CPU/memory)
   - Look for errors in job execution logs
   - Verify job functions are not hanging

3. **Health check fails**
   - Ensure scheduler database is accessible
   - Check file permissions on scheduler_jobs.db
   - Verify virtual environment is activated

### Log Locations
- **Direct execution**: Console output
- **PM2**: `logs/scraper-*.log`
- **Discord bot**: Included in main logs

### Database
The scheduler uses SQLite for persistence:
- Database file: `scheduler_jobs.db`
- Backup automatically created by APScheduler
- Can be safely deleted to reset all scheduled jobs

## Environment Variables

- `SCHEDULER_MODE`: Set to "disabled" to run without scheduler
- `SCHEDULER_TIMEZONE`: Timezone for scheduler (default: "Europe/Stockholm")
- `DISCORD_TOKEN`: Enable Discord bot integration
- `PIPELINES_CONFIG`: Path to pipelines configuration file

## Integration Examples

### Monitoring Script for Cron
```bash
#!/bin/bash
# Add to crontab: */5 * * * * /path/to/monitor.sh

cd /path/to/scraper
./scraper status > /dev/null
if [ $? -ne 0 ]; then
    echo "Scraper health check failed" | mail -s "Scraper Alert" admin@example.com
fi
```

### Nagios/Icinga Check
```bash
#!/bin/bash
# Nagios plugin for scraper health

cd /path/to/scraper
OUTPUT=$(./scraper status 2>&1)
if echo "$OUTPUT" | grep -q "System Healthy"; then
    echo "OK - Scraper is healthy"
    exit 0
elif echo "$OUTPUT" | grep -q "Warning"; then
    echo "WARNING - $OUTPUT"
    exit 1
else
    echo "CRITICAL - $OUTPUT"
    exit 2
fi
```

This monitoring system provides comprehensive visibility into your scraping platform's health and makes it easy to manage both in development and production environments.
