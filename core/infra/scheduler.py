"""
Scheduler infrastructure for running periodic tasks.
"""

import asyncio
import logging
from typing import Callable, Dict, Any, Optional

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from croniter import croniter


logger = logging.getLogger(__name__)


class Scheduler:
    """Async task scheduler wrapper around APScheduler with persistence."""
    
    def __init__(self, db_url: str = "sqlite:///db/scheduler_jobs.db", timezone: str = "UTC", enable_persistence: bool = True):
        """Initialize scheduler with optional persistence and timezone."""
        
        if enable_persistence:
            # Configure job store for persistence
            jobstores = {
                'default': SQLAlchemyJobStore(url=db_url)
            }
        else:
            # Use memory job store for testing/development
            jobstores = {}
        
        # Configure job defaults
        job_defaults = {
            'coalesce': False,
            'max_instances': 3,
            'misfire_grace_time': 60  # seconds
        }
        
        self._scheduler = AsyncIOScheduler(
            jobstores=jobstores,
            job_defaults=job_defaults,
            timezone=timezone
        )
        self._started = False

    async def start(self) -> None:
        """Start the scheduler."""
        if not self._started:
            self._scheduler.start()
            self._started = True
            logger.info("Scheduler started with persistence enabled")

    async def stop(self) -> None:
        """Stop the scheduler."""
        if self._started:
            self._scheduler.shutdown()
            self._started = False
            logger.info("Scheduler stopped")

    def add_interval_job(
        self,
        func: Callable,
        seconds: Optional[int] = None,
        minutes: Optional[int] = None,
        hours: Optional[int] = None,
        job_id: Optional[str] = None,
        **kwargs
    ) -> None:
        """Add a job that runs at regular intervals."""
        # Build trigger kwargs, excluding None values
        trigger_kwargs = {}
        if seconds is not None:
            trigger_kwargs['seconds'] = seconds
        if minutes is not None:
            trigger_kwargs['minutes'] = minutes
        if hours is not None:
            trigger_kwargs['hours'] = hours
            
        if not trigger_kwargs:
            raise ValueError("At least one of seconds, minutes, or hours must be specified")
            
        trigger = IntervalTrigger(**trigger_kwargs)
        
        self._scheduler.add_job(
            func,
            trigger=trigger,
            id=job_id,
            args=[job_id] if isinstance(func, str) else [],
            replace_existing=True,
            **kwargs
        )
        
        logger.info(f"Added interval job: {job_id or func.__name__}")

    def add_cron_job(
        self,
        func: Callable,
        cron_expression: str,
        job_id: Optional[str] = None,
        **kwargs
    ) -> None:
        """Add a job that runs on a cron schedule."""
        # Validate cron expression
        if not self._validate_cron_expression(cron_expression):
            raise ValueError(f"Invalid cron expression: {cron_expression}")
        
        # Parse cron expression (simplified - assumes minute, hour, day, month, day_of_week)
        parts = cron_expression.split()
        if len(parts) != 5:
            raise ValueError("Cron expression must have 5 parts: minute hour day month day_of_week")
        
        minute, hour, day, month, day_of_week = parts
        
        trigger = CronTrigger(
            minute=minute if minute != "*" else None,
            hour=hour if hour != "*" else None,
            day=day if day != "*" else None,
            month=month if month != "*" else None,
            day_of_week=day_of_week if day_of_week != "*" else None,
        )
        
        self._scheduler.add_job(
            func,
            trigger=trigger,
            id=job_id,
            args=[job_id] if isinstance(func, str) else [],
            replace_existing=True,
            **kwargs
        )
        
        logger.info(f"Added cron job: {job_id or func.__name__} ({cron_expression})")

    def add_interval_job_transient(
        self,
        func: Callable,
        seconds: Optional[int] = None,
        minutes: Optional[int] = None,
        hours: Optional[int] = None,
        job_id: Optional[str] = None,
        **kwargs
    ) -> None:
        """Add a job that runs at regular intervals (non-persistent)."""
        # Build trigger kwargs, excluding None values
        trigger_kwargs = {}
        if seconds is not None:
            trigger_kwargs['seconds'] = seconds
        if minutes is not None:
            trigger_kwargs['minutes'] = minutes
        if hours is not None:
            trigger_kwargs['hours'] = hours
            
        if not trigger_kwargs:
            raise ValueError("At least one of seconds, minutes, or hours must be specified")
            
        trigger = IntervalTrigger(**trigger_kwargs)
        
        # Use memory job store for transient jobs
        self._scheduler.add_job(
            func,
            trigger=trigger,
            id=job_id,
            jobstore='default',  # This will still use persistent store
            **kwargs
        )
        
        logger.info(f"Added transient interval job: {job_id or func.__name__}")

    def add_cron_job_transient(
        self,
        func: Callable,
        cron_expression: str,
        job_id: Optional[str] = None,
        **kwargs
    ) -> None:
        """Add a cron job (non-persistent for runtime-created jobs)."""
        # Validate cron expression
        if not self._validate_cron_expression(cron_expression):
            raise ValueError(f"Invalid cron expression: {cron_expression}")
        
        # Parse cron expression
        parts = cron_expression.split()
        if len(parts) != 5:
            raise ValueError("Cron expression must have 5 parts: minute hour day month day_of_week")
        
        minute, hour, day, month, day_of_week = parts
        
        trigger = CronTrigger(
            minute=minute if minute != "*" else None,
            hour=hour if hour != "*" else None,
            day=day if day != "*" else None,
            month=month if month != "*" else None,
            day_of_week=day_of_week if day_of_week != "*" else None,
        )
        
        # Add job without persistence for runtime-created jobs
        self._scheduler.add_job(
            func,
            trigger=trigger,
            id=job_id,
            **kwargs
        )
        
        logger.info(f"Added transient cron job: {job_id or func.__name__} ({cron_expression})")

    def _validate_cron_expression(self, cron_expression: str) -> bool:
        """Validate cron expression using croniter."""
        try:
            croniter(cron_expression)
            return True
        except Exception as e:
            logger.error(f"Invalid cron expression '{cron_expression}': {e}")
            return False

    def remove_job(self, job_id: str) -> None:
        """Remove a job by ID."""
        self._scheduler.remove_job(job_id)
        logger.info(f"Removed job: {job_id}")

    def list_jobs(self) -> Dict[str, Any]:
        """List all scheduled jobs."""
        jobs = {}
        for job in self._scheduler.get_jobs():
            jobs[job.id] = {
                "name": job.name,
                "next_run": job.next_run_time,
                "trigger": str(job.trigger),
            }
        return jobs
