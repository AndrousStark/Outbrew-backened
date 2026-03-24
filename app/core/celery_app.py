"""
Celery App Configuration (Stub for development)

In production, this would be configured with a real message broker.
For development, tasks run synchronously.
"""

import logging
from functools import wraps
from typing import Any, Callable

logger = logging.getLogger(__name__)


class MockTask:
    """Mock Celery task for development without message broker"""

    def __init__(self, func: Callable, name: str = None, bind: bool = False):
        self.func = func
        self.name = name or func.__name__
        self.bind = bind
        wraps(func)(self)

    def __call__(self, *args, **kwargs):
        return self.func(*args, **kwargs)

    def delay(self, *args, **kwargs):
        """Run task synchronously in development"""
        logger.info(f"[MOCK-CELERY] Running task {self.name} synchronously")
        if self.bind:
            return self.func(self, *args, **kwargs)
        return self.func(*args, **kwargs)

    def apply_async(self, args=None, kwargs=None, **options):
        """Run task synchronously in development"""
        args = args or ()
        kwargs = kwargs or {}
        logger.info(f"[MOCK-CELERY] Running task {self.name} synchronously (apply_async)")
        if self.bind:
            return self.func(self, *args, **kwargs)
        return self.func(*args, **kwargs)


class MockCeleryApp:
    """Mock Celery application for development"""

    def __init__(self):
        self.tasks = {}

    def task(self, name: str = None, bind: bool = False, **options):
        """Decorator to register a task"""
        def decorator(func: Callable) -> MockTask:
            task = MockTask(func, name=name, bind=bind)
            self.tasks[task.name] = task
            return task
        return decorator

    def send_task(self, name: str, args=None, kwargs=None, **options):
        """Send task by name"""
        if name in self.tasks:
            return self.tasks[name].delay(*(args or ()), **(kwargs or {}))
        raise KeyError(f"Task {name} not registered")


# Create mock celery app for development
celery_app = MockCeleryApp()

logger.info("[CELERY] Using mock Celery app for development (tasks run synchronously)")
