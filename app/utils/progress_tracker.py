"""
Progress Tracker Utility
Real-time extraction progress tracking using Redis Pub/Sub
Enables Server-Sent Events (SSE) streaming to frontend
"""

import json
import asyncio
import logging
from typing import Dict, Any, AsyncGenerator
from datetime import datetime, timezone
from redis import asyncio as aioredis
from sqlalchemy.orm import Session

from app.core.config import settings
from app.models.extraction import ExtractionProgress

logger = logging.getLogger(__name__)


class ProgressTracker:
    """
    Track and stream extraction progress in real-time
    Uses Redis Pub/Sub for efficient message distribution
    """

    def __init__(self, job_id: int):
        self.job_id = job_id
        self.channel_name = f"extraction:progress:{job_id}"
        self.redis_client: aioredis.Redis = None

    async def connect(self):
        """Initialize Redis connection"""
        try:
            self.redis_client = await aioredis.from_url(
                settings.REDIS_URL,
                encoding="utf-8",
                decode_responses=True
            )
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            # Continue without Redis - fallback to database polling
            self.redis_client = None

    async def publish_update(self, update: Dict[str, Any]) -> None:
        """
        Publish progress update to Redis channel
        Called by extraction service during processing
        """
        if not self.redis_client:
            logger.warning("Redis not available, skipping publish")
            return

        try:
            # Add metadata
            update["job_id"] = self.job_id
            update["timestamp"] = datetime.now(timezone.utc).isoformat()

            # Publish to Redis channel
            await self.redis_client.publish(
                self.channel_name,
                json.dumps(update)
            )

            logger.debug(f"Published update to {self.channel_name}: {update.get('message')}")

        except Exception as e:
            logger.error(f"Failed to publish update: {e}")

    async def subscribe(self, db: Session) -> AsyncGenerator[Dict[str, Any], None]:
        """
        Subscribe to progress updates for streaming to frontend
        Used by SSE endpoint

        Yields progress updates as they arrive
        Falls back to database polling if Redis unavailable
        """
        if self.redis_client:
            # Redis Pub/Sub mode (preferred - real-time)
            async for update in self._subscribe_redis():
                yield update
        else:
            # Fallback: Database polling mode
            async for update in self._subscribe_database(db):
                yield update

    async def _subscribe_redis(self) -> AsyncGenerator[Dict[str, Any], None]:
        """
        Subscribe to Redis Pub/Sub channel
        Real-time updates with minimal latency
        """
        try:
            pubsub = self.redis_client.pubsub()
            await pubsub.subscribe(self.channel_name)

            logger.info(f"Subscribed to Redis channel: {self.channel_name}")

            # Listen for messages
            async for message in pubsub.listen():
                if message["type"] == "message":
                    try:
                        update = json.loads(message["data"])
                        yield update

                        # Check if extraction is complete
                        if update.get("type") == "complete" or update.get("progress_percent") >= 100:
                            logger.info(f"Extraction {self.job_id} complete, closing subscription")
                            break

                    except json.JSONDecodeError as e:
                        logger.error(f"Failed to parse message: {e}")
                        continue

            await pubsub.unsubscribe(self.channel_name)
            await pubsub.close()

        except Exception as e:
            logger.error(f"Redis subscription error: {e}")
            # If Redis fails mid-stream, don't crash - just stop streaming

    async def _subscribe_database(self, db: Session) -> AsyncGenerator[Dict[str, Any], None]:
        """
        Fallback: Poll database for progress updates
        Used when Redis is not available
        Less efficient but still functional
        """
        logger.warning(f"Using database polling for job {self.job_id} (Redis unavailable)")

        last_progress_id = 0
        poll_interval = 1.0  # Poll every 1 second

        while True:
            try:
                # Query for new progress records
                new_progress = (
                    db.query(ExtractionProgress)
                    .filter(
                        ExtractionProgress.job_id == self.job_id,
                        ExtractionProgress.id > last_progress_id
                    )
                    .order_by(ExtractionProgress.id.asc())
                    .all()
                )

                for progress in new_progress:
                    update = {
                        "job_id": self.job_id,
                        "stage": progress.stage.value,
                        "message": progress.message,
                        "progress_percent": progress.progress_percent,
                        "current_source": progress.current_source,
                        "current_layer": progress.current_layer,
                        "records_extracted": progress.records_extracted,
                        "records_validated": progress.records_validated,
                        "errors_encountered": progress.errors_encountered,
                        "timestamp": progress.created_at.isoformat(),
                        "type": "progress"
                    }

                    yield update
                    last_progress_id = progress.id

                    # Check if complete
                    if progress.progress_percent >= 100:
                        yield {
                            "job_id": self.job_id,
                            "type": "complete",
                            "message": "Extraction complete"
                        }
                        return

                # Wait before next poll
                await asyncio.sleep(poll_interval)

            except Exception as e:
                logger.error(f"Database polling error: {e}")
                await asyncio.sleep(poll_interval)

    async def close(self):
        """Close Redis connection"""
        if self.redis_client:
            await self.redis_client.close()


class ProgressPublisher:
    """
    Helper class for publishing progress updates
    Used by ScraperManager during extraction
    """

    def __init__(self, job_id: int):
        self.tracker = ProgressTracker(job_id)
        self.connected = False

    async def connect(self):
        """Initialize connection"""
        await self.tracker.connect()
        self.connected = True

    async def publish(
        self,
        stage: str,
        message: str,
        progress_percent: float,
        **kwargs
    ) -> None:
        """
        Publish progress update

        Args:
            stage: Extraction stage (discovery, fetching, rendering, etc.)
            message: Human-readable progress message
            progress_percent: Progress percentage (0-100)
            **kwargs: Additional fields (current_source, records_extracted, etc.)
        """
        if not self.connected:
            await self.connect()

        update = {
            "stage": stage,
            "message": message,
            "progress_percent": progress_percent,
            "type": "progress",
            **kwargs
        }

        await self.tracker.publish_update(update)

    async def publish_complete(
        self,
        total_records: int,
        valid_records: int,
        duration_seconds: int
    ) -> None:
        """Publish completion event"""
        if not self.connected:
            await self.connect()

        update = {
            "type": "complete",
            "message": f"Extraction complete: {valid_records} valid records",
            "progress_percent": 100.0,
            "total_records": total_records,
            "valid_records": valid_records,
            "duration_seconds": duration_seconds
        }

        await self.tracker.publish_update(update)

    async def publish_error(self, error_message: str) -> None:
        """Publish error event"""
        if not self.connected:
            await self.connect()

        update = {
            "type": "error",
            "message": f"Extraction failed: {error_message}",
            "error": error_message
        }

        await self.tracker.publish_update(update)

    async def close(self):
        """Close connection"""
        if self.tracker:
            await self.tracker.close()


# Convenience functions for use in extraction services

async def publish_progress(
    job_id: int,
    stage: str,
    message: str,
    progress_percent: float,
    **kwargs
) -> None:
    """
    Quick function to publish progress update
    Creates temporary connection
    """
    publisher = ProgressPublisher(job_id)
    try:
        await publisher.publish(stage, message, progress_percent, **kwargs)
    finally:
        await publisher.close()


async def stream_progress(
    job_id: int,
    db: Session
) -> AsyncGenerator[Dict[str, Any], None]:
    """
    Quick function to stream progress updates
    Used directly by SSE endpoint
    """
    tracker = ProgressTracker(job_id)
    try:
        await tracker.connect()
        async for update in tracker.subscribe(db):
            yield update
    finally:
        await tracker.close()
