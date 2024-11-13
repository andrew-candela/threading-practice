"""
Classes capable of processing data in a multithreaded manner.
"""

from queue import Queue
from typing import Callable, Any
from threading import Thread, get_ident
import logging
from threading_practice.types.threading import Tombstone

logger = logging.getLogger(__name__)


def default_processor_function(job: Any):
    logger.debug(job)


def default_setup_function(): ...


def default_teardown_function(): ...


tombstone = Tombstone()


class Processor:
    def __init__(
        self,
        process_function: Callable[
            ...,
            None,
        ] = default_processor_function,
        setup_function: Callable[..., None] = default_setup_function,
        teardown_function: Callable[..., None] = default_teardown_function,
        workers: int = 1,
    ):
        self._queue = Queue()
        self._process_function = process_function
        self._setup_function = setup_function
        self._teardown_function = teardown_function
        self.workers = workers
        self.threads = [Thread(target=self._processor) for _ in range(self.workers)]

    def accept(self, thing: Any):
        self._queue.put(thing)

    def _processor(self):
        """
        An instance of the processor
        """
        setup_result = self._setup_function()
        while True:
            job = self._queue.get()
            try:
                if job == tombstone:
                    break
                logger.debug(f"processing: {job}")
                if setup_result is not None:
                    self._process_function(job, setup_result)
                else:
                    self._process_function(job)
            except Exception as e:
                logger.exception(
                    f"Exception occurred in thread {get_ident()}: \n {str(e)}"
                )
            finally:
                self._queue.task_done()
        self._teardown_function()
        logger.debug(f"Shutting down processor thread: {get_ident()}")

    def process(self):
        """
        Starts the processing threads and allows the main thread to continue.
        """
        logger.debug("Starting processor threads")

        for thread in self.threads:
            logger.debug(f"starting thread: {thread.ident}")
            thread.start()

    def join(self):
        """
        Blocks on the processing queue.
        Once jobs in the queue complete, shuts down the threads.
        """
        self._queue.join()
        for thread in self.threads:
            logger.debug(f"placing tombstone for thread {thread.ident}...")
            self._queue.put(tombstone)
        for thread in self.threads:
            thread.join()
        logger.debug("Finished processing")
