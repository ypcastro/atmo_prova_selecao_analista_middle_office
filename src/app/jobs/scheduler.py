"""
Q7 — Scheduler sem "drift" grosseiro.

Usa time.monotonic() para calcular o próximo tick independente
do tempo de execução do job, evitando acúmulo de atraso.
"""

import logging
import os
import signal
import threading
import time
from typing import Callable, Optional

logger = logging.getLogger(__name__)

_DEFAULT_INTERVAL = 60  # segundos


class PipelineScheduler:
    """
    Scheduler que executa um callable a cada `interval` segundos,
    sem drift: o tempo de espera é ajustado pelo tempo de execução do job.
    """

    def __init__(
        self,
        job: Callable,
        interval: Optional[float] = None,
    ) -> None:
        self.job = job
        self.interval = interval or float(
            os.environ.get("PIPELINE_INTERVAL_SECONDS", _DEFAULT_INTERVAL)
        )
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def _run_loop(self) -> None:
        logger.info(
            "Scheduler iniciado (interval=%.0fs)", self.interval
        )
        next_tick = time.monotonic()

        while not self._stop_event.is_set():
            now = time.monotonic()

            # Se atrasou mais de um intervalo inteiro, recalibra
            if now > next_tick + self.interval:
                logger.warning(
                    "Scheduler: atraso detectado (%.1fs); recalibrando",
                    now - next_tick,
                )
                next_tick = now

            if now >= next_tick:
                logger.info("Scheduler: disparando job")
                try:
                    self.job()
                except Exception as exc:
                    logger.error("Scheduler: erro no job: %s", exc, exc_info=True)
                next_tick += self.interval

            # Sleep pequeno para não consumir CPU (mas evitar drift)
            sleep_time = min(1.0, max(0.0, next_tick - time.monotonic()))
            self._stop_event.wait(timeout=sleep_time)

        logger.info("Scheduler encerrado.")

    def start(self, daemon: bool = True) -> None:
        """Inicia o scheduler em thread separada."""
        if self._thread and self._thread.is_alive():
            logger.warning("Scheduler já está em execução.")
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run_loop, name="PipelineScheduler", daemon=daemon
        )
        self._thread.start()
        logger.info("Scheduler thread iniciada.")

    def stop(self, timeout: float = 5.0) -> None:
        """Para o scheduler aguardando a thread finalizar."""
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=timeout)
        logger.info("Scheduler parado.")

    def run_blocking(self) -> None:
        """
        Executa o scheduler no thread atual (bloqueante).
        Captura SIGINT/SIGTERM para parada graciosa.
        """
        def _signal_handler(sig, frame):
            logger.info("Sinal %s recebido. Parando scheduler...", sig)
            self._stop_event.set()

        signal.signal(signal.SIGINT, _signal_handler)
        signal.signal(signal.SIGTERM, _signal_handler)
        self._run_loop()


def start_scheduler(job: Optional[Callable] = None) -> PipelineScheduler:
    """
    Conveniência: cria e inicia scheduler com o job padrão (run_once).
    """
    if job is None:
        from app.jobs.extract_job import run_once
        job = run_once

    scheduler = PipelineScheduler(job=job)
    scheduler.start(daemon=True)
    return scheduler

from datetime import datetime, timedelta


def compute_next_run(last: datetime, interval_seconds: int) -> datetime:
    if interval_seconds <= 0:
        raise ValueError("interval_seconds must be greater than 0")
    return last + timedelta(seconds=interval_seconds)
