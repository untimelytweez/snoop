import base64
import json
import logging
import threading
import time
from datetime import timedelta

import grpc
from celery import shared_task
from celery.signals import worker_ready
from django.conf import settings
from django.db.models import Count, Q
from django.utils import timezone
from jito_searcher_client import get_searcher_client
from jito_searcher_client.convert import versioned_tx_to_protobuf_packet
from jito_searcher_client.generated.bundle_pb2 import Bundle as ProtoBundle
from jito_searcher_client.generated.searcher_pb2 import SendBundleRequest, SubscribeBundleResultsRequest
from solders.keypair import Keypair
from solders.transaction import VersionedTransaction

from apps.mev.models import Bundle

logger = logging.getLogger(__name__)

SEND_MAX_RETRIES = 5
LANDED_KINDS = ("processed", "finalized")
FAILED_KINDS = ("rejected", "dropped")

_client = None
_subscriber = None


def _load_auth_keypair():
    with open(settings.JITO_AUTH_KEYPAIR_PATH) as fh:
        return Keypair.from_bytes(bytes(json.load(fh)))


def _get_client():
    global _client
    if _client is None:
        _client = get_searcher_client(settings.JITO_BLOCK_ENGINE_URL, _load_auth_keypair())
        logger.info("Cliente gRPC da Jito criado nesse worker, bora")
    return _client


def _decode_packets(transactions):
    return [
        versioned_tx_to_protobuf_packet(VersionedTransaction.from_bytes(base64.b64decode(raw)))
        for raw in transactions
    ]


@shared_task(
    bind=True,
    max_retries=SEND_MAX_RETRIES,
    autoretry_for=(grpc.RpcError,),
    retry_backoff=True,
)
def send_bundle(self, bundle_pk):
    bundle = Bundle.objects.get(pk=bundle_pk)
    bundle.attempts += 1
    bundle.status = "sending"
    bundle.save(update_fields=["attempts", "status"])

    try:
        packets = _decode_packets(bundle.transactions)
    except (ValueError, TypeError) as exc:
        logger.error("Bundle %s com tx zoada, nem manda pra Jito: %s", bundle_pk, exc)
        bundle.status = "failed"
        bundle.save(update_fields=["status"])
        return None

    request = SendBundleRequest(bundle=ProtoBundle(header=None, packets=packets))
    response = _get_client().SendBundle(request)

    bundle.jito_bundle_id = response.uuid
    bundle.status = "sent"
    bundle.save(update_fields=["jito_bundle_id", "status"])
    logger.info("Bundle %s enviado via gRPC, jito_bundle_id=%s", bundle_pk, response.uuid)
    return response.uuid


def _apply_result(result):
    kind = result.WhichOneof("result")
    if kind in LANDED_KINDS:
        status = "landed"
    elif kind in FAILED_KINDS:
        status = "failed"
    else:
        return 0
    for _ in range(5):
        updated = Bundle.objects.filter(
            jito_bundle_id=result.bundle_id, status__in=("sending", "sent")
        ).update(status=status)
        if updated or not Bundle.objects.filter(status="sending").exists():
            return updated
        time.sleep(0.2)
    return 0


def _results_loop():
    while True:
        try:
            for result in _get_client().SubscribeBundleResults(SubscribeBundleResultsRequest()):
                _apply_result(result)
        except Exception:
            logger.exception("Stream de resultados da Jito caiu, reconectando")
            time.sleep(2)


@worker_ready.connect
def start_results_subscriber(**kwargs):
    global _subscriber
    if _subscriber is not None and _subscriber.is_alive():
        return
    _subscriber = threading.Thread(target=_results_loop, name="jito-results", daemon=True)
    _subscriber.start()
    logger.info("Subscriber de resultados da Jito rodando, bora")


def land_rate(hours=24):
    since = timezone.now() - timedelta(hours=hours)
    stats = Bundle.objects.filter(
        created_at__gte=since, jito_bundle_id__isnull=False
    ).aggregate(
        sent=Count("id"),
        landed=Count("id", filter=Q(status="landed")),
    )
    rate = stats["landed"] / stats["sent"] if stats["sent"] else 0.0
    logger.info(
        "Land rate das ultimas %sh: %s/%s = %.2f%%",
        hours, stats["landed"], stats["sent"], rate * 100,
    )
    return rate
