# encoding: utf-8
import asyncio

from spectred.SpectredThread import SpectredThread, SpectredCommunicationError
import logging

_logger = logging.getLogger(__name__)

# pipenv run python -m grpc_tools.protoc -I./protos --python_out=. --grpc_python_out=. ./protos/rpc.proto ./protos/messages.proto ./protos/p2p.proto

class SpectredClient(object):
    def __init__(self, spectred_host, spectred_port):
        self.spectred_host = spectred_host
        self.spectred_port = spectred_port
        self.server_version = None
        self.is_utxo_indexed = None
        self.is_synced = None
        self.p2p_id = None

    async def ping(self):
        try:
            info = await self.request("getInfoRequest")
            self.server_version = info["getInfoResponse"]["serverVersion"]
            self.is_utxo_indexed = info["getInfoResponse"]["isUtxoIndexed"]
            self.is_synced = info["getInfoResponse"]["isSynced"]
            self.p2p_id = info["getInfoResponse"]["p2pId"]
            return info

        except Exception as exc:
            return False

    async def request(self, command, params=None, timeout=60, retry=0):
        _logger.debug(f'Request start: {command}, {params}')
        for i in range(1 + retry):
            try:
                with SpectredThread(self.spectred_host, self.spectred_port) as t:
                    resp = await t.request(command, params, wait_for_response=True, timeout=timeout)
                    _logger.debug('Request end')
                    return resp
            except SpectredCommunicationError:
                if i == retry:
                    _logger.debug('Retries done.')
                    raise
                else:
                    _logger.debug('Wait for next retry.')
                    await asyncio.sleep(0.3)
            except Exception:
                _logger.exception('I should not be here.')
                raise

    async def notify(self, command, params, callback):
        t = SpectredThread(self.spectred_host, self.spectred_port, async_thread=True)
        return await t.notify(command, params, callback)
