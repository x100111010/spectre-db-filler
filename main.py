import asyncio
import logging
import os
import threading
import time

from BlocksProcessor import BlocksProcessor
from TxAddrMappingUpdater import TxAddrMappingUpdater
from VirtualChainProcessor import VirtualChainProcessor
from dbsession import create_all
from helper import KeyValueStore
from spectred.SpectredMultiClient import SpectredMultiClient

logging.basicConfig(format="%(asctime)s::%(levelname)s::%(name)s::%(message)s",
                    level=logging.DEBUG if os.getenv("DEBUG", False) else logging.INFO,
                    handlers=[
                        logging.StreamHandler()
                    ]
                    )

# disable sqlalchemy notifications
logging.getLogger('sqlalchemy').setLevel(logging.ERROR)

# get file logger
_logger = logging.getLogger(__name__)

# create tables in database
_logger.info('Creating DBs if not exist.')
create_all(drop=False)

spectred_hosts = []

for i in range(100):
    try:
        spectred_hosts.append(os.environ[f"SPECTRED_HOST_{i + 1}"].strip())
    except KeyError:
        break

if not spectred_hosts:
    raise Exception('Please set at least SPECTRED_HOST_1 environment variable.')

# create Spectred client
client = SpectredMultiClient(spectred_hosts)
task_runner = None


async def main():
    # initialize spectreds
    await client.initialize_all()

    # wait for client to be synced
    while client.spectreds[0].is_synced == False:
        _logger.info('Client not synced yet. Waiting...')
        time.sleep(60)

    # find last acceptedTx's block hash, when restarting this tool
    start_hash = KeyValueStore.get("vspc_last_start_hash")

    # if there is nothing in the db, just get latest block.
    if not start_hash:
        daginfo = await client.request("getBlockDagInfoRequest", {})
        start_hash = daginfo["getBlockDagInfoResponse"]["virtualParentHashes"][0]

    _logger.info(f"Start hash: {start_hash}")

    # create instances of blocksprocessor and virtualchainprocessor
    bp = BlocksProcessor(client)
    vcp = VirtualChainProcessor(client, start_hash)

    async def handle_blocks_commited(e):
        """
        this function is executed, when a new cluster of blocks were added to the database
        """
        global task_runner
        if task_runner and not task_runner.done():
            return

        _logger.debug('Update is_accepted for TXs.')
        task_runner = asyncio.create_task(vcp.yield_to_database())

    # set up event to fire after adding new blocks
    bp.on_commited += handle_blocks_commited

    # start blocks processor working concurrent
    while True:
        try:
            await bp.loop(start_hash)
        except Exception:
            _logger.exception('Exception occured and script crashed..')
            raise


if __name__ == '__main__':
    tx_addr_mapping_updater = TxAddrMappingUpdater()


    # custom exception hook for thread
    def custom_hook(args):
        global tx_addr_mapping_updater
        # report the failure
        _logger.error(f'Thread failed: {args.exc_value}')
        thread = args[3]

        # check if TxAddrMappingUpdater
        if thread.name == 'TxAddrMappingUpdater':
            p = threading.Thread(target=tx_addr_mapping_updater.loop, daemon=True, name="TxAddrMappingUpdater")
            p.start()
            raise Exception("TxAddrMappingUpdater thread crashed.")


    # set the exception hook
    threading.excepthook = custom_hook

    # run TxAddrMappingUpdater
    # will be rerun
    _logger.info('Starting updater thread now.')
    threading.Thread(target=tx_addr_mapping_updater.loop, daemon=True, name="TxAddrMappingUpdater").start()
    _logger.info('Starting main thread now.')
    asyncio.run(main())
