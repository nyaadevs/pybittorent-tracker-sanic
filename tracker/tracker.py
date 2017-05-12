import logging
import asyncio
import aiomysql
from sanic import Sanic
from sanic.response import raw as response_raw
from sanic.exceptions import RequestTimeout
from random import sample
from bencode import encode as bencode_encode
from ipaddress import ip_address
from datetime import datetime, timedelta
from myparser import parse_args
from pickle import dump, load
from os.path import isfile

DEFAULT_INTERVAL = 300

PEER_INCREASE_LIMIT = 30
DEFAULT_ALLOWED_PEERS = 50
MAX_ALLOWED_PEERS = 55
INFO_HASH_LEN = 20
PEER_ID_LEN = 20

INVALID_REQUEST_TYPE = 100
MISSING_INFO_HASH = 101
MISSING_PEER_ID = 102
MISSING_PORT = 103
INVALID_INFO_HASH = 150
INVALID_PEER_ID = 151
INVALID_NUMWANT = 152
UNKNOWN_INFOHASH = 200
GENERIC_ERROR = 900


MYSQL_SERVER = ('127.0.0.1', 3306)
MYSQL_USER = 'test'
MYSQL_PASSWORD = 'test123'
MYSQL_DATABASE = 'nyaav2'
MYSQL_PREFIX = 'nyaa_'


class TrackerDatabase():
    def __init__(self, tracker, loop):
        self.logger = logging.getLogger()
        self.tracker = tracker
        self.loop = loop
        self.torrents = set()
        self.running = True
        self.pool = None
        self.sleep = None
        self.done = asyncio.Future(loop=loop)

        asyncio.ensure_future(self.start(), loop=loop)

    async def start(self):
        self.pool = await aiomysql.create_pool(host=MYSQL_SERVER[0], port=MYSQL_SERVER[1],
                                               user=MYSQL_USER, password=MYSQL_PASSWORD,
                                               db=MYSQL_DATABASE, loop=self.loop)

        self.logger.info('MySQL connection established.')

        while self.running:
            self.sleep = asyncio.ensure_future(asyncio.sleep(10, loop=self.loop))
            try:
                await self.sleep
            except asyncio.CancelledError:
                pass

            async with self.pool.acquire() as conn:
                async with conn.cursor() as cur:
                    data = []
                    for torrent in self.torrents:
                        data.append((torrent.seeders, torrent.leechers,
                                     torrent.completed, datetime.now(), torrent.id))
                        torrent.completed = 0

                    self.torrents.clear()
                    datalen = len(data)

                    if datalen:
                        await cur.executemany(
                            'UPDATE `{}statistics` SET '.format(MYSQL_PREFIX) +
                            '`seed_count`=%s,'
                            '`leech_count`=%s,'
                            '`download_count`=`download_count`+%s,'
                            '`last_updated`=%s'
                            'WHERE `torrent_id`=%s',
                            data
                        )

                        await conn.commit()

                        self.logger.info('Commited {} changes.'.format(datalen))

        self.pool.close()
        await self.pool.wait_closed()

        self.logger.info('MySQL connection closed.')
        self.done.set_result(True)

    async def query(self, info_hash):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                res = await cur.execute(
                    'SELECT `id` FROM `{}torrents` WHERE `info_hash`=%s'.format(MYSQL_PREFIX),
                    info_hash)

                if not res:
                    return None

                (torrent_id, ) = await cur.fetchone()

                return int(torrent_id)


def error_response(error, error_code):
    response = bencode_encode({'failure reason': error})
    return response_raw(response, status=error_code)


def get_real_ip(request):
    real_ip = request.ip[0]
    forwarded_for = request.headers.get('X-Forwarded-For', None)
    if forwarded_for:
        cut = forwarded_for.find(',')
        if cut != -1:
            forwarded_for = forwarded_for[:cut]
        real_ip = forwarded_for
    return real_ip


def parse_announce_request(request, real_ip):
    args = parse_args(request.query_string)

    info_hash = args.get('info_hash', None)
    if info_hash is None:
        return error_response('Missing info_hash field', MISSING_INFO_HASH)

    peer_id = args.get('peer_id', None)
    if peer_id is None:
        return error_response('Missing peer_id field', MISSING_PEER_ID)

    port = args.get('port', None)
    if port is None:
        return error_response('Missing port field', MISSING_PORT)

    if len(info_hash) != INFO_HASH_LEN:
        return error_response('info_hash is not {} bytes'.format(INFO_HASH_LEN), INVALID_INFO_HASH)

    if len(peer_id) != PEER_ID_LEN:
        return error_response('peer_id is not {} bytes'.format(PEER_ID_LEN), INVALID_PEER_ID)

    try:
        port = int(port)
    except ValueError:
        return error_response('Invalid port value', MISSING_PORT)

    if port < 1 or port > 65535:
        return error_response('Invalid port', MISSING_PORT)

    uploaded = args.get('uploaded', None)
    if uploaded is None:
        return error_response('Missing uploaded field', GENERIC_ERROR)

    try:
        uploaded = int(uploaded)
    except ValueError:
        return error_response('Invalid uploaded value', GENERIC_ERROR)

    downloaded = args.get('downloaded', None)
    if downloaded is None:
        return error_response('Missing downloaded field', GENERIC_ERROR)

    try:
        downloaded = int(downloaded)
    except ValueError:
        return error_response('Invalid downloaded value', GENERIC_ERROR)

    left = args.get('left', None)
    if left is None:
        return error_response('Missing left field', GENERIC_ERROR)

    try:
        left = int(left)
    except ValueError:
        return error_response('Invalid left value', GENERIC_ERROR)

    # Optional
    ip = args.get('ip', None)
    if ip is None:
        ip = ip_address(real_ip)
    else:
        try:
            ip = ip_address(ip.decode('ascii', errors='ignore'))
        except ValueError:
            return error_response('Invalid ip value', GENERIC_ERROR)

    ipv4 = args.get('ipv4', None)
    port4 = None
    if ipv4 is not None:
        ipv4 = ipv4.split(b':', maxsplit=1)
        if isinstance(ipv4, tuple):
            ipv4, port4 = ipv4

            if port4 < 1 or port4 > 65535:
                return error_response('Invalid ipv4 port', MISSING_PORT)
        else:
            ipv4 = ipv4[0]
            port4 = port

        try:
            ipv4 = ip_address(ipv4.decode('ascii', errors='ignore'))
            if ipv4.version != 4:
                raise ValueError
        except ValueError:
            return error_response('Invalid ipv4 value', GENERIC_ERROR)
    elif ip.version == 4:
        ipv4 = ip
        port4 = port

    ipv6 = args.get('ipv6', None)
    port6 = None
    if ipv6 is not None:
        ipv6 = ipv6.split(b']:', maxsplit=1)
        if isinstance(ipv6, tuple):
            ipv6, port6 = ipv6
            ipv6 += b']'

            if port6 < 1 or port6 > 65535:
                return error_response('Invalid ipv6 port', MISSING_PORT)
        else:
            ipv6 = ipv6[0]
            port6 = port

        try:
            ipv6 = ip_address(ipv6.decode('ascii', errors='ignore'))
            if ipv6.version != 6:
                raise ValueError
        except ValueError:
            return error_response('Invalid ipv6 value', GENERIC_ERROR)
    elif ip.version == 6:
        ipv6 = ip
        port6 = port

    numwant = args.get('numwant', None)
    if numwant is None:
        numwant = DEFAULT_ALLOWED_PEERS
    else:
        try:
            numwant = int(numwant)
        except ValueError:
            return error_response('Invalid numwant value', GENERIC_ERROR)

        numwant = min(MAX_ALLOWED_PEERS, numwant)

    event = args.get('event', None)
    if event is not None:
        event = event.lower()
        if event == b'started':
            event = 2
        elif event == b'stopped':
            event = 3
        elif event == b'completed':
            event = 1
        else:
            return error_response('Invalid event', GENERIC_ERROR)
    else:
        event = 0

    # Extension
    no_peer_id = args.get('no_peer_id', None)
    if no_peer_id is not None:
        if no_peer_id == b'1' or no_peer_id == b'true':
            no_peer_id = True
        else:
            no_peer_id = False

    compact = args.get('compact', None)
    if compact is not None:
        if compact == b'1' or compact == b'true':
            compact = True
        else:
            compact = False
    else:
        compact = True

    if not compact:
        return error_response('Only compact response allowed.', GENERIC_ERROR)

    return info_hash, peer_id, ipv4, port4, ipv6, port6, uploaded, downloaded, left, numwant, event, no_peer_id, compact


class TorrentPeer(object):
    __slots__ = ['torrent', 'ipv4', 'ipv6', 'port4', 'port6', 'uploaded',
                 'downloaded', 'left', 'completed', 'timestamp']

    def __init__(self, torrent, ipv4, ipv6, port4, port6, uploaded, downloaded, left, completed):
        self.ipv4 = ipv4
        self.ipv6 = ipv6
        self.port4 = port4
        self.port6 = port6
        self.uploaded = uploaded
        self.downloaded = downloaded
        self.left = left
        self.completed = completed
        self.timestamp = datetime.now()


class Torrent(object):
    __slots__ = ['info_hash', 'id', 'peers', 'leechers', 'seeders', 'completed', 'total_completed']

    def __init__(self, info_hash, torrent_id):
        self.info_hash = info_hash
        self.id = torrent_id

        self.peers = dict()
        self.leechers = 0
        self.seeders = 0
        self.completed = 0
        self.total_completed = 0


class Tracker():
    def __init__(self, loop):
        self.logger = logging.getLogger()
        self.loop = loop
        self.torrents = dict()
        self.running = True

        self.database = TrackerDatabase(tracker=self, loop=loop)

        asyncio.ensure_future(self.cleanup(), loop=loop)

    # http://jonas.nitro.dk/bittorrent/bittorrent-rfc.html#anchor17
    # https://wiki.theory.org/BitTorrent_Tracker_Protocol
    # https://wiki.theory.org/BitTorrentSpecification#Tracker_HTTP.2FHTTPS_Protocol
    async def handle_announce(self, request):
        real_ip = get_real_ip(request)
        self.logger.info('Announce from: {}'.format(real_ip))

        parsed = parse_announce_request(request, real_ip)
        if not isinstance(parsed, tuple):
            return parsed

        info_hash, peer_id, ipv4, port4, ipv6, port6, uploaded, downloaded, left, numwant, event, no_peer_id, compact = parsed

        torrent = self.torrents.get(info_hash, None)
        if not torrent:
            torrent_id = await self.database.query(info_hash)

            if not torrent_id:
                self.logger.info('Unauthorized torrent: {}'.format(info_hash.hex()))
                return error_response('Requested download is not authorized for use with this tracker.', UNKNOWN_INFOHASH)

            self.logger.info('New torrent: {}'.format(info_hash.hex()))
            torrent = Torrent(info_hash=info_hash, torrent_id=torrent_id)
            self.torrents[info_hash] = torrent

        peer_new = False
        peer = torrent.peers.get(peer_id, None)
        if not peer:
            peer_new = True
            self.logger.info('New peer: {}'.format(peer_id.hex()))

            peer = TorrentPeer(torrent=torrent, ipv4=ipv4, port4=port4, ipv6=ipv6, port6=port6,
                               uploaded=uploaded, downloaded=downloaded, left=left,
                               completed=(left == 0 or event == 1))
            torrent.peers[peer_id] = peer

            if peer.completed:
                torrent.seeders += 1
            else:
                torrent.leechers += 1

            self.database.torrents.add(torrent)

        if not peer_new and event == 0 and \
                peer.timestamp > (datetime.now() - timedelta(seconds=DEFAULT_INTERVAL / 2)):
            return error_response('Please slow down.', UNKNOWN_INFOHASH)

        peer.timestamp = datetime.now()

        # Regular announce
        if event == 0:
            if peer.ipv4 != ipv4 or peer.port4 != port4:
                self.logger.info('Peer "{}" announcing from new IPv4 address {}:{}'
                                 .format(peer_id.hex(), ipv4, port4))
                peer.ipv4 = ipv4
                peer.port4 = port4

            if peer.ipv6 != ipv6 or peer.port4 != port4:
                self.logger.info('Peer "{}" announcing from new IPv6 address {}:{}'
                                 .format(peer_id.hex(), ipv6, port6))
                peer.ipv6 = ipv6
                peer.port6 = port6

            peer.uploaded = uploaded
            peer.downloaded = downloaded
            peer.left = left

        # Completed
        elif event == 1:
            self.logger.info('Peer "{}" completed'.format(peer_id.hex()))

            peer.uploaded = uploaded
            peer.downloaded = downloaded
            peer.left = left
            if not peer.completed:
                peer.completed = True
                torrent.seeders += 1
                torrent.leechers -= 1
                torrent.completed += 1
                torrent.total_completed += 1

                self.database.torrents.add(torrent)

        # Started
        elif event == 2:
            self.logger.info('Peer "{}" started'.format(peer_id.hex()))

            peer.uploaded = uploaded
            peer.downloaded = downloaded
            peer.left = left

        # Stopped
        elif event == 3:
            self.logger.info('Peer "{}" stopped'.format(peer_id.hex()))

            if peer.completed:
                torrent.seeders -= 1
            else:
                torrent.leechers -= 1

            self.database.torrents.add(torrent)

            del torrent.peers[peer_id]
            if torrent.peers == {}:
                self.logger.info('Torrent "{}" removed'.format(info_hash.hex()))
                del self.torrents[info_hash]

        numwant = min(len(torrent.peers), numwant)
        random_sample = sample(list(torrent.peers.values()), numwant)

        peers_ipv4 = b''
        peers_ipv6 = b''
        if compact:
            for peer in random_sample:
                if peer.ipv4:
                    peers_ipv4 += peer.ipv4.packed + peer.port4.to_bytes(2, byteorder='big')
                if peer.ipv6:
                    peers_ipv6 += peer.ipv6.packed + peer.port6.to_bytes(2, byteorder='big')

        else:  # Never
            pass

        response = {
            'interval': DEFAULT_INTERVAL,
            'complete': torrent.seeders,
            'incomplete': torrent.leechers,
            'peers': peers_ipv4,
        }

        if peers_ipv6:
            response['peers6'] = peers_ipv6

        response = bencode_encode(response)

        return response_raw(response)

    async def handle_scrape(self, request):
        real_ip = get_real_ip(request)
        self.logger.info('Scrape from: {}'.format(real_ip))
        args = parse_args(request.query_string, multi=True)

        info_hashes = args.get('info_hash', None)
        if info_hashes is None:
            return error_response('Missing info_hash field', MISSING_INFO_HASH)

        files = {}
        for info_hash in info_hashes:
            if len(info_hash) != INFO_HASH_LEN:
                return error_response('info_hash is not {} bytes'.format(INFO_HASH_LEN), INVALID_INFO_HASH)

            torrent = self.torrents.get(info_hash, None)
            if not torrent:
                return error_response('Unknown info hash.', UNKNOWN_INFOHASH)

            files[info_hash] = {
                'complete': torrent.total_completed,
                'incomplete': torrent.leechers,
                'downloaded': torrent.seeders
            }

        response = bencode_encode({'files': files})

        return response_raw(response)

    async def cleanup(self):
        while self.running:
            await asyncio.sleep(60)

            last_valid = datetime.now() - timedelta(seconds=DEFAULT_INTERVAL * 2)
            removed_peers = 0
            removed_torrents = 0
            remove_torrent = []

            for info_hash, torrent in self.torrents.items():

                remove_peer = []

                for peer_id, peer in torrent.peers.items():
                    if peer.timestamp < last_valid:
                        remove_peer.append(peer_id)

                        if peer.completed:
                            torrent.seeders -= 1
                        else:
                            torrent.leechers -= 1

                removed_peers += len(remove_peer)
                for peer_id in remove_peer:
                    del torrent.peers[peer_id]

                if torrent.peers == {}:
                    remove_torrent.append(info_hash)

            removed_torrents += len(remove_torrent)
            for info_hash in remove_torrent:
                del self.torrents[info_hash]

            if removed_peers or removed_torrents:
                self.logger.info('[Cleanup] Removed {} dead peers and {} empty torrents.'.format(
                    removed_peers, removed_torrents))

    async def stop(self):
        self.logger.warning('TRACKER SHUTTING DOWN')

        self.running = False
        self.database.running = False
        self.database.sleep.cancel()
        await self.database.done

        with open('state.pickle', 'wb') as fp:
            dump(self.torrents, fp)

        self.logger.warning('TRACKER STOPPED')


def setup_logging(args):
    level = {
        'debug': logging.DEBUG,
        'info': logging.INFO,
        'warning': logging.WARNING,
        'error': logging.ERROR,
        'critical': logging.CRITICAL
    }[args.log_level]

    logging_format = "[%(asctime)s] %(levelname)s "
    logging_format += "%(module)s::%(funcName)s():l%(lineno)d: "
    logging_format += "%(message)s"

    logging.basicConfig(
        format=logging_format,
        level=level
    )


def end_point(v):
    if ':' in v:
        host, port = v.split(':')
    else:
        host, port = v, 8000

    if host == '':
        host = '127.0.0.1'

    if port == '':
        port = '8000'

    port = int(port)

    return host, port


async def handle_default(request):
    return response_raw(b'403', status=403)


def main():
    import argparse

    parser = argparse.ArgumentParser(description='HTTP BitTorrent tracker.')
    parser.add_argument(
        '--bind', '-b', default='127.0.0.1:8000', type=end_point,
        metavar='HOST:PORT',
        help='The address to bind to. Defaults to 127.0.0.1:8000')
    parser.add_argument(
        '--log-level', '-L', default='info',
        choices=['debug', 'info', 'warning', 'error', 'critical'],
        help='Set log level. Defaults to "info".')

    args = parser.parse_args()

    setup_logging(args)

    loop = asyncio.get_event_loop()

    tracker = Tracker(loop=loop)

    if isfile('state.pickle'):
        with open('state.pickle', 'rb') as fp:
            tracker.torrents = load(fp)

    app = Sanic()
    app.config.keep_alive = False

    app.add_route(handle_default, '/', methods=['GET'])
    app.add_route(tracker.handle_announce, '/announce', methods=['GET'])
    app.add_route(tracker.handle_scrape, '/scrape', methods=['GET'])

    @app.exception(RequestTimeout)
    def timeout(request, exception):
        return response_raw(b'RequestTimeout from error_handler.', 408)

    server = app.create_server(host=args.bind[0], port=args.bind[1])
    asyncio.ensure_future(server, loop=loop)

    try:
        loop.run_forever()
    except KeyboardInterrupt:
        print("INTERRUPT")
        loop.run_until_complete(tracker.stop())


if __name__ == '__main__':
    main()
