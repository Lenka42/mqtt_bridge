import argparse
import configparser
import logging
import signal
import asyncio
import gmqtt

from utils import parse_config

STOP = asyncio.Event()

CLIENTS = []


def on_connect(client, flags, rc, properties):
    logging.info('[CONNECTED {}]'.format(client._client_id))


def on_message(client, topic, payload, qos, properties):
    for other_client in CLIENTS:
        if other_client._client_id == client._client_id:
            continue
        # TODO: republish also properties
        other_client.publish(topic, payload, qos=qos)
    logging.info('[RECV MSG {}] TOPIC: {} PAYLOAD: {} QOS: {} PROPERTIES: {}'
                 .format(client._client_id, topic, payload, qos, properties))


def on_disconnect(client, packet, exc=None):
    logging.info('[DISCONNECTED {}]'.format(client._client_id))


def on_subscribe(client, mid, qos, properties):
    logging.info('[SUBSCRIBED {}] QOS: {}'.format(client._client_id, qos))


def assign_callbacks_to_client(client):
    client.on_connect = on_connect
    client.on_message = on_message
    client.on_disconnect = on_disconnect
    client.on_subscribe = on_subscribe


def ask_exit(*args):
    STOP.set()


async def main(config):
    global CLIENTS
    for client_name in config.keys():
        client = gmqtt.Client(client_name, clean_session=False, session_expiry_interval=0xFFFFFFFF)
        assign_callbacks_to_client(client)
        client.set_auth_credentials(config[client_name]['username'], config[client_name]['password'])
        await client.connect(config[client_name]['host'], config[client_name]['port'],
                             ssl=config[client_name]['ssl'])
        for topic in config[client_name]['topics']:
            client.subscribe(topic, qos=1, no_local=True)
        CLIENTS.append(client)

    await STOP.wait()
    for client in CLIENTS:
        await client.disconnect()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    logging.basicConfig(level=logging.INFO)

    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('--config', dest='config_path', default='config.yaml')
    args = arg_parser.parse_args()
    parsed_config = parse_config(args.config_path)

    loop.add_signal_handler(signal.SIGINT, ask_exit)
    loop.add_signal_handler(signal.SIGTERM, ask_exit)

    loop.run_until_complete(main(parsed_config))
