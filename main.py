import configparser
import logging
import signal
import asyncio
import gmqtt

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
    for client_name in config.sections():
        client = gmqtt.Client(client_name)
        assign_callbacks_to_client(client)
        client.set_auth_credentials(config[client_name]['username'], config[client_name]['password'])
        # TODO: make ssl a config parameter
        await client.connect(config[client_name]['host'], config[client_name]['port'], ssl=True)
        client.subscribe(config[client_name]['topic'])
        CLIENTS.append(client)

    await STOP.wait()
    for client in CLIENTS:
        await client.disconnect()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    logging.basicConfig(level=logging.INFO)

    config = configparser.ConfigParser()
    config.read('config.ini')

    loop.add_signal_handler(signal.SIGINT, ask_exit)
    loop.add_signal_handler(signal.SIGTERM, ask_exit)

    loop.run_until_complete(main(config))
