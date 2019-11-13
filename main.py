import argparse
from Twitter import *
import logging
import sys

def build_logger():
    # create logger for "Sample App"
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)

    # create file handler which logs even debug messages
    fh = logging.FileHandler(dir + '/Logs/DEBUG/debug '+today+'.txt', mode='w')
    fh.setLevel(logging.DEBUG)

    eh = logging.FileHandler(dir + '/Logs/DEBUG/debug '+today+'.txt', mode='w')
    eh.setLevel(logging.ERROR)
    # create console handler with a higher log level
    ch = logging.StreamHandler(stream=sys.stdout)
    ch.setLevel(logging.INFO)

    # create formatter and add it to the handlers
    formatter = logging.Formatter('[%(asctime)s] %(levelname)8s --- %(message)s ' +
                                  '(%(filename)s:%(lineno)s)', datefmt='%Y-%m-%d %H:%M:%S')
    fh.setFormatter(formatter)
    eh.setFormatter(formatter)
    ch.setFormatter(formatter)

    # add the handlers to the logger
    logger.addHandler(ch)
    logger.addHandler(eh)
    logger.addHandler(fh)

    return logger

def get_args():

    parser = argparse.ArgumentParser(description='Twitter Listener or Reader client.')

    parser.add_argument('-t', '--topics', help='Topics to present the Twitter Listener to listen by. Topics should be in comma separated list e.g. "topic1,topic2,topic3"', required=True, type=str)
    parser.add_argument('-o', '--host', default='127.0.0.1', help='Host domain to migrate the twitter streaming data towards.', type=str)
    parser.add_argument('-p', '--port', default=5555, help='Port number to send the twitter streaming data towards.', type=int)
    parser.add_argument('-f', '--filename', default=None, help='Filename for saving streaming log files.', type=str)

    return parser.parse_args()

def main():
    args = get_args()

    logger = build_logger()
    try:
        s = socket.socket()
        host = args.host
        port = args.port
        filename = args.filename
        s.bind((host, port))
        logger.info('Listening on port: {}'.format(port))
        logger.info('Host address: {}'.format(host))

        topics = args.topics.split(',')

        logger.info('Topics listening on: {}'.format(topics))
        logger.debug('Topics: {}'.format(topics))

        sendData(topics,logger=logger, filename=filename)
    except (KeyboardInterrupt):
        sys.exit(0)
    except Exception as e:
        logger.error("Unknown error occurred: {}", exc_info=True)
        raise


if __name__ == '__main__':
    main()