'''The logger which is used in other modules is initialized here.'''
import logging

logger = logging.getLogger('Extraction')
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(levelname)s - %(asctime)s:   %(message)s' )
handler = logging.StreamHandler()
handler.setFormatter(formatter)
logger.addHandler(handler)
