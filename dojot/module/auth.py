"""
Authentication module
"""

import base64
import json
import requests
import time
from .logger import Log
from .http_requester import HttpRequester
import traceback

LOGGER = Log().color_log()


class Auth:
    """
    Class responsible for authentication mechanisms in dojot
    """

    def __init__(self, config):
        """
        Object initialization

        :type config: Config
        :param config: The configuration object.
        """
        self.config = config

    def get_management_token(self):
        """
        Retrieves a token for management operations

        :rtype: str
        :return: The token
        """
        LOGGER.debug('getting token from keycloak')

        url = self.config.keycloak['base_path'] + \
            'realms/master/protocol/openid-connect/token'

        form_params = self.config.keycloak['credentials']

        try:
            payload = requests.post(url, data=form_params)
            token = payload.json()['access_token']
            LOGGER.debug('token succefully generated')
            return token
        except Exception as e:
            LOGGER.error(e)
            LOGGER.error(traceback.format_exc())
            raise

    def get_access_token(self, tenant):
        """
        Retrieves a token for normal operations associated to a particular
        tenant.

        :rtype: str
        :return: The token
        """

        userinfo = {
            "username": self.config.dojot["management"]["user"],
            "service": tenant
        }

        jwt = "{}.{}.{}".format(base64.b64encode("model".encode()).decode(),
                                base64.b64encode(json.dumps(
                                    userinfo).encode()).decode(),
                                base64.b64encode("signature".encode()).decode())

        return jwt

    def get_tenants(self):
        """
        Retrieves all tenants

        If there is a problem while retrieving the list of tenants, then None
        is returned.

        :rtype: list or None
        :return: List of tenants
        """
        LOGGER.debug('getting tenants...')

        url = self.config.keycloak["base_path"] + 'admin/realms'
        retry_counter = self.config.keycloak["connection_retries"]
        timeout_sleep = self.config.keycloak["timeout_sleep"]

        try:
            token = self.get_management_token()
        except Exception as e:
            LOGGER.error('Unable generate token')
            LOGGER.error(e)
            LOGGER.error(traceback.format_exc())

        try:
            payload = HttpRequester.do_it(
                url, token, retry_counter, timeout_sleep)
            if payload is None:
                return None  # because Python, that's because.

            tenants = []
            for tenant in payload:
                tenants.append(tenant['realm'])

            try:
                tenants.remove(self.config.keycloak["ignore_realm"])
            except Exception as e:
                LOGGER.error('Unable to remove ignore_realm on the list')
                LOGGER.error(e)
                LOGGER.error(traceback.format_exc())

            return tenants
        except Exception as e:
            LOGGER.error(e)
            LOGGER.error(traceback.format_exc())
            return None
