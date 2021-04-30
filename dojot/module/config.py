'''
Configuration data module
'''

import os


class Config:
    '''
    Main configuration class

    This class contains all needed configuration for this library
    '''

    def __init__(self, config=None):
        '''
        Config constructor

        :type config: dict or None
        :param config: A configuration dictionary. If set, all its attributes
            will be set to this object.

        Any top level key will overwrite the default configuration, i.e.,
        setting a `kafka` object to config param will overwrite all Kafka
        configuration. An example of such dictionary is:

        .. code-block:: python

            config = {
                "kafka" : {
                    "producer": {
                        "client.id": "kafka",
                        "bootstrap_servers": ["kafka:9092"],
                        "compression.codec": "gzip",
                        "retry.backoff.ms": 200,
                        "message.send.max.retries": 10,
                        "socket.keepalive.enable": True,
                        "queue.buffering.max.messages": 100000,
                        "queue.buffering.max.ms": 1000,
                        "batch.num.messages": 1000000,
                        "dr_cb": True
                    },
                    "consumer": {
                        "group_id": "my-module",
                        "bootstrap_servers": ["kafka:9092"]
                    },
                    "dojot": {
                        "poll_timeout": 2000,
                        "subscription_holdoff": 2.5
                    }
                },
                "data_broker" : {
                    "url": "http://data-broker",
                    "timeout_sleep": 5,
                    "connection_retries": 3
                },
                "device_manager": {
                    "url": "http://device-manager:5000",
                    "timeout_sleep": 5,
                    "connection_retries": 3
                },
                "keycloak" = {
                    "timeout_sleep": 5,
                    "connection_retries": 3,
                    "base_path": "http://apigw:8000/auth/",
                    "ignore_realm": "master",
                    "credentials": {
                        "username": "admin",
                        "password": "admin",
                        "client_id": "admin-cli",
                        "grant_type": "password",
                    }
                },
                "dojot" : {
                    "management": {
                        "user" : "dojot-management",
                        "tenant" : "dojot-management"
                    },
                    "subjects": {
                        "tenancy": "dojot.tenancy",
                        "devices": "dojot.device-manager.device",
                        "device_data": "device-data,
                    }
                }
            }

        .. warning::

            If set, the `dojot` section should be in sync with all other
            modules. Otherwise this module won't work properly.

        .. note::

            The Kafka object is straight from librdkafka configuration,
            separated into producer and consumer subobjects. For more
            information about this configuration, you should check its
            documentation.

        '''
        self.load_defaults()
        self.load_env()
        if config is not None:
            if "kafka" in config:
                if "consumer" in config["kafka"]:
                    self.kafka["consumer"] = {
                        **self.kafka["consumer"], **config["kafka"]["consumer"]}
                if "producer" in config["kafka"]:
                    self.kafka["producer"] = {
                        **self.kafka["producer"], **config["kafka"]["producer"]}
                if "dojot" in config["kafka"]:
                    self.kafka["dojot"] = {
                        **self.kafka["dojot"], **config["kafka"]["dojot"]}
            if "data_broker" in config:
                self.data_broker = {
                    **self.data_broker, **config["data_broker"]}
            if "device_manager" in config:
                self.device_manager = {
                    **self.device_manager, **config["device_manager"]}
            if "dojot" in config:
                if "management" in config["dojot"]:
                    self.dojot["management"] = {
                        **self.dojot["management"], **config["dojot"]["management"]}
                if "subjects" in config["dojot"]:
                    self.dojot["subjects"] = {
                        **self.dojot["subjects"], **config["dojot"]["subjects"]}

    def load_defaults(self):
        '''
        Load default configuration, which is:

        .. code-block:: yaml

            kafka:
                producer:
                    client.id: "kafka"
                    bootstrap_servers:
                     - "kafka:9092"
                    compression.codec: "gzip"
                    retry.backoff.ms: 200
                    message.send.max.retries: 10
                    socket.keepalive.enable: True
                    queue.buffering.max.messages: 100000
                    queue.buffering.max.ms: 1000
                    batch.num.messages: 1000000
                    dr_cb: true
                consumer:
                    group_id: "my-module"
                    bootstrap_servers:
                     - "kafka:9092"
                dojot:
                    poll_timeout: 2000
                    subscription_holdoff: 2.5
            data_broker:
                url: "http://data-broker"
                "timeout_sleep": 5
                "connection_retries": 3
            device_manager:
                url: "http://device-manager:5000"
                "timeout_sleep": 5
                "connection_retries": 3
            keycloak:
                "base_path": "http://apigw:8000/auth"
                "timeout_sleep": 5
                "connection_retries": 3
                "ignore_realm": "master",
                "credentials":
                    "username": "admin",
                    "password": "admin",
                    "client_id": "admin-cli",
                    "grant_type": "password"
            dojot:
                management:
                    user: "dojot-management"
                    tenant: "dojot-management"
                subjects:
                    tenancy: "dojot.tenancy"
                    devices: "dojot.device-manager.device"
                    device_data: "device-data"

        .. warning::

            Calling this function will overwrite any previously set
            configuration in the created object. Also setting any configuration
            *after* Kafka is started or any Messenger object is created will
            have no effect on them.

        .. warning::

            If set, the `dojot` section should be in sync with all other
            modules. Otherwise this module won't work properly.

        '''
        self.kafka = {
            "producer": {
                "client.id": "kafka",
                "bootstrap_servers": ["kafka:9092"],
                "compression.codec": "gzip",
                "retry.backoff.ms": 200,
                "message.send.max.retries": 10,
                "socket.keepalive.enable": True,
                "queue.buffering.max.messages": 100000,
                "queue.buffering.max.ms": 1000,
                "batch.num.messages": 1000000,
                "dr_cb": True
            },
            "consumer": {
                "group_id": "my-module-python",
                "bootstrap_servers": ["kafka:9092"],
            },
            "dojot": {
                "poll_timeout": 2000,
                "subscription_holdoff": 2.5
            }
        }

        self.device_manager = {
            "url": "http://device-manager:5000",
            "timeout_sleep": 5,
            "connection_retries": 3
        }

        self.data_broker = {
            "url": "http://data-broker",
            "timeout_sleep": 5,
            "connection_retries": 3
        }

        self.keycloak = {
            "timeout_sleep": 5,
            "connection_retries": 3,
            "ignore_realm": "master",
            "base_path": "http://apigw:8000/auth/",
            "credentials": {
                "username": "admin",
                "password": "admin",
                "client_id": "admin-cli",
                "grant_type": "password",
            }
        }

        self.dojot = {
            "management": {
                "user": "dojot-management",
                "tenant": "dojot-management"
            },
            "subjects": {
                "tenancy": "dojot.tenancy",
                "devices": "dojot.device-manager.device",
                "device_data": "device-data"
            }
        }

    def load_env(self):
        '''
        Load configuration from environment variables.

        Any environment variable will overwrite the default configuration.
        Check load_defaults() function.

        The list of envirnoment variables is:

        - ``KAFKA_HOSTS``: a comma-separated list of hosts where an instance
          of Kafka is running. This will affect the `bootstrap_servers`
          parameter for both Kafka consumer and producer.
        - ``KAFKA_GROUP_ID``: The Kafka consumer group ID to be used.
        - ``DOJOT_KAFKA_SUBSCRIPTION_HOLDOFF``: Time to wait before performing any
          subscription.
        - ``DOJOT_KAFKA_POLL_TIMEOUT``: Time to wait for new messages in Kafka.
        - ``DATA_BROKER_URL``: Where DataBroker service can be reached.
        - ``DEVICE_MANAGER_URL``: URL to reach the device-manager service.
        - ``AUTH_URL``: Where Auth service can be reached.
        - ``KEYCLOAK_URL``: Where Keycloak service can be reached.
        - ``KEYCLOAK_USER``: Keycloak user (this user must have permission to list realms).
        - ``KEYCLOAK_PASSWORD``: Keycloak user password.
        - ``KEYCLOAK_CLIENT_ID``: Keycloak client id.
        - ``DOJOT_MANAGEMENT_TENANT``: tenant to be used when asking
          DataBroker for management topics (such as tenancy-related topics)
        - ``DOJOT_MANAGEMENT_USER``: user to be used when asking
          DataBroker for management topics (such as tenancy-related topics)
        - ``DOJOT_SUBJECT_TENANCY``: Subject to be used when asking
          DataBroker for tenancy topics.
        - ``DOJOT_SUBJECT_DEVICES``: Subject to be used when asking
          DataBroker for device topics.
        - ``DOJOT_SUBJECT_DEVICE_DATA``: Subject to be used when asking
          DataBroker for device data topics.

        '''

        bootstrap_servers = os.environ.get('KAFKA_HOSTS', None)
        if bootstrap_servers:
            bootstrap_servers = bootstrap_servers.split(',')
            self.kafka["producer"]["bootstrap_servers"] = bootstrap_servers
            self.kafka["consumer"]["bootstrap_servers"] = bootstrap_servers

        self.kafka["dojot"]["subscription_holdoff"] = int(os.environ.get(
            'DOJOT_KAFKA_SUBSCRIPTION_HOLDOFF', self.kafka["dojot"]["subscription_holdoff"]))

        self.kafka["dojot"]["poll_timeout"] = int(os.environ.get(
            'DOJOT_KAFKA_POLL_TIMEOUT', self.kafka["dojot"]["poll_timeout"]))

        self.kafka["consumer"]["group_id"] = os.environ.get(
            'KAFKA_GROUP_ID', self.kafka["consumer"]["group_id"])

        self.data_broker["url"] = os.environ.get(
            'DATA_BROKER_URL', self.data_broker["url"])

        self.device_manager["url"] = os.environ.get(
            'DEVICE_MANAGER_URL', self.device_manager["url"])

        self.keycloak["base_path"] = os.environ.get(
            "KEYCLOAK_URL", self.keycloak["base_path"])
        self.keycloak["credentials"]["username"] = os.environ.get(
            "KEYCLOAK_USER", self.keycloak["credentials"]["username"])
        self.keycloak["credentials"]["password"] = os.environ.get(
            "KEYCLOAK_PASSWORD", self.keycloak["credentials"]["password"])
        self.keycloak["credentials"]["client_id"] = os.environ.get(
            "KEYCLOAK_CLIENT_ID", self.keycloak["credentials"]["client_id"])

        self.dojot["management"]["user"] = os.environ.get(
            'DOJOT_MANAGEMENT_USER', self.dojot["management"]["user"])

        self.dojot["management"]["tenant"] = os.environ.get(
            'DOJOT_MANAGEMENT_TENANT', self.dojot["management"]["tenant"])

        self.dojot["subjects"]["tenancy"] = os.environ.get(
            'DOJOT_SUBJECT_TENANCY', self.dojot["subjects"]["tenancy"])

        self.dojot["subjects"]["devices"] = os.environ.get(
            'DOJOT_SUBJECT_DEVICES', self.dojot["subjects"]["devices"])

        self.dojot["subjects"]["device_data"] = os.environ.get(
            'DOJOT_SUBJECT_DEVICE_DATA', self.dojot["subjects"]["device_data"])
