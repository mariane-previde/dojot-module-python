"""
dojot messenger module
"""

import json
import uuid
from .kafka import Producer
from .kafka import TopicManager
from .kafka import Consumer
from . import Auth
from .logger import Log

LOGGER = Log().color_log()

class Messenger:
    """
    Class responsible for sending and receiving messages through Kafka using
    dojot subjects and tenants.

    Using this class should be as easy as:

    .. code-block:: python
        :linenos:

        from dojot.module import Messenger, Config
        from dojot.module.logger import Log

        LOGGER = Log().color_log()
        def rcv_msg(tenant,data):
            LOGGER.critical("rcvd msg from tenant: %s -> %s" % (tenant,data))

        config = Config()
        messenger = Messenger("Dojot-Snoop", config)
        messenger.init()

        # Create a channel using a default subject ``device-data``.
        messenger.create_channel(config.dojot['subjects']['device_data'], "rw")

        # Create a second channel using a particular subject ``device-status``
        messenger.create_channel("service-status", "w")
        
        # Register callback to process incoming device data
        messenger.on(config.dojot['subjects']['device_data'], "message", rcv_msg)

        # Publish a message on ``service-status`` subject using ``dojot-management`` service.
        messenger.publish("service-status", config.dojot['dojot-management'], "service X is up")
    
    
    And that's all. 
    
    You can use an internal event publishing/subscribing mechanism in order to
    send events to other parts of the code (using ``messenger.on()`` and
    ``messenger.emit()`` functions) without actually send or receive any
    messages to/from Kafka. An example:


    .. code-block:: python

        messenger.on("extra-subject", "subject-event", lambda tenant, data: print("Message received ({}): {}", (tenant, data)))
        messenger.emit("extra-subject", "management-tenant", "subject-event", "message data")
    
    """
    def __init__(self, name, config):
        self.config = config
        self.topic_manager = TopicManager(config)
        self.event_callbacks = dict()
        self.tenants = []
        self.subjects = dict()
        self.topics = dict()
        self.producer_topics = dict()
        self.global_subjects = dict()
        self.instance_id = name + str(uuid.uuid4())

        self.producer = Producer(config)
        ret = self.producer.init()


        if ret:
            LOGGER.info("Producer for module %s is ready", self.instance_id)
        else:
            LOGGER.info("Could not create producer")

        self.consumer = Consumer("dojotmodulepython"+ str(uuid.uuid4()), config)

        self.create_channel(self.config.dojot['subjects']['tenancy'], "rw", True)



    def init(self):
        """
        Initializes the messenger and sets with all tenants

        This library uses its own mechanism to discover new tenants and
        subscribe to topics related to all configured subjects. That way the
        user can rely only on calling ``messenger.on()`` functions and therefore
        it will receive all messages from that subject related to different
        tenants.
        """
        self.on(self.config.dojot['subjects']['tenancy'], "message", self.process_new_tenant)
        auth = Auth(self.config)
        try:
            ret_tenants = auth.get_tenants()
            LOGGER.info("Retrieved list of tenants")
            for ten in ret_tenants:
                LOGGER.info("Bootstraping tenant: %s", ten)
                self.process_new_tenant(
                    self.config.dojot['management_service'], json.dumps({"tenant": ten}))
                LOGGER.info("%s bootstrapped.", ten)
                LOGGER.debug("tenants: %s", self.tenants)
            LOGGER.info("Finished tenant boostrapping")
        except Exception as error:
            LOGGER.warning("Could not get list of tenants: %s", error)


    def process_new_tenant(self, tenant, msg):
        """
        Process new tenant: bootstrap it for all subjects registered and emit
        an event

        :type tenant: str
        :param tenant: The tenant associated to the message (NOT NEW TENANT)
        :type msg: dict
        :param msg: The message just received with the new tenant..
        """
        LOGGER.info("Received message in tenanct subject.")
        LOGGER.debug("Tenant is: %s", tenant)
        LOGGER.debug("Message is: %s", msg)
        try:
            data = json.loads(msg)

        except json.JSONDecodeError as error:
            LOGGER.warning("Data is not a valid JSON. Bailing out.")
            LOGGER.warning("Error is: %s", error)
            return

        if "tenant" not in data:
            LOGGER.info("Received message is invalid. Bailing out.")
            return

        if data['tenant'] in self.tenants:
            LOGGER.info("This tenant was already registered. Bailing out.")
            return

        self.tenants.append(data['tenant'])
        for sub in self.subjects:
            self.__bootstrap_tenants(sub, data['tenant'], self.subjects[sub]['mode'])
        self.emit(self.config.dojot['subjects']['tenancy'],
                  self.config.dojot['management_service'], "new-tenant", data['tenant'])



    def emit(self, subject, tenant, event, data):
        """
        Executes all callbacks related to that subject:event

        :type subject: str
        :param subject: The subject to be used when emitting this new event
        :type tenant: str
        :param tenant: The tenant to be used when emitting this new event
        :type event: str
        :param event: The event to be emitted. This is a arbitrary string.
            The module itself will emit only ``message`` events (seldomly 
            ``new-tenant`` also)
        :type data: dict
        :param data: The data to be emitted

        """
        LOGGER.info("Emitting new event %s for subject %s@%s", event, subject, tenant)
        if subject not in self.event_callbacks:
            LOGGER.info("No on is listening to subject %s events", subject)
            return
        if event not in self.event_callbacks[subject]:
            LOGGER.info("No one is listening to subject %s %s events",
                        subject, event)
            return
        for callback in self.event_callbacks[subject][event]:
            callback(tenant, data)



    def on(self, subject, event, callback):
        """
        Register new callbacks to be invoked when something happens to a subject
        The callback should have two parameters: tenant, data

        :type subject: str
        :param subject: The subject which this subscription is associated to.
        :type event: str
        :param event: The event of this subscription.
        :param callback: The callback function. Its signature should be
            (tenant: str, message:any) : void
        """
        LOGGER.info("Registering new callback for subject %s and event %s", subject, event)

        if subject not in self.event_callbacks:
            self.event_callbacks[subject] = dict()

        if event not in self.event_callbacks[subject]:
            self.event_callbacks[subject][event] = []

        self.event_callbacks[subject][event].append(callback)

        if(subject not in self.subjects and subject not in self.global_subjects):
            self.create_channel(subject)


    def create_channel(self, subject, mode="r", is_global=False):
        """
        Creates a new channel tha is related to tenants, subjects, and kafka
        topics.

        :type subject: str
        :param subject: The subject associated to this channel.

        :type mode: str
        :param mode: Channel type ("r" for only receiving messages, "w" for
            only sending messages, "rw" for receiving and sending messages)
        
        :type is_global: bool
        :param is_global: flag indicating whether this channel should be 
            associated to a service or be global.
        """

        LOGGER.info("Creating channel for subject: %s", subject)

        associated_tenants = []

        if is_global is True:
            associated_tenants = [self.config.dojot['management_service']]
            self.global_subjects[subject] = dict()
            self.global_subjects[subject]['mode'] = mode
        else:
            associated_tenants = self.tenants
            self.subjects[subject] = dict()
            self.subjects[subject]['mode'] = mode

        LOGGER.debug("tenants in create channel: %s", self.tenants)
        for tenant in associated_tenants:
            self.__bootstrap_tenants(subject, tenant, mode, is_global)



    def __bootstrap_tenants(self, subject, tenant, mode, is_global=False):
        """
        Given a tenant, bootstrap it to all subjects registered.

        :type subject: str
        :param subject: The subject being bootstrapped
        :type tenant: str
        :param tenant: the tenant being bootstrapped
        :type mode: str
        :param mode: R/W channel mode (send only, receive only or both)
        :type is_global: bool
        :param is_global: flag indicating whether this channel should be 
            associated to a service or be global.
        """

        LOGGER.info("Bootstraping tenant %s for subject %s", tenant, subject)
        LOGGER.debug("Global: %s, mode: %s", is_global, mode)

        LOGGER.info("Requesting topic for %s@%s", subject, tenant)

        try:
            ret_topic = self.topic_manager.get_topic(tenant, subject, is_global)
            if ret_topic is None:
                LOGGER.warning("Could not bootstrap tenant %s. Bailing out.", tenant)
                return
            LOGGER.info("Got topics: %s", (json.dumps(ret_topic)))
            if ret_topic in self.topics:
                LOGGER.info("Already have a topic for %s@%s", subject, tenant)
                return

            LOGGER.info("Got topic for subject %s and tenant %s: %s", subject, tenant, ret_topic)
            self.topics[ret_topic] = {"tenant": tenant, "subject": subject}

            if "r" in mode:
                LOGGER.info("Telling consumer to subscribe to new topic")
                self.consumer.subscribe(ret_topic, self.__process_kafka_messages)
                if len(self.topics) == 1:
                    LOGGER.debug("Starting consumer thread")
                    try:
                        self.consumer.start()
                    except RuntimeError as error:
                        LOGGER.info("Something went wrong while starting thread: %s", error)
                else:
                    LOGGER.debug("Consumer thread is already started")

            if "w" in mode:
                LOGGER.info("Adding a producer topic.")
                if subject not in self.producer_topics:
                    self.producer_topics[subject] = dict()
                self.producer_topics[subject][tenant] = ret_topic
        except Exception as error:
            LOGGER.warning("Could not get topic: %s", error)



    def __process_kafka_messages(self, topic, messages):
        """
        This method is the callback that consumer will call when receives a message.

        This method is not supposed to be called by any module but as a callback
        to kafka-python library.

        :type topic: str
        :param topic: The topic used to receive the message.
        :type messages: str
        :param messages: The messages received
        """

        if topic not in self.topics:
            LOGGER.info("Nothing to do with messages of this topic")
            return

        # LOGGER.info("[Process kafka messages] Received messages: %s", messages)
        self.emit(self.topics[topic]['subject'], self.topics[topic]['tenant'], "message", messages)

    def publish(self, subject, tenant, message):
        """
        Publishes a message in kafka

        :type subject: str
        :param subject: The subject to be used when publish the data
        :type tenant: str
        :param tenant: The tenant associated to that message
        :type message: str
        :param messsage: The message to be published.
        """

        LOGGER.info("Trying to publish something on kafka, \
                     current producer-topics: %s", self.producer_topics)

        if subject not in self.producer_topics:
            LOGGER.info("No producer was created for subject %s", subject)
            LOGGER.info("Discarding message %s", message)
            return

        if tenant not in self.producer_topics[subject]:
            LOGGER.info(
                "No producer was created for subject %s@%s. \
                Maybe it was not registered?", subject, tenant)
            LOGGER.info("Discarding message %s", message)
            return

        self.producer.produce(self.producer_topics[subject][tenant], message)

        LOGGER.info("Published message: %s on topic %s", message,
                    self.producer_topics[subject][tenant])
