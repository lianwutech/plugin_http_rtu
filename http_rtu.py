#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
    modbus网络的tcp数据采集插件
    1、device_id的组成方式为addr_slaveid
    2、设备类型为0，协议类型为modbus
    3、devices_info_dict需要持久化设备信息，启动时加载，变化时写入
"""

import os
import sys
import time
import json
import serial
from httplib2 import Http
import paho.mqtt.client as mqtt
from pymodbus.client.sync import ModbusSerialClient as ModbusClient
import threading
import logging
import ConfigParser
try:
    import paho.mqtt.publish as publish
except ImportError:
    # This part is only required to run the example from within the examples
    # directory when the module itself is not installed.
    #
    # If you have the module installed, just use "import paho.mqtt.publish"
    import os
    import inspect
    cmd_subfolder = os.path.realpath(os.path.abspath(os.path.join(os.path.split(inspect.getfile( inspect.currentframe() ))[0],"../src")))
    if cmd_subfolder not in sys.path:
        sys.path.insert(0, cmd_subfolder)
    import paho.mqtt.publish as publish
from json import loads, dumps

from libs.utils import *

# 全局变量
# 切换工作目录
# 程序运行路径
procedure_path = cur_file_dir()
# 工作目录修改为python脚本所在地址，后续成为守护进程后会被修改为'/'
os.chdir(procedure_path)

# 日志对象
logger = logging.getLogger('http_rtu')
hdlr = logging.FileHandler('./http_rtu.log')
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr)
logger.setLevel(logging.DEBUG)

# 加载配置项
config = ConfigParser.ConfigParser()
config.read("./http_rtu.cfg")
url = config.get('http', 'url')
url_params = config.get('http', 'params')
mqtt_server_ip = config.get('mqtt', 'server')
mqtt_server_port = int(config.get('mqtt', 'port'))
gateway_topic = config.get('gateway', 'topic')
device_id = config.get('device', 'id')
data_protocol = config.get('device', 'protocol')


def get_dict(url):
    http_obj = Http(timeout=5)
    resp, content = http_obj.request(
        uri=url,
        method='GET',
        headers={'Content-Type': 'application/json; charset=UTF-8'})

    if resp.status == 200:
        return content

    return ""


def publish_device_data(device_id, device_type, device_addr, device_port, device_data):
    # device_data: 16进制字符串
    # 组包
    device_msg = "%s,%d,%s,%d,%s" % (device_id, device_type, device_addr, device_port, device_data)

    # MQTT发布
    publish.single(topic=gateway_topic,
                   payload=device_msg,
                   hostname=mqtt_server_ip,
                   port=mqtt_server_port)
    logger.info("向Topic(%s)发布消息：%s" % (gateway_topic, device_msg))


# 串口数据读取线程
def process_mqtt():
    """
    :param device_id 设备地址
    :return:
    """
    # The callback for when the client receives a CONNACK response from the server.
    def on_connect(client, userdata, rc):
        logger.info("Connected with result code " + str(rc))
        # Subscribing in on_connect() means that if we lose the connection and
        # reconnect then subscriptions will be renewed.
        mqtt_client.subscribe(device_id)

    # The callback for when a PUBLISH message is received from the server.
    def on_message(client, userdata, msg):
        logger.info("收到数据消息" + msg.topic + " " + str(msg.payload))
        # 消息只包含device_cmd，为json字符串
        try:
            device_cmd = loads(msg.payload)
        except Exception, e:
            device_cmd = None
            logger.error("消息内容错误，%r" % msg.payload)

        # 根据设备指令组装消息
        value = device_cmd.get("value", None)
        if value is None:
            visit_url = url + device_cmd["resource_route"] + url_params
        else:
            visit_url = url + device_cmd["resource_route"] + url_params + "&value=" + value

        result = get_dict(visit_url)
        if len(result) > 0:
            publish_device_data(device_id, 0, device_id, 0, result)
        else:
            logger.error("访问url(%s)返回失败." % visit_url)


    mqtt_client = mqtt.Client(client_id=gateway_topic)
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message

    mqtt_client.connect(mqtt_server_ip, mqtt_server_port, 60)

    # Blocking call that processes network traffic, dispatches callbacks and
    # handles reconnecting.
    # Other loop*() functions are available that give a threaded interface and a
    # manual interface.
    mqtt_client.loop_forever()


if __name__ == "__main__":

    # 初始化mqtt监听
    mqtt_thread = threading.Thread(target=process_mqtt)
    mqtt_thread.start()

    # 发送设备信息
    publish_device_data(device_id,
                        0,
                        device_id,
                        0,
                        data_protocol,
                        "")
    while True:
        # 如果线程停止则创建
        if not mqtt_thread.is_alive():
            mqtt_thread = threading.Thread(target=process_mqtt)
            mqtt_thread.start()

        logger.debug("处理完成，休眠5秒")
        time.sleep(5)
