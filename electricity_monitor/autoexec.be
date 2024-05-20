import electricity_monitor_MQTT

var emQ = electricity_monitor_MQTT.driver()
tasmota.add_driver(emQ)
tasmota.add_cmd(electricity_monitor_MQTT.setup_command_name, electricity_monitor_MQTT.setup_command)    