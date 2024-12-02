from opcua import Server
from datetime import datetime
import time
import paho.mqtt.client as mqtt

# Funções do cliente MQTT
def on_connect(client, userdata, flags, rc):
    print("Conectado ao MQTT Broker!")
    client.subscribe("esp32/sensor1")
    client.subscribe("esp32/sensor2")

def on_message(client, userdata, msg):
    try:
        value = float(msg.payload.decode())
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        if msg.topic == "esp32/temperatura":
            sensor1.set_value(value)
        elif msg.topic == "esp32/umidade":
            sensor2.set_value(value)

        print(f"Atualizado {msg.topic} com valor {value} e timestamp {timestamp}")
    except Exception as e:
        print(f"Erro ao processar mensagem MQTT: {e}")



# Configuração do servidor OPC UA
opc_server = Server()
opc_server.set_endpoint("opc.tcp://localhost:4840/freeopcua/server/")
opc_server.set_server_name("Servidor OPC UA com MQTT")

# Configuração do namespace
namespace = opc_server.register_namespace("ESP32/Sensores")

# Criando objetos para os sensores
obj = opc_server.nodes.objects.add_object(namespace, "Sensores")

# Nó do sensor1
sensor1 = obj.add_variable(namespace, "Sensor_Temperatura", 0.0)

# Nó do sensor2
sensor2 = obj.add_variable(namespace, "Sensor_Umidade", 0.0)

# Permitir gravação nos nós
sensor1.set_writable()
sensor2.set_writable()

# Configuração do cliente MQTT
mqtt_client = mqtt.Client()
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message

# Conexão ao broker MQTT
mqtt_client.connect("broker.emqx.io", 1883, 60)  # Altere para o endereço do seu broker

# Inicializando o servidor OPC UA
opc_server.start()
print("Servidor OPC UA iniciado!")

try:
    while True:
        mqtt_client.loop(0.1)
        time.sleep(0.1)
        
except KeyboardInterrupt:
    print("Finalizando servidor...")
finally:
    opc_server.stop()
