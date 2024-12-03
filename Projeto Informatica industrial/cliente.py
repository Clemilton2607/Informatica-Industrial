from opcua import Client
from opcua.ua.uaerrors import UaStatusCodeError
import psycopg2
from psycopg2 import OperationalError
import time
from datetime import datetime

# Configuração do Servidor OPC UA
opcua_url = "opc.tcp://localhost:4840/freeopcua/server/"
client = Client(opcua_url)

try:
    print("Tentando conectar ao servidor OPC UA...")
    client.connect()
    print("Conexão estabelecida com sucesso!")
    
    conn = psycopg2.connect(
        dbname='Dados_ESP',
        user='postgres',
        password='postgres',
    )
    print("Conexão com o PostgreSQL foi estabelecida com sucesso!")
    cursor = conn.cursor()
    
    # Localizar os nós do servidor OPC UA
    sensor_temp_node = client.get_node("ns=2;i=2")
    sensor_umidade_node = client.get_node("ns=2;i=3")
    
    # Variáveis para armazenar os valores anteriores
    last_sensor_temp = None
    last_sensor_umidade = None
    
    while True:
        # Lendo os valores do servidor
        sensor_temperatura = sensor_temp_node.get_value()
        sensor_umidade = sensor_umidade_node.get_value()
        
        # Atualizar apenas se o valor mudou
        if sensor_temperatura != last_sensor_temp:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            cursor.execute(
                """INSERT INTO dados_sensor_temperatura (data_medicao, medida_temperatura) VALUES (%s, %s);""",
                (timestamp, sensor_temperatura)
            )
            conn.commit()
            print(f"Dado do sensor de temperatura ({sensor_temperatura}) inserido no banco com sucesso!")
            last_sensor_temp = sensor_temperatura  # Atualiza o valor anterior
        
        if sensor_umidade != last_sensor_umidade:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            cursor.execute(
                """INSERT INTO dados_sensor_umidade (data_medicao, medida_umidade) VALUES (%s, %s);""",
                (timestamp, sensor_umidade)
            )
            conn.commit()
            print(f"Dado do sensor de umidade ({sensor_umidade}) inserido no banco com sucesso!")
            last_sensor_umidade = sensor_umidade  # Atualiza o valor anterior
        
        time.sleep(1)  # Intervalo para evitar consultas constantes ao servidor

except UaStatusCodeError as e:
    print(f"Erro na conexão com o servidor OPC UA: {e}")
except Exception as ex:
    print(f"Erro inesperado: {ex}")
except OperationalError as e:
    print("Erro ao tentar conectar ao PostgreSQL.")
    print(f"Detalhes do erro: {e}")
finally:
    # Desconectar o cliente, se estiver conectado
    try:
        client.disconnect()
        print("Cliente desconectado com sucesso.")
        conn.close()
        print("Conexão com banco de dados fechada.")
    except Exception as ex:
        print(f"Erro ao tentar desconectar: {ex}")
