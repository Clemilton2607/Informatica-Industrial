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
    # Se a conexão for bem-sucedida
    print("Conexão com o PostgreSQL foi estabelecida com sucesso!")
    cursor = conn.cursor()
    
    # Localizar os nós do servidor OPC UA
    sensor_temp_node = client.get_node("ns=2;i=2")  
    sensor_temp_timestamp_node = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    sensor_umidade_node = client.get_node("ns=2;i=4")
    sensor_umidade_timestamp_node = datetime .now().strftime("%Y-%m-%d %H:%M:%S")
    

    while True:
        # Lendo os valores do servidor
        sensor_temperatura = sensor_temp_node.get_value()
        data_sensor_temp = sensor_temp_timestamp_node
        sensor_umidade = sensor_umidade_node.get_value()
        data_sensor_umidade = sensor_umidade_timestamp_node
        # Inersindo Valores dos sensores no banco de dados
        cursor.execute(""" INSERT INTO dados_sensor_temperatura (data_medicao, medida_temperatura) VALUES (%s, %s);""", (data_sensor_temp, sensor_temperatura))
        conn.commit()
        print("Dado do sensor de temperatura inserido no banco com sucesso!")
        cursor.execute(""" INSERT INTO dados_sensor_umidade (data_medicao, medida_umidade) VALUES (%s, %s);""", (data_sensor_umidade, sensor_umidade))
        conn.commit()
        print("Dado do sensor de umidade inserido no banco com sucesso!")
        time.sleep(5) 
        
        
        
    
    
    
    
    
    
    
    
   
        
      
        
        
        
        
except UaStatusCodeError as e:
    print(f"Erro na conexão com o servidor OPC UA: {e}")
except Exception as ex:
    print(f"Erro inesperado: {ex}")
except OperationalError as e:
        # Se ocorrer um erro durante a conexão
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
    except NameError:
            # Se a conexão nunca foi aberta, ignore
        pass
        
        
        
