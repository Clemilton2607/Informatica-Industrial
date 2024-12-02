from flask import Flask, render_template
import psycopg2
import pandas as pd
import plotly.express as px
import dash
from dash import dcc, html

# Configuração do Flask
app = Flask(__name__)

# Configuração do PostgreSQL
DB_CONFIG = {
    'dbname': 'Dados_ESP',
    'user': 'postgres',
    'password': 'postgres',
    'host': 'localhost',
    'port': 5432
}

def fetch_data():
    """Consulta os dados do banco de dados e retorna DataFrames"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        temperatura_df = pd.read_sql_query("SELECT * FROM dados_sensor_temperatura", conn)
        umidade_df = pd.read_sql_query("SELECT * FROM dados_sensor_umidade", conn)
        conn.close()
        return temperatura_df, umidade_df
    except Exception as e:
        print(f"Erro ao conectar ao banco de dados: {e}")
        return None, None

# Rota principal do Flask
@app.route("/")
def index():
    # Obter dados do banco de dados
    temperatura_df, umidade_df = fetch_data()

    if temperatura_df is None or umidade_df is None:
        return "Erro ao carregar dados do banco de dados."

    # Renderizar template HTML
    return render_template("dashboard.html", temperatura=temperatura_df, umidade=umidade_df)

# Inicializar Dash no Flask
dash_app = dash.Dash(
    __name__,
    server=app,
    url_base_pathname='/visualizacao/'
)

def create_dash_layout():
    # Obter dados
    temperatura_df, umidade_df = fetch_data()

    if temperatura_df is None or umidade_df is None:
        return html.Div("Erro ao carregar dados do banco de dados.")

    # Criar gráficos
    fig_temp = px.line(temperatura_df, x="data_medicao", y="medida_temperatura", title="Temperatura ao longo do tempo")
    fig_umid = px.line(umidade_df, x="data_medicao", y="medida_umidade", title="Umidade ao longo do tempo")

    return html.Div([
        dcc.Graph(figure=fig_temp),
        dcc.Graph(figure=fig_umid)
    ])

dash_app.layout = create_dash_layout

if __name__ == "__main__":
    app.run(debug=True)
