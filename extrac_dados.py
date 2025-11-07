import json
import urllib.request


class _Response:
    def __init__(self, data):
        self._data = data

    def json(self):
        return self._data


class requests:
    @staticmethod
    def get(url, *args, **kwargs):
        with urllib.request.urlopen(url) as resp:
            return _Response(json.load(resp))


dados = requests.get("https://servicodados.ibge.gov.br/api/v2/cnae/classes").json()

dados[0]

qtde_atividades_distintas = len(dados)

grupos = []

for registro in dados:
    grupos.append(registro["grupo"]["descricao"])

grupos[:10]

qtde_grupos_distintos = len(set(grupos))

grupos_count = [(grupo, grupos.count(grupo)) for grupo in set(grupos)]

grupos_count[:5]

grupos_count = dict(grupos_count)

valor_maximo = max(grupos_count.values())

grupos_mais_atividades = [
    chave for (chave, valor) in grupos_count.items() if valor == valor_maximo
]

print(len(grupos_mais_atividades))
print(grupos_mais_atividades)

import urllib.request
import json

from datetime import datetime

class ETL:
    def __init__(self):
        self.url = None

    def extract_cnae_data(self, url):
        self.url = url

        data_extracao = datetime.today().strftime("%Y/%m/%d - %H:%M:%S")

        dados = requests.get(self.url).json()

        grupos = []

        for registro in dados:
            grupos.append(registro["grupo"]["descricao"])

        grupos_count = [(grupo, grupos.count(grupo)) for grupo in set(grupos)]
        grupos_count = dict(grupos_count)

        valor_maximo = max(grupos_count.values())

        grupos_mais_atividades = [
            chave for (chave, valor) in grupos_count.items() if valor == valor_maximo
        ]

        qtde_atividades_distintas = len(dados)

        qtde_grupos_distintos = len(set(grupos))

        print(f"Dados extraídos em: {data_extracao}")
        print(f"Quantidade de atividades distintas = {qtde_atividades_distintas}")
        print(f"Quantidade de grupos distintos = {qtde_grupos_distintos}")
        print(
            f"Grupos com o maior número de atividades = {grupos_mais_atividades}, atividades = {valor_maximo}"
        )

        return None


ETL().extract_cnae_data("https://servicodados.ibge.gov.br/api/v2/cnae/classes")
