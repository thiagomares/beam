import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.options.pipeline_options import PipelineOptions
from typing import List, Dict
# Aqui nos estamos fazendo a importação do beam e das opções de pipeline que teremos, e que serão de uso dentro do contexto

"""
    O que é uma PCollection?
    
    PCollection é uma coleção de dados que é processada pelo Apache Beam. Ela é a principal abstração de dados que o Beam utiliza para processar os dados
    
    O que é um PTransform?
    
    PTransform é uma transformação que é aplicada a uma PCollection para gerar uma nova PCollection. Ela é a principal abstração de processamento de dados que o Beam utiliza para processar os dados
    
    O que é um Pipeline?
    
    Pipeline é um conjunto de transformações que são aplicadas a uma ou mais PCollection para gerar uma nova PCollection. Ele é a principal abstração de execução de processamento de dados que o Beam utiliza para processar os dados
    
    Como podemos combinar o beam com o spark?
    
    O Apache Beam é uma biblioteca que permite a execução de pipelines de dados em ambientes de execução distribuídos, como o Apache Spark, o Apache Flink, o Google Cloud Dataflow, entre outros. 
    
    
"""

pipe_opts = PipelineOptions(argc=None) 
pipeline = beam.Pipeline(options=pipe_opts)

def texto_para_lista(elements: str, delimiter: str = '|'):
    """
        Esta função recebe um texto e um delimitador e retorna uma lista de elementos pelo delimitador
    """
    return elements.split(delimiter)


colunas_dengue = [
    'id',
    'data_iniSE',
    'casos',
    'ibge_code',
    'cidade',
    'uf',
    'cep',
    'latitude',
    'logitude'
]
class Teste:
    def lista_dicionario(elemento: str, colunas: List) -> Dict:
        """
            Esta função irá retornar um dicionário com as listas e as colunas que nos temos no nosso pipeline
        """
        return dict(zip(colunas, elemento))

    def trata_data(elemento: Dict):
        """
            Esta função recebe um dicionário e faz o tratamento das datas, criando um novo campo com ANO-MES, retornando o dicionário com um novo campo
        """
        elemento['ano_mes'] = '-'.join(elemento['data_iniSE'].split('-')[:2])
        return elemento

    def chave_uf(elemento: Dict):
        """
            Esta função irá receber um dicionario e retornar uma tupla (uf, dicionario)
        """
        chave = elemento['uf']
        return(chave, elemento)
    
    def casos_dengue(elements):
        uf, dados = elements

        for dado in dados:
            yield (f'{uf}-{dado["ano_mes"]}', int(dado['casos']))

with beam.Pipeline() as pipeline:
    dengue = (
        pipeline
        | "Leitura do dataset de dengue" >> ReadFromText('./alura-apachebeam-basedados/casos_dengue.txt', skip_header_lines=1) # Nos damos aqui um label e fazemos a leitura de um arquivo de texto com o método de leitura do texto, pulando uma linha de cabeçalho
        # As Pcollections vai guardar os dados do arquivo ou da fonte que nos utilizamos
        | 'Texto p/ Lista' >> beam.Map(texto_para_lista)
        | "Converte os dados para um dicionário" >> beam.Map(Teste.lista_dicionario, colunas_dengue)
        | "Extrai o ano e o mes" >> beam.Map(Teste.trata_data)
        | "Retorna chave UF" >> beam.Map(Teste.chave_uf)
        | "Agrupa os dados" >> beam.GroupByKey() #O group by key vai retornar um agrupamento dos dados a partir de uma chave que nos fizemos no passo anterior
        # A nossa PCollection vai ser uma tupla, onde o primeiro elemento é a chave e o segundo é um iterável com os valores
        | "Calcula casos de dengue" >> beam.FlatMap(Teste.casos_dengue)
        | "Soma os casos de dengue" >> beam.CombinePerKey(sum) # com o combine per key, nos vamos fazer a soma dos valores que temos para cada chave
        | "Mostra Resultados" >> beam.Map(print)
        # Toda vez que formos utilizar um método externo, nos vamos utilizar um map, para que o beam consiga localizar estes métodos
)
pipeline.run()
