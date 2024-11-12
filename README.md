# Grafos1_KevinBacon

**Número da Lista**: (Sem número)<br>
**Conteúdo da Disciplina**: Grafos 1<br>

## Alunos
|Matrícula | Aluno |
| -- | -- |
| 221007813  |  André Emanuel Bispo da Silva |
| 221007869  |  Artur Henrique Holz Bartz |

## Sobre
Este projeto tem como objetivo explorar e analisar as conexões entre filmes e atores, utilizando dados extraídos do banco de dados IMDb. A ideia é construir um grafo onde os nós representam filmes e atores, e as arestas representam suas relações (atores que atuaram juntos em um mesmo filme).

Por meio dessa estrutura, é possível calcular caminhos e graus de separação entre atores, como no famoso exemplo de "Kevin Bacon" (Seis Graus de Kevin Bacon). O objetivo principal é identificar a menor distância entre dois atores em termos de filmes compartilhados, permitindo responder a perguntas como "Qual é a menor sequência de filmes que conecta dois atores?"

### Como Funciona?
Extração e Transformação de Dados: O projeto começa com um script ETL (Extract, Transform, Load), que baixa, descomprime, e importa arquivos .tsv (Tab-Separated Values) fornecidos pelo IMDb para um banco de dados SQLite. As tabelas de dados contêm informações sobre filmes, atores, diretores e gêneros.

Construção do Grafo: Com os dados já carregados e organizados no banco, outro script lê as tabelas do banco de dados e constrói um grafo, onde as conexões (arestas) representam a co-participação de atores em filmes.

Busca por Menor Caminho: Utilizando um algoritmo de busca em largura (BFS), o projeto encontra o menor caminho entre dois atores, caso exista uma conexão entre eles. O algoritmo retorna tanto a distância mínima (graus de separação) quanto o caminho específico (sequência de filmes e atores).

Interface com o Usuário: A navegação e visualização dos resultados pode ser feita via terminal com o uso do módulo curses do Python, permitindo que o usuário interaja com o grafo e visualize o caminho calculado entre os atores.

## Screenshots
Adicione 3 ou mais screenshots do projeto em funcionamento.

## Instalação
**Linguagem**: Python
**Framework**: Não há frameworks específicos; utilizamos bibliotecas padrão e populares do Python para ETL, manipulação de dados, e visualização.

### Pré Requisitos
Antes de rodar o projeto, é necessário instalar as seguintes dependências e configurar o ambiente:

Python 3.10 ou superior - Certifique-se de que o Python está instalado e configurado no PATH do sistema.

SQLite - O projeto utiliza SQLite como banco de dados. Certifique-se de que o SQLite está disponível no ambiente.

As principais bibliotecas utilizadas são:

SQLite3 (nativa do Python) - Para manipulação de bancos de dados SQLite.

Polars - Para leitura e processamento dos arquivos .tsv de forma otimizada.

Requests - Para baixar os arquivos do IMDb.

curses (somente para Linux/macOS) - Para interface em terminal. Em Windows, é necessário instalar a biblioteca windows-curses para suporte a curses.

## Uso

### Configuração
Crie o banco de dados: O script ETL criará o banco automaticamente. Não é necessário criar manualmente.

### Passos para rodar o projeto
ETL e Carregamento dos Dados:

O primeiro passo é rodar o script de ETL para baixar, descomprimir e carregar os dados no banco SQLite.

python etl_script.py

Construção do Grafo e Busca de Caminhos:

Depois de carregar os dados, utilize o script graph_generator.py para construir o grafo e realizar buscas de menor caminho entre atores:

python graph_generator.py

Este procedimento criará e populará o banco de dados, construirá o grafo, e permitirá que você realize buscas de caminhos entre atores e filmes conforme descrito.
