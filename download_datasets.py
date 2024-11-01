import requests
import gzip
import shutil
import os

DATASETS = {
    'name_basics': 'https://datasets.imdbws.com/name.basics.tsv.gz',
    'title_akas': 'https://datasets.imdbws.com/title.akas.tsv.gz',
    'title_basics': 'https://datasets.imdbws.com/title.basics.tsv.gz',
    'title_crew': 'https://datasets.imdbws.com/title.crew.tsv.gz',
    'title_episode': 'https://datasets.imdbws.com/title.episode.tsv.gz',
    'title_principals': 'https://datasets.imdbws.com/title.principals.tsv.gz',
    'title_ratings': 'https://datasets.imdbws.com/title.ratings.tsv.gz'
}

def download_and_extract(url: str, output_path: str):
    gz_file_path = output_path + '.gz'
    
    if os.path.exists(output_path):
        print(f"Arquivo {output_path} já existe, pulando download.")
        return

    print(f"Baixando {url}...")
    response = requests.get(url, stream=True)
    if response.status_code == 200:
        with open(gz_file_path, 'wb') as f:
            f.write(response.content)
        print(f"Download concluído: {gz_file_path}")
    else:
        print(f"Erro ao baixar o arquivo: {url}")
        return
    
    print(f"Extraindo {gz_file_path} para {output_path}...")
    with gzip.open(gz_file_path, 'rb') as f_in:
        with open(output_path, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    print(f"Extração concluída: {output_path}")
    
    os.remove(gz_file_path)
    print(f"Arquivo comprimido {gz_file_path} removido.")

if __name__ == "__main__":
    for name, url in DATASETS.items():
        output_path = f"data/{name}.tsv"
        download_and_extract(url, output_path)
