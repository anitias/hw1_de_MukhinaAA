# Загружаем библиотеки
import gzip
import io
import logging
import luigi
import os
import pandas as pd
import shutil
import tarfile
import wget

# Загрузка датасета (шаг 1)
class DownloadData(luigi.Task):
    # Задаем название датасета (по умолчанию GSE68849)
    ds_name = luigi.Parameter(default="GSE68849")
    
    def output(self):
        return luigi.LocalTarget(f"data/{self.ds_name}/{self.ds_name}_RAW.tar")

    def run(self):
        target_dir = f"data/{self.ds_name}/"
        os.makedirs(target_dir, exist_ok=True)    # Создаем директорию для датасета, если ее нет

        # Задаем путь для скачивания
        url = f"https://www.ncbi.nlm.nih.gov/geo/download/?acc={self.ds_name}&format=file"
        output_path = os.path.join(target_dir, f"{self.ds_name}_RAW.tar")
        wget.download(url, out=output_path)
        
        try:
            logging.info(f"Downloading {self.ds_name}")
            wget.download(url, self.output().path)
        except Exception as e:
            logging.error(f"Download failed: {e}")
            raise        # Прерываем выполнение задачи в случае ошибки


# Распаковка архива, обработка таблиц + удаление ненужных файлов (шаги 2, 3, 4)
class ProcessingData(luigi.Task):
    # Задаем название датасета (по умолчанию GSE68849)
    ds_name = luigi.Parameter(default="GSE68849")
    
    # Создаем список столбцов для удаления
    columns_to_remove = ['Definition',
                         'Ontology_Component',
                         'Ontology_Function',
                         'Obsolete_Probe_Id',
                         'Ontology_Process',
                         'Probe_Sequence',
                         'Synonyms']

    # Работа данного блока зависит от выполнения предыдущего (от загрузки данных)
    def requires(self):
        return DownloadData(ds_name=self.ds_name)
    
    def output(self):
        return luigi.LocalTarget(f"data/{self.ds_name}/extracted/")

    def run(self):
        # Путь к загруженному датасету
        input_path = self.input().path
        # Директория для извлеченных файлов
        extract_dir = f"data/{self.ds_name}/extracted/"
        # Создание директории, если ее не существует
        os.makedirs(extract_dir, exist_ok=True)

        with tarfile.open(input_path, "r") as tar:
            # Извлечение файлов из архива
            tar.extractall(path=extract_dir) 
        # Удаление архива
        os.remove(input_path)

        for file_name in os.listdir(extract_dir):
            if file_name.endswith(".gz"):
                # Путь к сжатому файлу
                file_path = os.path.join(extract_dir, file_name)
                # Путь к директории для извлеченного файла
                folder_path = os.path.join(extract_dir, file_name[:-3])
                # Создание директории, если ее не существует
                os.makedirs(folder_path, exist_ok=True)

                with gzip.open(file_path, 'rb') as f_in:
                    # Извлечение содержимого файла из архива
                    with open(os.path.join(folder_path, file_name[:-3]), 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)  

                # Удаление сжатого файла
                os.remove(file_path) 
                # Путь к извлеченному файлу
                txt_file_path = os.path.join(folder_path, file_name[:-3])
                # Обработка извлеченного файла
                self.process_tsv(txt_file_path)
                # Удаление извлеченного файла  
                os.remove(txt_file_path)

    def process_tsv(self, file_path):
        # Код из модуля для разделения таблиц
        dfs = {}
        with open(file_path) as f:
            write_key = None
            fio = io.StringIO()
            for line in f.readlines():
                if line.startswith('['):
                    if write_key:
                        fio.seek(0)
                        header = None if write_key == 'Heading' else 'infer'
                        dfs[write_key] = pd.read_csv(fio, sep='\t', header=header)
                        fio = io.StringIO()
                    write_key = line.strip('[]\n')
                    continue
                if write_key:
                    fio.write(line)
            if write_key:
                fio.seek(0)
                dfs[write_key] = pd.read_csv(fio, sep='\t')

            for key, df in dfs.items():
                output_file_path = os.path.join(os.path.dirname(file_path), f"{key}.tsv")
                df.to_csv(output_file_path, sep='\t', index=False)
                
                if key == "Probes":
                    small_df = df.drop(columns=self.columns_to_remove)
                    small_df.to_csv(os.path.join(os.path.dirname(file_path), f"{key}_small_version.tsv"), sep='\t', index=False)  # Сохранение упрощённой версии таблицы.
    
    # Проверка существования выходной директории
    def complete(self):
        return self.output().exists()

# Запускаем выполнение пайплайна
if __name__ == '__main__':
    luigi.build([ProcessingData(ds_name='GSE68849')], local_scheduler=True)
