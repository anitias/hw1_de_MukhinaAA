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

# Шаг 1 - скачиваем данные
class DownloadData(luigi.Task):
    ds_name = luigi.Parameter(default='GSE68849')
    
    def output(self):
        return luigi.LocalTarget(os.path.join('data', self.ds_name + '_RAW.tar'))
    
    def run(self):
        os.makedirs('data', exist_ok=True)
        url = f'https://www.ncbi.nlm.nih.gov/geo/download/?acc={self.ds_name}&format=file'

        try:
            logging.info(f"Downloading {self.ds_name}...")
            wget.download(url, self.output().path)
        except Exception as e:
            logging.error(f"Download failed: {e}")
            raise


# Шаг 2 - распаковка архива
class UnpackData(luigi.Task):
    ds_name = luigi.Parameter(default='GSE68849')

    def requires(self):
        return DownloadData(ds_name=self.ds_name)
    
    def output(self):
        return luigi.LocalTarget(os.path.join('extracted', self.ds_name))

    def run(self):
        tar_path = self.input().path
        extract_path = os.path.join('extracted', self.ds_name)
        os.makedirs(extract_path, exist_ok=True)
        
        try:
            with tarfile.open(tar_path) as tar:
                logging.info(f"Extracting {self.ds_name}...")
                tar.extractall(path=extract_path)
        except Exception as e:
            logging.error(f"Extraction failed: {e}")
            raise
        finally:
            os.remove(tar_path)

        for item in os.listdir(extract_path):
            if item.endswith('.gz'):
                self._process_gzip_file(os.path.join(extract_path, item), extract_path)

    def _process_gzip_file(self, gz_path, extract_path):
        filename = os.path.splitext(os.path.basename(gz_path))[0]
        output_path = os.path.join(extract_path, filename)
        try:
            with gzip.open(gz_path, 'rb') as f_in:
                with open(output_path, 'wb') as f_out:
                    logging.info(f"Decompressing {gz_path}")
                    shutil.copyfileobj(f_in, f_out)
        except Exception as e:
            logging.error(f"Decompression failed: {e}")
            raise
        finally:
            os.remove(gz_path)


# Продолжение шага 2 - разделение таблиц
class SeparationTables(luigi.Task):
    ds_name = luigi.Parameter(default='GSE68849')

    def requires(self):
        return UnpackData(ds_name=self.ds_name)
    
    def output(self):
        return luigi.LocalTarget(os.path.join('processed', self.ds_name))
    
    def run(self):
        extract_path = self.input().path
        for filename in os.listdir(extract_path):
            file_path = os.path.join(extract_path, filename)
            if not filename.endswith('.txt'):
                continue
            self._process_text_file(file_path, filename)  

    def _process_text_file(self, file_path, filename):
        dfs = {}
        with open(file_path) as file:
            fio = io.StringIO()
            write_key = None
            for line in file:
                if line.startswith('['):
                    if write_key:
                        fio.seek(0)
                        dfs[write_key] = pd.read_csv(fio, sep='\t', header='infer')
                        fio = io.StringIO()
                    write_key = line.strip('[]\n')
                    continue
                fio.write(line)
            fio.seek(0)
            dfs[write_key] = pd.read_csv(fio, sep='\t')

        output_dir = os.path.join('processed', self.ds_name)
        os.makedirs(output_dir, exist_ok=True)

        for key, df in dfs.items():
            output_path = os.path.join(output_dir, filename.replace('.txt', f'_{key}.tsv'))
            logging.info(f"Saving table {key} to {output_path}")
            df.to_csv(output_path, sep='\t', index=False) 


# Шаг 3 - удаление ненужных колонок
class ReduceProbes(luigi.Task):
    ds_name = luigi.Parameter(default='GSE68849')

    def requires(self):
        return SeparationTables(ds_name=self.ds_name)

    def output(self):
        return luigi.LocalTarget(os.path.join('reduced', self.ds_name))

    def run(self):
        columns_to_remove = ['Definition',
                             'Ontology_Component',
                             'Ontology_Function',
                             'Obsolete_Probe_Id',
                             'Ontology_Process',
                             'Probe_Sequence'
                             'Synonyms']

        tsv_files = []
        with open(self.input().path, 'r') as f:
            for line in f:
                tsv_file_path = line.replace('\n', '')
                if 'Probes.tsv' in tsv_file_path:
                    tsv_files.append(tsv_file_path)

        if tsv_files:
            probes_path = tsv_files[0]
            df = pd.read_csv(probes_path, sep='\t')
            df_reduced = df.drop(columns=columns_to_remove)
            reduced_probes_path = os.path.dirname(probes_path) + '/reduced_probes.tsv'
            df_reduced.to_csv(reduced_probes_path, sep='\t', index=False)

            with open(self.output().path, 'w') as f:
                f.write(reduced_probes_path + '\n')
                f.close()


# Шаг 4 - удаление изначальных txt файлов
class CleanUpTask(luigi.Task):
    ds_name = luigi.Parameter(default='GSE68849')

    def requires(self):
        return ReduceProbes(ds_name=self.ds_name)

    def output(self):
        readme = 'data/readme.txt'
        return luigi.LocalTarget(readme)

    def run(self):
        created_files = [] 
        with open(self.input().path, 'r') as f:
            for line in f:
                created_files.append(line)         

        base_path = os.path.join('data', self.ds_name)
        removed_files = [] 
        for root, dirs, files in os.walk(base_path):
            for file in files:
                if file.endswith('.txt'):  
                    file_path = os.path.join(root, file)
                    try:
                        os.remove(file_path)  
                        removed_files.append(file) 
                    except FileNotFoundError:
                        pass  # Файл может быть уже удален

        with open(self.output().path, 'w') as f:
            f.write('Созданные файлы:' + '\n')
            for k in created_files:
                f.write(k)

            f.write('\n')
            f.write('Созданные временно и удаленные файлы:' + '\n')
            for k in removed_files:
                f.write(k + '\n')
            f.close()

    def complete(self):
        return self.output().exists()

if __name__ == "__main__":
    luigi.run()