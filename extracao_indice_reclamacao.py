from modulo_selenium_ans import export_ans
import os,logging,datetime,shutil
import pandas as pd
from zipfile import ZipFile

 #variaveis global
mod_ans= export_ans()
inicio = datetime.datetime.now()
url = "http://landpage-h.cgu.gov.br/dadosabertos/index.php?url=http://ftp.dadosabertos.ans.gov.br/FTP/PDA/IGR/igr_anual.zip"
path_down= 'C:\\Users\\russel\\Downloads'
ano_corrente = inicio.strftime("%Y")
ano_corrente_dig = inicio.strftime("%y")
path_down_arq= 'C:\\Users\\russel\\Downloads\\igr_anual.zip'
path_import='C:\\Users\\russel\\Desktop\\Ambiente Programacao\\mba\\Projetos\\pa\\arquivos'
path_extract='C:\\Users\\russel\\Downloads\\ZIP_EXTRACT'
logging.basicConfig(filename='C:\\Users\\russel\\Desktop\\Ambiente Programacao\\mba\\Projetos\\pa\\logs\\extracao_site_ans.txt', level=logging.INFO,
                    format='  %(asctime)s - %(levelname)s - %(message)s', datefmt='%d/%m/%Y %I:%M:%S %p')
logging.info('Iniciando programa de download do arquivo da ans')

def extracao_csv():
    status="falha"

    try:
     for x in os.listdir(path_down):
            if x.endswith(".zip"):
                os.remove(path_down + '\\' + x)
            if x.endswith(".csv"):
                os.remove(path_down + '\\' + x)
     logging.info('Extrações anteriores removidas!')
     mod_ans.download_arquivo(url)
     mod_ans.fecha_navegador()
     print("Sucesso na execução do modulo de extração!")
     status="sucesso"
     logging.info('Download do arquivo realizado com sucesso do site da ans!!!')
    except Exception as e:
        print("Houve falaha na execução do modulo de extração!",e)
        logging.info('problema no download do arquivo no site da ans!!!')

    return status

def pivot_arquivo():
    for x in os.listdir(path_extract):
            if x.endswith(str(ano_corrente_dig)+".csv"):
               csv=pd.read_csv(path_extract+"\\"+x,delimiter=";" ,encoding='ISO-8859-1')
               df =pd.DataFrame(csv)
               filter_col = [col for col in df if col.endswith(str(ano_corrente_dig))]
               df = df.melt(id_vars=['Razão Social (Registro ANS)', 'Cobertura', 'Porte', 'Classificação no Mês',
                                     'Classificação no Mês Anterior','Competência','Data de atualização'],
                            value_vars=filter_col,
                            value_name='vlr_idx')
               df.to_csv(path_import+"\\"+x,index=False)


def tratamento_arquivo():

   status="falha"
   try:
       if extracao_csv()=="sucesso":
        for x in os.listdir(path_import):
               if x.endswith(str(ano_corrente)+".csv"):
                    os.remove(path_import + '\\' + x)
        for x in os.listdir(path_extract):
               if x.endswith(str(ano_corrente)+".csv"):
                    os.remove(path_extract + '\\' + x)
        z = ZipFile(path_down_arq, 'r')
        z.extractall(path_extract)
        z.close()
        pivot_arquivo()
        status="sucesso"
        logging.info('Arquivo tratado com sucesso e disponibilizado no diretorio local!')
        print("arquivo tratado com sucesso e disponibilizado no diretorio local!")
   except Exception as e:
       print("falaha no processo de tratamento do csv!",e)
       logging.info('Falha no tratamento do arquivo')

   return status



if tratamento_arquivo() == "sucesso":
    status_final = 'Sucesso'
else:
    status_final = 'Falha'

logging.info("Status final: {} Tempo decorrido: {} segundos.".format(status_final,
                                                                     str((datetime.datetime.now() - inicio).seconds)))