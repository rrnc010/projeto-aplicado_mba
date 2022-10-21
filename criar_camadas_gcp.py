from modulo_apibigquery import conect_bigquery
import os,logging,datetime


 #var_global
logging.basicConfig(filename='C:\\Users\\russel\\Desktop\\Ambiente Programacao\\mba\\Projetos\\pa\\logs\\extracao_site_ans.txt', level=logging.INFO,
                    format='  %(asctime)s - %(levelname)s - %(message)s', datefmt='%d/%m/%Y %I:%M:%S %p')
logging.info('Iniciando programa de ingestao gcp')
mod = conect_bigquery()
bucket_name="camada-row"
dataset_id='bd_camada_silver'
path_arquivos="C:\\Users\\russel\\Desktop\\Ambiente Programacao\\mba\\Projetos\\pa\\arquivos"
data = datetime.datetime.now().date()
ano_corrente = data.strftime("%Y")
qry_camada_silver='C:\\Users\\russel\\Desktop\\Ambiente Programacao\\mba\\Projetos\\pa\\querie\\cria_tabela_silver.txt'
qry_camada_gold='C:\\Users\\russel\\Desktop\\Ambiente Programacao\\mba\\Projetos\\pa\\querie\\cria_tabela_gold.txt'
qry_rotina_atualizacao='C:\\Users\\russel\\Desktop\\Ambiente Programacao\\mba\\Projetos\\pa\\querie\\rotina_atualizacao_fechamento.txt'



def cria_bucket():
    mod.create_bucket_class_location(bucket_name)
    logging.info('bucket criado com sucesso')



def upload_arquivos_bucket():
    status = "nok"
    try:
       for x in os.listdir(path_arquivos):
        if x.endswith(ano_corrente+".csv"):
            arq_destino = x
            bucket = 'camada-row'
            subpasta = 'arquivos/'
            mod.upload_cs(path_arquivos+"\\"+x, arq_destino, bucket, subpasta)
            status = "ok"
            logging.info('Upload ao bucket realizado com sucesso!')
            print("arquivos carregados com sucesso para bucket!")
    except Exception as e:
        print("Falha ao realizar upload Api Bigquery:", e)
        logging.info('Erro ao realizar upload da base!')
    return status




def cria_camada_silver():
    mod.cria_dataset(dataset_id)
    logging.info('dataset criado com sucesso')


def cria_tabela_camada_silver():
 status='nok'
 try:
    arq_qry = open(qry_camada_silver,'r')
    qry = arq_qry.read()
    df = mod.executaquery_df(qry)
    status='ok'
    logging.info('Tabela tbl_silver_ans reprocessada!')
 except:
     print('falha em rodar querie!')
     logging.info('Tabela tbl_silver_ans teve falha no reprocessamento!')

 return status


def cria_camada_gold():
    dataset_id='bd_camada_gold'
    mod.cria_dataset(dataset_id)
    logging.info('dataset criado com sucesso na camada Gold!')


def cria_tabela_camada_gold():
 status='nok'
 try:
    arq_qry = open(qry_camada_gold,'r')
    qry = arq_qry.read()
    df = mod.executaquery_df(qry)
    status='ok'
    logging.info('Tabela tbl_indice_ans reprocessada!')
 except:
     print('falha em rodar querie!')
     logging.info('Tabela tbl_indice_ans teve falha no reprocessamento!')


def rotina_atualizacao_fechamento():
 status='nok'
 try:
    arq_qry = open(qry_rotina_atualizacao,'r')
    qry = arq_qry.read()
    df = mod.executaquery_df(qry)
    status='ok'
    logging.info('carga realizada com sucesso da tabela tbl_indice_ans!')
    print('carga realizada com sucesso da tabela tbl_indice_ans!')
 except:
     print('falha em rodar querie!')
     logging.info('Tabela tbl_indice_ans teve falha no reprocessamento!')

upload_arquivos_bucket()
cria_camada_silver()
cria_tabela_camada_silver()
#cria_camada_gold()
rotina_atualizacao_fechamento()