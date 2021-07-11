from utils.database_settings import DatabaseSettings
import requests
import urllib
from bs4 import BeautifulSoup
import dateparser


class SunatCrawler(DatabaseSettings):
    BASE_URL = 'https://e-consultaruc.sunat.gob.pe/cl-ti-itmrconsruc/jcrS00Alias'

    def __init__(self):
        super().__init__()

    def get_html(self, params):
        url = f'{self.BASE_URL}?{urllib.parse.urlencode(params)}'
        response = requests.post(url, headers={'Content-Length': '0', 'User-Agent': 'PostmanRuntime/7.28.0',
                                               'Accept': '*/*', 'Accept-Encoding': 'gzip, deflate, br',
                                               'Connection': 'keep-alive'})

        soup = BeautifulSoup(response.text, 'lxml')
        if soup.find('tbody') is not None:
            table_body = soup.find('tbody').tr.contents
            res = []
            for child in table_body:
                res.append(child.string.lstrip('\r\n').lstrip(' ').rstrip(' '))
            res = [x for x in res if x != '']
            return res
        return []

    def get_legal_rep_data(self, ruc=20481772550):
        params = {
            'accion': 'getRepLeg',
            'contexto': 'ti-it',
            'modo': 1,
            'desRuc': 'empresa',
            'nroRuc': ruc
        }

        html_data = self.get_html(params)

        if len(html_data) == 0:
            return {}

        return {
                'doc_type': html_data[0],
                'doc_number': html_data[1],
                'full_name': html_data[2],
                'job': html_data[3],
                'designation': html_data[4]
            }

    def get_coactive_debt(self, ruc):
        params = {
            'accion': 'getInfoDC',
            'contexto': 'ti-it',
            'modo': 1,
            'desRuc': 'empresa',
            'nroRuc': ruc
        }

        html_data = self.get_html(params)
        res = []
        if len(html_data) == 0:
            return []
        for i in range(0, len(html_data)):
            if (i+1) % 4 == 0:
                res.append({
                    'debt_amount': html_data[i-3],
                    'tributary_period': html_data[i-2],
                    'coactive_date': html_data[i-1],
                    'entity_debt': html_data[i]
                })
        return res

    def get_entity_provider_data(self):
        legal_representation_cursor = self.db.legalRepresentationInformation
        contract_list = []
        valid_contract_list = []
        provider_list = {}
        full_contract_document = self.db.fullContractData

        for document in legal_representation_cursor.find():
            print(f"Entity ruc: {document['ruc_entity'] } \n")
            print(f"Provider ruc: {document['ruc_provider']} \n")
            entity_legal_designation_date = dateparser.parse(document['legal_rep_entity_designation_date'],
                                                             date_formats=['%Y-%m-%d'])
            provider_legal_designation_date = dateparser.parse(document['legal_rep_provider_designation_date'],
                                                               date_formats=['%Y-%m-%d'])
            entity_location = full_contract_document.find_one({'ruc_entity': document['ruc_entity']})
            provider_location = full_contract_document.find_one({'ruc_provider': str(document['ruc_provider'])})
            if entity_location is None or provider_location is None:
                continue

            contract_data = {
                'entity_code': document['code_entity'],
                'entity_ruc': document['ruc_entity'],
                'entity': document['entity'],
                'entity_legal_rep_doc_type': document['legal_rep_entity_doc_type'],
                'entity_legal_rep_doc_number': document['legal_rep_entity_doc_number'],
                'entity_legal_rep_name': document['legal_rep_entity_name'],
                'entity_legal_rep_job': document['legal_rep_entity_job'],
                'entity_legal_rep_designation_date': entity_legal_designation_date,
                'entity_lat': entity_location['entity_lat'],
                'entity_lng': entity_location['entity_lng'],
                'referential_money': document['referential_money'],
                'contract_money': document['contract_money'],
                'provider_ruc': document['ruc_provider'],
            }

            provider_data = {
                    'provider_ruc': document['ruc_provider'],
                    'provider_name': document['name_provider'],
                    'provider_legal_rep_doc_type': document['legal_rep_provider_doc_type'],
                    'provider_legal_rep_doc_number': document['legal_rep_provider_doc_number'],
                    'provider_legal_rep_name': document['legal_rep_provider_name'],
                    'provider_legal_rep_job': document['legal_rep_provider_job'],
                    'provider_legal_rep_designation_date': provider_legal_designation_date,
                    'provider_heading': document['heading_provider'],
                    'provider_lat': provider_location['provider_lat'],
                    'provider_lng': provider_location['provider_lng'],
                    'coactive_debt': self.get_coactive_debt(document['ruc_provider'])
                }
            if document['ruc_provider'] not in provider_list.keys():
                provider_list[document['ruc_provider']] = provider_data

            valid_contract_data = {
                **contract_data,
                **provider_data
            }

            contract_list.append(contract_data)
            valid_contract_list.append(valid_contract_data)

        self.db.contracts.insert_many(contract_list)
        self.db.providerDocument.insert_many(list(provider_list.values()))
        self.db.validContractData.insert_many(valid_contract_list)

    def get_all_legal_rep(self):
        cursor = self.db.fullContractData
        res = []
        count = 0
        for document in cursor.find():
            entity_legal = self.get_legal_rep_data(int(document['ruc_entity']))
            provider_legal = self.get_legal_rep_data(int(document['ruc_provider']))
            count += 1

            if bool(entity_legal) and bool(provider_legal):
                res.append({
                    'code_entity': document['code_entity'],
                    'ruc_entity': document['ruc_entity'],
                    'entity': document['entity'],
                    'legal_rep_entity_doc_type': entity_legal['doc_type'],
                    'legal_rep_entity_doc_number': entity_legal['doc_number'],
                    'legal_rep_entity_name': entity_legal['full_name'],
                    'legal_rep_entity_job': entity_legal['job'],
                    'legal_rep_entity_designation_date': entity_legal['designation'],
                    'ruc_provider': document['ruc_provider'],
                    'name_provider': document['name_provider'],
                    'legal_rep_provider_doc_type': provider_legal['doc_type'],
                    'legal_rep_provider_doc_number': provider_legal['doc_number'],
                    'legal_rep_provider_name': provider_legal['full_name'],
                    'legal_rep_provider_job': provider_legal['job'],
                    'legal_rep_provider_designation_date': provider_legal['designation'],
                    'heading_provider': document['heading_provider'],
                    'referential_money': document['referential_price'],
                    'contract_money': document['official_price']
                })

                print(f'Registro: {count}')

        if len(res) > 0:
            print(len(res))
            self.db.legalRepresentationInformation.insert_many(res)
